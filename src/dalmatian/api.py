from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Response, status
from fastapi.responses import StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.concurrency import run_in_threadpool

from dalmatian import __version__
from dalmatian.config import get_settings
from dalmatian.dependencies import (
    get_job_queue,
    get_query_service,
    get_storage_registry,
    shutdown_dependencies,
)
from dalmatian.errors import (
    BusyError,
    IdempotencyConflict,
    QueryCancelled,
    QueryRejected,
    QueryTimeout,
)
from dalmatian.fingerprint import dataset_affinity_key
from dalmatian.jobs import JobQueue
from dalmatian.models import (
    DeliveryMode,
    ExplainResult,
    JobRecord,
    QueryRequest,
    SourceInspection,
    SourceSpec,
    StorageLocationInfo,
)
from dalmatian.observability import JOB_QUEUE_DEPTH, configure_logging
from dalmatian.service import QueryService
from dalmatian.storage import StorageRegistry


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    configure_logging(get_settings().log_level)
    yield
    await run_in_threadpool(shutdown_dependencies)


app = FastAPI(
    title="Dalmatian",
    version=__version__,
    description=(
        "Spark SQL over file datasets with reuse, async execution, "
        "and bounded result delivery."
    ),
    lifespan=lifespan,
)

QueryServiceDep = Annotated[QueryService, Depends(get_query_service)]
JobQueueDep = Annotated[JobQueue, Depends(get_job_queue)]
StorageDep = Annotated[StorageRegistry, Depends(get_storage_registry)]


@app.exception_handler(BusyError)
async def busy_handler(_request, exc: BusyError):
    from fastapi.responses import JSONResponse

    return JSONResponse(status_code=429, content={"detail": str(exc)}, headers={"Retry-After": "1"})


@app.exception_handler(QueryRejected)
async def rejected_handler(_request, exc: QueryRejected):
    from fastapi.responses import JSONResponse

    return JSONResponse(status_code=400, content={"detail": str(exc)})


@app.exception_handler(QueryTimeout)
async def timeout_handler(_request, exc: QueryTimeout):
    from fastapi.responses import JSONResponse

    return JSONResponse(status_code=408, content={"detail": str(exc)})


@app.exception_handler(QueryCancelled)
async def cancelled_handler(_request, exc: QueryCancelled):
    from fastapi.responses import JSONResponse

    return JSONResponse(status_code=409, content={"detail": str(exc)})


@app.exception_handler(IdempotencyConflict)
async def idempotency_handler(_request, exc: IdempotencyConflict):
    from fastapi.responses import JSONResponse

    return JSONResponse(status_code=409, content={"detail": str(exc)})


@app.get("/health/live")
def live() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/ready")
async def ready(service: QueryServiceDep, queue: JobQueueDep) -> dict[str, str]:
    try:
        checks = await run_in_threadpool(lambda: service.ready() and queue.ping())
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc
    if not checks:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="not ready")
    return {"status": "ok"}


@app.get("/metrics", include_in_schema=False)
def metrics() -> Response:
    try:
        queue = get_job_queue()
        depth = getattr(queue, "depth", None)
        if depth is not None:
            JOB_QUEUE_DEPTH.set(depth())
    except Exception:
        pass
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/v1/query")
async def query(request: QueryRequest, service: QueryServiceDep) -> Response:
    affinity = dataset_affinity_key(request)
    if request.output.mode == DeliveryMode.STREAM:
        media_type, iterator, query_id = await run_in_threadpool(service.stream, request)
        extension = "csv" if request.output.format.value == "csv" else "jsonl"
        return StreamingResponse(
            iterator,
            media_type=media_type,
            headers={
                "Content-Disposition": f'inline; filename="dalmatian-result.{extension}"',
                "X-Dalmatian-Query-Id": query_id,
                "X-Dalmatian-Result-Cache": "bypass",
                "X-Dalmatian-Affinity-Key": affinity,
            },
        )

    result = await run_in_threadpool(service.execute, request)
    headers = {
        "X-Dalmatian-Query-Id": result.query_id or "",
        "X-Dalmatian-Affinity-Key": affinity,
    }
    if getattr(result, "type", None) == "inline":
        headers["X-Dalmatian-Result-Cache"] = "hit" if result.cached else "miss"
    return Response(
        content=result.model_dump_json(),
        media_type="application/json",
        headers=headers,
    )


@app.post("/v1/query/affinity")
async def query_affinity(request: QueryRequest, service: QueryServiceDep) -> dict[str, str]:
    return {"key": await run_in_threadpool(service.affinity_key, request)}


@app.post("/v1/query/explain", response_model=ExplainResult)
async def explain(request: QueryRequest, service: QueryServiceDep) -> ExplainResult:
    return await run_in_threadpool(service.explain, request)


@app.post("/v1/sources/inspect", response_model=SourceInspection)
async def inspect_source(source: SourceSpec, service: QueryServiceDep) -> SourceInspection:
    return await run_in_threadpool(service.inspect, source)


@app.get("/v1/storage-locations", response_model=list[StorageLocationInfo])
async def storage_locations(storage_registry: StorageDep) -> list[StorageLocationInfo]:
    values = await run_in_threadpool(storage_registry.locations)
    return [StorageLocationInfo(name=name, uri=uri, source=source) for name, uri, source in values]


@app.post("/v1/jobs", response_model=JobRecord, status_code=status.HTTP_202_ACCEPTED)
async def create_job(
    request: QueryRequest,
    response: Response,
    queue: JobQueueDep,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> JobRecord:
    if request.output.mode == DeliveryMode.STREAM:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="async jobs do not support stream output; use inline or store",
        )
    job = await run_in_threadpool(queue.enqueue, request, idempotency_key)
    response.headers["Location"] = f"/v1/jobs/{job.id}"
    response.headers["X-Dalmatian-Affinity-Key"] = dataset_affinity_key(request)
    return job


@app.get("/v1/jobs/{job_id}", response_model=JobRecord)
async def get_job(job_id: str, queue: JobQueueDep) -> JobRecord:
    job = await run_in_threadpool(queue.get, job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job not found")
    return job


@app.delete("/v1/jobs/{job_id}", response_model=JobRecord)
async def cancel_job(job_id: str, queue: JobQueueDep) -> JobRecord:
    job = await run_in_threadpool(queue.request_cancel, job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job not found")
    return job
