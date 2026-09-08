from collections.abc import Iterator

from fastapi.testclient import TestClient

from dalmatian.api import app
from dalmatian.dependencies import get_job_queue, get_query_service, get_storage_registry
from dalmatian.errors import BusyError, QueryRejected
from dalmatian.jobs import ReservedJob
from dalmatian.models import (
    ExplainResult,
    InlineResult,
    JobRecord,
    JobStatus,
    QueryRequest,
    QueryResult,
    SourceInspection,
)


class FakeService:
    def execute(self, request: QueryRequest) -> QueryResult:
        return InlineResult(
            query_id="query-1",
            columns=["answer"],
            rows=[{"answer": 42}],
            truncated=False,
        )

    def stream(self, request: QueryRequest) -> tuple[str, Iterator[str], str]:
        return "text/csv", iter(["answer\n", "42\n"]), "query-stream"

    def explain(self, request: QueryRequest) -> ExplainResult:
        return ExplainResult(
            query_id="query-explain",
            normalized_sql=request.sql,
            source_versions={"orders": "v1"},
            plan="Project [42]",
        )

    def inspect(self, _source) -> SourceInspection:
        return SourceInspection(
            location="local",
            resolved_uri="file:///data/orders.csv",
            fingerprint="v1",
            cache_safe=True,
            columns=[],
        )

    def affinity_key(self, request: QueryRequest) -> str:
        from dalmatian.fingerprint import dataset_affinity_key

        return dataset_affinity_key(request)

    def ready(self) -> bool:
        return True


class FakeQueue:
    def enqueue(
        self, request: QueryRequest, idempotency_key: str | None = None
    ) -> JobRecord:
        return JobRecord(id="job-1", status=JobStatus.QUEUED)

    def get(self, job_id: str) -> JobRecord | None:
        if job_id == "missing":
            return None
        return JobRecord(id=job_id, status=JobStatus.SUCCEEDED)

    def request_cancel(self, job_id: str) -> JobRecord | None:
        if job_id == "missing":
            return None
        return JobRecord(id=job_id, status=JobStatus.CANCELLED)

    def reserve(
        self,
        worker_id: str,
        timeout_seconds: int = 5,
        shard: int = 0,
    ) -> ReservedJob | None:
        return None

    def heartbeat(self, job_id: str, worker_id: str) -> bool:
        return True

    def owns(self, job_id: str, worker_id: str) -> bool:
        return True

    def cancel_requested(self, job_id: str, worker_id: str) -> bool:
        return False

    def complete(self, job_id: str, worker_id: str, result: QueryResult) -> bool:
        return True

    def publish_output(
        self,
        job_id: str,
        worker_id: str,
        attempt_number: int,
        publication_key: str,
        attempt_id: str,
        manifest_uri: str,
        write_mode: str,
    ) -> int:
        return 1

    def cancel_complete(self, job_id: str, worker_id: str) -> bool:
        return True

    def fail(
        self,
        job_id: str,
        worker_id: str,
        error: str,
        retryable: bool = False,
        error_class: str | None = None,
    ) -> bool:
        return True

    def reap_stale(self) -> int:
        return 0

    def promote_retries(self) -> int:
        return 0

    def ping(self) -> bool:
        return True


class FakeStorage:
    def locations(self):
        return [("local", "file:///exports", "config")]


class BusyService(FakeService):
    def execute(self, request: QueryRequest) -> QueryResult:
        raise BusyError("query concurrency limit reached")


class RejectedService(FakeService):
    def execute(self, request: QueryRequest) -> QueryResult:
        raise QueryRejected("query analysis failed: unresolved column")


class UnreadyQueue(FakeQueue):
    def ping(self) -> bool:
        return False


def payload() -> dict[str, object]:
    return {
        "sql": "select 42 as answer from orders limit 1",
        "sources": {"orders": {"path": "example/orders.csv", "format": "csv"}},
    }


def setup_module() -> None:
    app.dependency_overrides[get_query_service] = lambda: FakeService()
    app.dependency_overrides[get_job_queue] = lambda: FakeQueue()
    app.dependency_overrides[get_storage_registry] = lambda: FakeStorage()


def teardown_module() -> None:
    app.dependency_overrides.clear()


def test_sync_inline_query() -> None:
    response = TestClient(app).post("/v1/query", json=payload())
    assert response.status_code == 200
    assert response.json()["rows"] == [{"answer": 42}]
    assert response.headers["x-dalmatian-query-id"] == "query-1"
    assert "row_count" not in response.json()


def test_sync_csv_stream() -> None:
    body = payload() | {"output": {"mode": "stream", "format": "csv"}}
    response = TestClient(app).post("/v1/query", json=body)
    assert response.status_code == 200
    assert response.text == "answer\n42\n"
    assert response.headers["x-dalmatian-query-id"] == "query-stream"
    assert response.headers["x-dalmatian-result-cache"] == "bypass"


def test_async_query_is_accepted() -> None:
    response = TestClient(app).post("/v1/jobs", json=payload())
    assert response.status_code == 202
    assert response.headers["location"] == "/v1/jobs/job-1"
    assert response.json()["status"] == "queued"
    assert response.json()["attempts"] == 0


def test_async_stream_is_rejected() -> None:
    body = payload() | {"output": {"mode": "stream", "format": "jsonl"}}
    response = TestClient(app).post("/v1/jobs", json=body)
    assert response.status_code == 400
    assert "do not support stream" in response.json()["detail"]


def test_job_can_be_cancelled() -> None:
    response = TestClient(app).delete("/v1/jobs/job-1")
    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"


def test_explain_endpoint() -> None:
    response = TestClient(app).post("/v1/query/explain", json=payload())
    assert response.status_code == 200
    assert response.json()["plan"] == "Project [42]"


def test_storage_locations_endpoint() -> None:
    response = TestClient(app).get("/v1/storage-locations")
    assert response.status_code == 200
    assert response.json() == [{"name": "local", "uri": "file:///exports", "source": "config"}]


def test_missing_job_is_404() -> None:
    response = TestClient(app).get("/v1/jobs/missing")
    assert response.status_code == 404


def test_affinity_endpoint_returns_stable_dataset_key() -> None:
    client = TestClient(app)
    first = client.post("/v1/query/affinity", json=payload())
    second_body = payload() | {"sql": "select count(*) from orders"}
    second = client.post("/v1/query/affinity", json=second_body)
    assert first.status_code == 200
    assert first.json()["key"] == second.json()["key"]


def test_sync_response_exposes_affinity_key() -> None:
    response = TestClient(app).post("/v1/query", json=payload())
    assert response.headers["x-dalmatian-affinity-key"]


def test_concurrency_backpressure_is_429_with_retry_after() -> None:
    original = app.dependency_overrides[get_query_service]
    app.dependency_overrides[get_query_service] = lambda: BusyService()
    try:
        response = TestClient(app).post("/v1/query", json=payload())
    finally:
        app.dependency_overrides[get_query_service] = original
    assert response.status_code == 429
    assert response.headers["retry-after"] == "1"


def test_query_analysis_rejection_is_400() -> None:
    original = app.dependency_overrides[get_query_service]
    app.dependency_overrides[get_query_service] = lambda: RejectedService()
    try:
        response = TestClient(app).post("/v1/query", json=payload())
    finally:
        app.dependency_overrides[get_query_service] = original
    assert response.status_code == 400
    assert response.json() == {"detail": "query analysis failed: unresolved column"}


def test_failed_dependency_readiness_is_503() -> None:
    original = app.dependency_overrides[get_job_queue]
    app.dependency_overrides[get_job_queue] = lambda: UnreadyQueue()
    try:
        response = TestClient(app).get("/health/ready")
    finally:
        app.dependency_overrides[get_job_queue] = original
    assert response.status_code == 503
    assert response.json() == {"detail": "not ready"}
