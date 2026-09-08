from __future__ import annotations

import logging
import re
import signal
import socket
import threading
import time
import uuid

from prometheus_client import start_http_server

from dalmatian.config import get_settings
from dalmatian.dependencies import get_job_queue, get_query_service
from dalmatian.errors import (
    CommitFenced,
    QueryCancelled,
    QueryRejected,
    QueryTimeout,
    RetryableQueryError,
)
from dalmatian.execution import ExecutionContext
from dalmatian.jobs import JobQueue
from dalmatian.observability import (
    JOB_OUTCOMES,
    JOB_QUEUE_DEPTH,
    STALE_RECOVERIES,
    configure_logging,
)
from dalmatian.service import QueryService

logger = logging.getLogger(__name__)


class LeaseHeartbeat:
    def __init__(
        self,
        queue: JobQueue,
        service: QueryService,
        job_id: str,
        worker_id: str,
        interval_seconds: float,
    ) -> None:
        self.queue = queue
        self.service = service
        self.job_id = job_id
        self.worker_id = worker_id
        self.interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name=f"lease-{job_id}", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=self.interval_seconds + 1)

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            try:
                if not self.queue.heartbeat(self.job_id, self.worker_id):
                    self.service.cancel(self.job_id)
                    logger.warning("job lease lost", extra={"job_id": self.job_id})
                    return
                if self.queue.cancel_requested(self.job_id, self.worker_id):
                    self.service.cancel(self.job_id)
                    return
            except Exception:
                logger.exception("job heartbeat failed", extra={"job_id": self.job_id})


def is_retryable(exc: Exception) -> bool:
    if isinstance(exc, (QueryRejected, QueryTimeout, ValueError, FileExistsError)):
        return False
    if isinstance(exc, (RetryableQueryError, OSError, ConnectionError)):
        return True
    text = f"{type(exc).__name__}: {exc}".lower()
    return any(
        needle in text
        for needle in (
            "py4jnetworkerror",
            "executorlostfailure",
            "fetchfailed",
            "connection refused",
            "temporarily unavailable",
            "connection reset",
        )
    )


def work_once(
    queue: JobQueue,
    service: QueryService,
    worker_id: str,
    reserve_timeout_seconds: int,
    heartbeat_interval_seconds: float,
    shard: int = 0,
) -> bool:
    job = queue.reserve(worker_id, reserve_timeout_seconds, shard=shard)
    if job is None:
        return False

    heartbeat = LeaseHeartbeat(queue, service, job.id, worker_id, heartbeat_interval_seconds)
    heartbeat.start()
    attempt_id = f"{job.attempts}-{uuid.uuid4().hex}"

    def can_commit() -> bool:
        return queue.heartbeat(job.id, worker_id) and not queue.cancel_requested(
            job.id, worker_id
        )

    context = ExecutionContext(
        query_id=job.id,
        attempt_id=attempt_id,
        attempt_number=job.attempts,
        timeout_seconds=job.request.timeout_seconds or 300,
        can_commit=can_commit,
        publish_output=lambda publication_key, manifest_uri, write_mode: queue.publish_output(
            job.id,
            worker_id,
            job.attempts,
            publication_key,
            attempt_id,
            manifest_uri,
            write_mode,
        ),
        kind="async",
    )
    try:
        result = service.execute(job.request, context=context)
    except QueryCancelled:
        heartbeat.stop()
        if queue.cancel_requested(job.id, worker_id):
            queue.cancel_complete(job.id, worker_id)
            JOB_OUTCOMES.labels(status="cancelled").inc()
        elif queue.owns(job.id, worker_id):
            queue.fail(
                job.id,
                worker_id,
                "query was cancelled",
                retryable=False,
                error_class="QueryCancelled",
            )
            JOB_OUTCOMES.labels(status="failed").inc()
    except CommitFenced:
        heartbeat.stop()
        if queue.cancel_requested(job.id, worker_id):
            if queue.cancel_complete(job.id, worker_id):
                JOB_OUTCOMES.labels(status="cancelled").inc()
        else:
            logger.warning(
                "stale worker fenced from output commit",
                extra={"job_id": job.id, "attempt": job.attempts},
            )
    except Exception as exc:
        heartbeat.stop()
        logger.exception("job failed", extra={"job_id": job.id, "attempt": job.attempts})
        if queue.owns(job.id, worker_id):
            queue.fail(
                job.id,
                worker_id,
                str(exc),
                retryable=is_retryable(exc),
                error_class=type(exc).__name__,
            )
    else:
        heartbeat.stop()
        if queue.cancel_requested(job.id, worker_id):
            if queue.cancel_complete(job.id, worker_id):
                JOB_OUTCOMES.labels(status="cancelled").inc()
        elif queue.complete(job.id, worker_id, result):
            JOB_OUTCOMES.labels(status="succeeded").inc()
        elif queue.cancel_requested(job.id, worker_id):
            if queue.cancel_complete(job.id, worker_id):
                JOB_OUTCOMES.labels(status="cancelled").inc()
        else:
            logger.warning(
                "job completion fenced",
                extra={"job_id": job.id, "attempt": job.attempts},
            )
    return True


def _worker_shard(hostname: str, configured: int | None, shards: int) -> int:
    if configured is not None:
        return configured % shards
    match = re.search(r"(\d+)$", hostname)
    if match:
        value = int(match.group(1))
        # StatefulSet ordinals are zero-based. Docker Compose scale suffixes are one-based.
        if "-worker-" in hostname or "_worker_" in hostname:
            value = max(0, value - 1)
        return value % shards
    return 0


def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    queue = get_job_queue()
    service = get_query_service()
    hostname = socket.gethostname()
    worker_id = f"{hostname}:{uuid.uuid4().hex[:8]}"
    shard = _worker_shard(hostname, settings.worker_affinity_shard, settings.worker_affinity_shards)
    stop = threading.Event()

    def request_stop(signum: int, _frame: object) -> None:
        logger.info("worker draining", extra={"signal": signum})
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    heartbeat_interval = max(1.0, settings.job_lease_seconds / 3)
    start_http_server(settings.worker_metrics_port, addr=settings.worker_metrics_host)
    next_reap = 0.0
    logger.info(
        "worker started",
        extra={
            "worker_id": worker_id,
            "shard": shard,
            "metrics_port": settings.worker_metrics_port,
        },
    )
    try:
        while not stop.is_set():
            now = time.monotonic()
            if now >= next_reap:
                try:
                    reaped = queue.reap_stale()
                    depth = getattr(queue, "depth", None)
                    if depth is not None:
                        JOB_QUEUE_DEPTH.set(depth())
                    if reaped:
                        STALE_RECOVERIES.inc(reaped)
                        logger.info("stale jobs recovered", extra={"count": reaped})
                except Exception:
                    logger.exception("stale job recovery failed")
                next_reap = now + settings.worker_reap_interval_seconds

            work_once(
                queue=queue,
                service=service,
                worker_id=worker_id,
                reserve_timeout_seconds=settings.worker_reserve_timeout_seconds,
                heartbeat_interval_seconds=heartbeat_interval,
                shard=shard,
            )
    finally:
        service.close()
        close_queue = getattr(queue, "close", None)
        if close_queue is not None:
            close_queue()
    logger.info("worker stopped", extra={"worker_id": worker_id})


if __name__ == "__main__":
    run()
