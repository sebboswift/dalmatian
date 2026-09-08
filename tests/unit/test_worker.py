from dalmatian.errors import CommitFenced
from dalmatian.execution import ExecutionContext
from dalmatian.jobs import ReservedJob
from dalmatian.models import DataFormat, InlineResult, QueryRequest, SourceSpec
from dalmatian.worker import is_retryable, work_once


class FakeQueue:
    def __init__(self) -> None:
        self.completed = False
        self.failed = False
        self.cancelled = False
        self.cancel_requested_value = False
        self.heartbeats = 0

    def reserve(
        self,
        worker_id: str,
        timeout_seconds: int = 5,
        shard: int = 0,
    ) -> ReservedJob | None:
        request = QueryRequest(
            sql="select * from orders limit 1",
            sources={"orders": SourceSpec(path="orders.csv", format=DataFormat.CSV)},
        )
        return ReservedJob(id="job-1", request=request, attempts=1, shard=shard)

    def heartbeat(self, job_id: str, worker_id: str) -> bool:
        self.heartbeats += 1
        return True

    def owns(self, job_id: str, worker_id: str) -> bool:
        return True

    def cancel_requested(self, job_id: str, worker_id: str) -> bool:
        return self.cancel_requested_value

    def complete(self, job_id: str, worker_id: str, result: InlineResult) -> bool:
        self.completed = True
        return True

    def cancel_complete(self, job_id: str, worker_id: str) -> bool:
        self.cancelled = True
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

    def fail(
        self,
        job_id: str,
        worker_id: str,
        error: str,
        retryable: bool = False,
        error_class: str | None = None,
    ) -> bool:
        self.failed = True
        return True


class FakeService:
    def execute(
        self,
        request: QueryRequest,
        context: ExecutionContext | None = None,
    ) -> InlineResult:
        return InlineResult(columns=["value"], rows=[{"value": 1}], truncated=False)

    def cancel(self, query_id: str) -> None:
        return None


class CommitFencedService(FakeService):
    def execute(
        self,
        request: QueryRequest,
        context: ExecutionContext | None = None,
    ) -> InlineResult:
        raise CommitFenced("output commit fenced")


def test_worker_completes_owned_job() -> None:
    queue = FakeQueue()
    worked = work_once(
        queue=queue,  # type: ignore[arg-type]
        service=FakeService(),  # type: ignore[arg-type]
        worker_id="worker-1",
        reserve_timeout_seconds=1,
        heartbeat_interval_seconds=0.01,
    )
    assert worked is True
    assert queue.completed is True
    assert queue.failed is False


def test_network_failures_are_retryable_but_validation_is_not() -> None:
    assert is_retryable(ConnectionError("connection reset")) is True
    assert is_retryable(ValueError("bad SQL")) is False


def test_commit_fence_finishes_requested_cancellation() -> None:
    queue = FakeQueue()
    queue.cancel_requested_value = True
    worked = work_once(
        queue=queue,  # type: ignore[arg-type]
        service=CommitFencedService(),  # type: ignore[arg-type]
        worker_id="worker-1",
        reserve_timeout_seconds=1,
        heartbeat_interval_seconds=0.01,
    )
    assert worked is True
    assert queue.cancelled is True
    assert queue.completed is False
    assert queue.failed is False
