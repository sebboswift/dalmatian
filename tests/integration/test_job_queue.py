import os
import threading
import time

import pytest

from dalmatian.jobs import ValkeyJobQueue
from dalmatian.models import (
    DataFormat,
    InlineResult,
    JobStatus,
    OutputFormat,
    OutputSpec,
    QueryRequest,
    SourceSpec,
)

pytestmark = pytest.mark.integration


def queue() -> ValkeyJobQueue:
    url = os.getenv("DALMATIAN_TEST_VALKEY_URL")
    if not url:
        pytest.skip("DALMATIAN_TEST_VALKEY_URL is not set")
    name = f"dalmatian:test:{time.time_ns()}"
    return ValkeyJobQueue(
        url,
        name,
        job_ttl_seconds=60,
        lease_seconds=10,
        max_attempts=2,
        affinity_shards=2,
        retry_base_seconds=0.01,
        retry_max_seconds=0.01,
    )


def request() -> QueryRequest:
    return QueryRequest(
        sql="select * from orders",
        sources={"orders": SourceSpec(path="orders.csv", format=DataFormat.CSV)},
    )


def test_expired_lease_is_retried_and_stale_owner_cannot_complete() -> None:
    jobs = queue()
    created = jobs.enqueue(request())
    shard = jobs._shard(request())
    first = jobs.reserve("worker-1", timeout_seconds=1, shard=shard)
    assert first is not None
    assert first.attempts == 1

    jobs.client.hset(jobs._key(created.id), "lease_expires_ms", "0")
    assert jobs.reap_stale() == 1
    assert jobs.get(created.id).status in {JobStatus.RETRYING, JobStatus.QUEUED}
    jobs.client.zadd(jobs.retry_name, {created.id: 0})
    jobs.promote_retries()

    second = jobs.reserve("worker-2", timeout_seconds=1, shard=shard)
    assert second is not None
    assert second.attempts == 2

    result = InlineResult(columns=["value"], rows=[{"value": 1}], truncated=False)
    assert jobs.complete(created.id, "worker-1", result) is False
    assert jobs.complete(created.id, "worker-2", result) is True
    assert jobs.get(created.id).status == JobStatus.SUCCEEDED


def test_queued_job_can_be_cancelled_without_worker() -> None:
    jobs = queue()
    created = jobs.enqueue(request())
    cancelled = jobs.request_cancel(created.id)
    assert cancelled is not None
    assert cancelled.status == JobStatus.CANCELLED


def test_idempotency_key_replays_same_job_and_rejects_new_payload() -> None:
    from dalmatian.errors import IdempotencyConflict

    jobs = queue()
    first = jobs.enqueue(request(), idempotency_key="daily-orders")
    second = jobs.enqueue(request(), idempotency_key="daily-orders")
    assert second.id == first.id

    changed = request().model_copy(update={"sql": "select count(*) from orders"})
    with pytest.raises(IdempotencyConflict):
        jobs.enqueue(changed, idempotency_key="daily-orders")


def test_concurrent_idempotent_submissions_create_exactly_one_job() -> None:
    jobs = queue()
    barrier = threading.Barrier(21)
    job_ids: list[str] = []
    failures: list[Exception] = []

    def submit() -> None:
        barrier.wait()
        try:
            job_ids.append(jobs.enqueue(request(), idempotency_key="concurrent-daily").id)
        except Exception as exc:
            failures.append(exc)

    threads = [threading.Thread(target=submit) for _ in range(20)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join()

    assert failures == []
    assert len(job_ids) == 20
    assert len(set(job_ids)) == 1
    assert jobs.depth() == 1


def test_retained_idempotency_binding_never_silently_recreates_expired_job() -> None:
    from dalmatian.errors import IdempotencyConflict

    jobs = queue()
    first = jobs.enqueue(request(), idempotency_key="retained-binding")
    jobs.client.delete(jobs._key(first.id))

    with pytest.raises(IdempotencyConflict, match="job record has expired"):
        jobs.enqueue(request(), idempotency_key="retained-binding")


def test_exhausted_retry_is_failed_once_and_moved_to_dead_letter_queue() -> None:
    jobs = queue()
    created = jobs.enqueue(request())
    shard = jobs._shard(request())

    first = jobs.reserve("worker-1", timeout_seconds=1, shard=shard)
    assert first is not None
    assert jobs.fail(created.id, "worker-1", "temporary outage", retryable=True)
    assert jobs.get(created.id).status == JobStatus.RETRYING
    jobs.client.zadd(jobs.retry_name, {created.id: 0})
    assert jobs.promote_retries() == 1

    second = jobs.reserve("worker-2", timeout_seconds=1, shard=shard)
    assert second is not None
    assert second.attempts == 2
    assert jobs.fail(created.id, "worker-2", "still unavailable", retryable=True)

    final = jobs.get(created.id)
    assert final is not None
    assert final.status == JobStatus.FAILED
    assert final.attempts == 2
    assert final.error == "still unavailable"
    assert jobs.client.lrange(jobs.dead_name, 0, -1).count(created.id) == 1
    assert jobs.client.zscore(jobs.retry_name, created.id) is None
    assert jobs.client.lpos(jobs.processing_name, created.id) is None


def test_output_publication_is_fenced_by_current_attempt() -> None:
    jobs = queue()
    created = jobs.enqueue(request())
    shard = jobs._shard(request())
    first = jobs.reserve("worker-1", timeout_seconds=1, shard=shard)
    assert first is not None
    publication_key = f"dalmatian:test:output:{created.id}"

    assert jobs.publish_output(
        created.id,
        "worker-1",
        first.attempts,
        publication_key,
        "attempt-1",
        "file:///tmp/attempt-1.json",
        "overwrite",
    ) == 1

    jobs.client.hset(jobs._key(created.id), "lease_expires_ms", "0")
    assert jobs.reap_stale() == 1
    jobs.client.zadd(jobs.retry_name, {created.id: 0})
    jobs.promote_retries()
    second = jobs.reserve("worker-2", timeout_seconds=1, shard=shard)
    assert second is not None

    assert jobs.publish_output(
        created.id,
        "worker-1",
        first.attempts,
        publication_key,
        "attempt-stale",
        "file:///tmp/stale.json",
        "overwrite",
    ) == -1
    assert jobs.publish_output(
        created.id,
        "worker-2",
        second.attempts,
        publication_key,
        "attempt-2",
        "file:///tmp/attempt-2.json",
        "overwrite",
    ) == 1
    assert jobs.client.hget(publication_key, "manifest_uri") == "file:///tmp/attempt-2.json"


def test_cancelled_running_job_is_not_retried_after_worker_loss() -> None:
    jobs = queue()
    created = jobs.enqueue(request())
    shard = jobs._shard(request())
    reserved = jobs.reserve("worker-1", timeout_seconds=1, shard=shard)
    assert reserved is not None

    cancelling = jobs.request_cancel(created.id)
    assert cancelling is not None
    assert cancelling.status == JobStatus.RUNNING

    jobs.client.hset(jobs._key(created.id), "lease_expires_ms", "0")
    assert jobs.reap_stale() == 1
    final = jobs.get(created.id)
    assert final is not None
    assert final.status == JobStatus.CANCELLED


def test_stored_request_round_trips_through_queue() -> None:
    jobs = queue()
    stored = request().model_copy(
        update={
            "output": OutputSpec(
                mode="store",
                format=OutputFormat.PARQUET,
                location="local",
                path="daily/orders",
            )
        }
    )
    created = jobs.enqueue(stored)
    reserved = jobs.reserve("worker-1", timeout_seconds=1, shard=jobs._shard(stored))
    assert reserved is not None
    assert reserved.id == created.id
    assert reserved.request.output.mode.value == "store"


def test_queue_depth_limit_is_atomic_under_concurrent_submission() -> None:
    jobs = queue()
    jobs.max_queue_depth = 1
    barrier = threading.Barrier(3)
    outcomes: list[str] = []

    def submit() -> None:
        barrier.wait()
        try:
            jobs.enqueue(request())
        except Exception as exc:
            outcomes.append(type(exc).__name__)
        else:
            outcomes.append("created")

    threads = [threading.Thread(target=submit) for _ in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join()

    assert sorted(outcomes) == ["BusyError", "created"]
    assert jobs.depth() == 1


def test_expired_owner_is_fenced_before_reaping() -> None:
    jobs = queue()
    created = jobs.enqueue(request())
    reserved = jobs.reserve("worker-1", timeout_seconds=1, shard=jobs._shard(request()))
    assert reserved is not None
    jobs.client.hset(jobs._key(created.id), "lease_expires_ms", "0")

    result = InlineResult(columns=["value"], rows=[{"value": 1}], truncated=False)
    assert jobs.heartbeat(created.id, "worker-1") is False
    assert jobs.owns(created.id, "worker-1") is False
    assert jobs.complete(created.id, "worker-1", result) is False
    assert jobs.publish_output(
        created.id,
        "worker-1",
        reserved.attempts,
        f"dalmatian:test:expired:{created.id}",
        "attempt-expired",
        "file:///tmp/expired.json",
        "overwrite",
    ) == -1
