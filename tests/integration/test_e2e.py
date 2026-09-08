import os
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from dalmatian.api import app
from dalmatian.cache import ValkeyResultCache
from dalmatian.dependencies import get_job_queue, get_query_service, get_storage_registry
from dalmatian.errors import CommitFenced
from dalmatian.execution import ExecutionContext
from dalmatian.jobs import ValkeyJobQueue
from dalmatian.models import JobStatus, QueryRequest
from dalmatian.service import QueryService
from dalmatian.spark import SparkEngine
from dalmatian.storage import StorageRegistry, ValkeyOutputPublicationRegistry
from dalmatian.worker import work_once

pytestmark = pytest.mark.integration


def test_http_to_worker_to_spark_to_stored_manifest(tmp_path: Path) -> None:
    url = os.getenv("DALMATIAN_TEST_VALKEY_URL")
    if not url:
        pytest.skip("DALMATIAN_TEST_VALKEY_URL is not set")

    data = tmp_path / "data"
    exports = tmp_path / "exports"
    data.mkdir()
    exports.mkdir()
    (data / "orders.csv").write_text("id,total\n1,10\n2,20\n")
    namespace = f"dalmatian:e2e:{time.time_ns()}"
    publication = ValkeyOutputPublicationRegistry(url)
    storage = StorageRegistry(
        {"local": exports.as_uri()},
        data_root=data,
        publication_registry=publication,
        publication_prefix=f"{namespace}:outputs",
    )
    queue = ValkeyJobQueue(
        url=url,
        queue_name=f"{namespace}:jobs",
        job_ttl_seconds=60,
        lease_seconds=10,
        max_attempts=2,
        affinity_shards=1,
    )
    engine = SparkEngine(
        master="local[2]",
        app_name="dalmatian-e2e",
        data_root=data,
        storage=storage,
        dataset_cache_entries=4,
        dataset_cache_ttl_seconds=60,
        spark_app_max_cores=2,
        spark_executor_cores=1,
        spark_executor_memory="1g",
    )
    service = QueryService(
        engine=engine,
        cache=ValkeyResultCache(url, prefix=f"{namespace}:cache"),
        default_cache_ttl_seconds=60,
        max_cache_ttl_seconds=60,
        result_row_limit=1000,
    )

    app.dependency_overrides[get_job_queue] = lambda: queue
    app.dependency_overrides[get_query_service] = lambda: service
    app.dependency_overrides[get_storage_registry] = lambda: storage
    try:
        body = {
            "sql": "select id, total from orders order by id",
            "sources": {
                "orders": {
                    "path": "orders.csv",
                    "format": "csv",
                    "schema": {"id": "long", "total": "long"},
                }
            },
            "output": {
                "mode": "store",
                "format": "parquet",
                "location": "local",
                "path": "daily/orders",
                "write_mode": "overwrite",
            },
        }
        with TestClient(app) as client:
            accepted = client.post(
                "/v1/jobs",
                json=body,
                headers={"Idempotency-Key": "daily-orders"},
            )
            assert accepted.status_code == 202
            job_id = accepted.json()["id"]
            assert work_once(queue, service, "worker-e2e", 1, 0.05, shard=0)
            finished = client.get(f"/v1/jobs/{job_id}")
            assert finished.status_code == 200
            payload = finished.json()
            assert payload["status"] == "succeeded"
            result = payload["result"]
            assert result["type"] == "stored"
            assert Path(result["manifest_uri"].removeprefix("file://")).exists()
            assert Path(result["data_uri"].removeprefix("file://")).exists()
    finally:
        app.dependency_overrides.clear()
        service.close()
        queue.close()


def test_failed_stored_attempt_cannot_publish_after_lease_moves(tmp_path: Path) -> None:
    url = os.getenv("DALMATIAN_TEST_VALKEY_URL")
    if not url:
        pytest.skip("DALMATIAN_TEST_VALKEY_URL is not set")

    data = tmp_path / "data"
    exports = tmp_path / "exports"
    data.mkdir()
    exports.mkdir()
    (data / "orders.csv").write_text("id,total\n1,10\n2,20\n")
    namespace = f"dalmatian:failure:{time.time_ns()}"
    publication = ValkeyOutputPublicationRegistry(url)
    storage = StorageRegistry(
        {"local": exports.as_uri()},
        data_root=data,
        publication_registry=publication,
        publication_prefix=f"{namespace}:outputs",
    )
    queue = ValkeyJobQueue(
        url=url,
        queue_name=f"{namespace}:jobs",
        job_ttl_seconds=60,
        lease_seconds=10,
        max_attempts=2,
        affinity_shards=1,
        retry_base_seconds=0.01,
        retry_max_seconds=0.01,
    )
    engine = SparkEngine(
        master="local[2]",
        app_name="dalmatian-failure-injection",
        data_root=data,
        storage=storage,
        dataset_cache_entries=4,
        dataset_cache_ttl_seconds=60,
    )
    service = QueryService(
        engine=engine,
        cache=ValkeyResultCache(url, prefix=f"{namespace}:cache"),
        default_cache_ttl_seconds=60,
        max_cache_ttl_seconds=60,
        result_row_limit=1000,
    )
    request = QueryRequest.model_validate(
        {
            "sql": "select id, total from orders order by id",
            "sources": {
                "orders": {
                    "path": "orders.csv",
                    "format": "csv",
                    "schema": {"id": "long", "total": "long"},
                }
            },
            "output": {
                "mode": "store",
                "format": "parquet",
                "location": "local",
                "path": "daily/orders",
                "write_mode": "overwrite",
            },
        }
    )

    try:
        created = queue.enqueue(request)
        first = queue.reserve("worker-a", timeout_seconds=1, shard=0)
        assert first is not None
        attempt_a = "1-worker-a-dies"
        context_a = ExecutionContext(
            query_id=created.id,
            attempt_id=attempt_a,
            attempt_number=1,
            timeout_seconds=60,
            can_commit=lambda: False,
            publish_output=lambda key, manifest, mode: queue.publish_output(
                created.id, "worker-a", 1, key, attempt_a, manifest, mode
            ),
            kind="async",
        )
        with pytest.raises(CommitFenced):
            service.execute(first.request, context_a)

        attempt_a_data = exports / "daily/orders/_dalmatian_data" / created.id / attempt_a
        assert attempt_a_data.exists()
        queue.client.hset(queue._key(created.id), "lease_expires_ms", "0")
        assert queue.reap_stale() == 1
        queue.client.zadd(queue.retry_name, {created.id: 0})
        queue.promote_retries()

        second = queue.reserve("worker-b", timeout_seconds=1, shard=0)
        assert second is not None
        attempt_b = "2-worker-b"
        context_b = ExecutionContext(
            query_id=created.id,
            attempt_id=attempt_b,
            attempt_number=2,
            timeout_seconds=60,
            can_commit=lambda: queue.heartbeat(created.id, "worker-b"),
            publish_output=lambda key, manifest, mode: queue.publish_output(
                created.id, "worker-b", 2, key, attempt_b, manifest, mode
            ),
            kind="async",
        )
        result = service.execute(second.request, context_b)
        assert queue.complete(created.id, "worker-b", result)

        logical = storage.resolve("local", "daily/orders")
        current = publication.current(storage.publication_key(logical))
        assert current is not None
        assert current["attempt_id"] == attempt_b
        assert attempt_a not in current["manifest_uri"]
        assert queue.get(created.id).status == JobStatus.SUCCEEDED
    finally:
        service.close()
        queue.close()
