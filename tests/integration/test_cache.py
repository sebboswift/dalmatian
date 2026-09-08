import os
import threading
import time

import pytest

from dalmatian.cache import ValkeyResultCache
from dalmatian.datasets import PreparedQuery, ResolvedSource
from dalmatian.execution import ExecutionContext
from dalmatian.models import DataFormat, InlineResult, QueryRequest, SourceSpec
from dalmatian.service import QueryService

pytestmark = pytest.mark.integration


def cache(max_bytes: int = 4 * 1024 * 1024) -> ValkeyResultCache:
    url = os.getenv("DALMATIAN_TEST_VALKEY_URL")
    if not url:
        pytest.skip("DALMATIAN_TEST_VALKEY_URL is not set")
    return ValkeyResultCache(
        url,
        prefix=f"dalmatian:test:cache:{time.time_ns()}",
        max_bytes=max_bytes,
    )


def test_corrupted_and_oversized_cache_entries_fail_safe() -> None:
    subject = cache(max_bytes=32)
    result = InlineResult(columns=["value"], rows=[{"value": "x" * 1000}], truncated=False)
    try:
        assert subject.set("oversized", result, 60) is False
        subject.client.set(subject._key("corrupted"), b"not-zlib")
        assert subject.get("corrupted") is None
        assert subject.client.exists(subject._key("corrupted")) == 0
    finally:
        subject.close()


def test_cache_entry_expires_at_configured_ttl() -> None:
    subject = cache()
    result = InlineResult(columns=["value"], rows=[{"value": 1}], truncated=False)
    try:
        assert subject.set("short-lived", result, 1) is True
        assert subject.get("short-lived") is not None
        deadline = time.monotonic() + 2.5
        while time.monotonic() < deadline and subject.get("short-lived") is not None:
            time.sleep(0.05)
        assert subject.get("short-lived") is None
    finally:
        subject.close()


class CountingEngine:
    def __init__(self) -> None:
        self.calls = 0
        self._lock = threading.Lock()

    def prepare(self, request: QueryRequest, normalized_sql: str) -> PreparedQuery:
        return PreparedQuery(
            request=request,
            normalized_sql=normalized_sql,
            sources={
                "orders": ResolvedSource(
                    uri="file:///data/orders.csv",
                    fingerprint="v1",
                    cache_safe=True,
                )
            },
        )

    def execute(self, prepared: PreparedQuery, context: ExecutionContext) -> InlineResult:
        with self._lock:
            self.calls += 1
        time.sleep(0.2)
        return InlineResult(columns=["value"], rows=[{"value": 1}], truncated=False)

    def cancel(self, query_id: str) -> None:
        return None

    def ping(self) -> bool:
        return True


def test_twenty_identical_callers_share_one_execution() -> None:
    shared_cache = cache()
    engine = CountingEngine()
    service = QueryService(
        engine=engine,  # type: ignore[arg-type]
        cache=shared_cache,
        default_cache_ttl_seconds=60,
        max_cache_ttl_seconds=60,
        result_row_limit=1000,
        max_concurrent_queries=20,
        admission_wait_seconds=1,
        singleflight_wait_seconds=5,
    )
    request = QueryRequest(
        sql="select * from orders",
        sources={"orders": SourceSpec(path="orders.csv", format=DataFormat.CSV)},
    )
    barrier = threading.Barrier(21)
    results: list[InlineResult] = []

    def call() -> None:
        barrier.wait()
        results.append(service.execute(request))

    threads = [threading.Thread(target=call) for _ in range(20)]
    try:
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join()
        assert engine.calls == 1
        assert len(results) == 20
        assert sum(result.cached for result in results) == 19
    finally:
        service.close()
