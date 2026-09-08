import threading
from collections.abc import Iterator

import pytest

from dalmatian.cache import MemoryCache, NullCache
from dalmatian.datasets import PreparedQuery, ResolvedSource
from dalmatian.execution import ExecutionContext
from dalmatian.models import (
    DataFormat,
    InlineResult,
    OutputFormat,
    OutputSpec,
    QueryRequest,
    SourceInspection,
    SourceSpec,
)
from dalmatian.service import QueryService


class BusySingleFlightCache(NullCache):
    def acquire(self, key: str, ttl_seconds: int) -> str | None:
        return None

    def wait(self, key: str, timeout_seconds: float) -> InlineResult | None:
        return None


class TrackingMetadata:
    def __init__(self) -> None:
        self.statuses: list[str] = []

    def record_invocation(self, invocation_id: str, kind: str, request_json: str) -> None:
        self.statuses.append("running")

    def update_invocation(self, invocation_id: str, status: str, **kwargs) -> None:
        self.statuses.append(status)


class FakeEngine:
    def __init__(self) -> None:
        self.calls = 0
        self.prepare_calls = 0
        self.source_version = "v1"
        self.cache_safe = True

    def prepare(self, request: QueryRequest, normalized_sql: str) -> PreparedQuery:
        self.prepare_calls += 1
        return PreparedQuery(
            request=request,
            normalized_sql=normalized_sql,
            sources={
                "orders": ResolvedSource(
                    uri="file:///data/orders.csv",
                    fingerprint=self.source_version,
                    cache_safe=self.cache_safe,
                )
            },
        )

    def execute(self, prepared: PreparedQuery, context: ExecutionContext) -> InlineResult:
        self.calls += 1
        return InlineResult(
            query_id=context.query_id,
            columns=["value"],
            rows=[{"value": 1}],
            truncated=False,
        )

    def stream(
        self,
        prepared: PreparedQuery,
        context: ExecutionContext,
    ) -> tuple[str, Iterator[str]]:
        return "text/csv", iter(["value\n", "1\n"])

    def explain(self, prepared: PreparedQuery, context: ExecutionContext) -> str:
        return "plan"

    def inspect(self, source: SourceSpec) -> SourceInspection:
        raise NotImplementedError

    def cancel(self, query_id: str) -> None:
        return None

    def ping(self) -> bool:
        return True


class BlockingEngine(FakeEngine):
    def __init__(self) -> None:
        super().__init__()
        self.started = threading.Event()
        self.release = threading.Event()

    def execute(self, prepared: PreparedQuery, context: ExecutionContext) -> InlineResult:
        self.started.set()
        assert self.release.wait(timeout=2)
        return super().execute(prepared, context)


def query_request(**updates: object) -> QueryRequest:
    request = QueryRequest(
        sql="select * from orders",
        sources={"orders": SourceSpec(path="orders.csv", format=DataFormat.CSV)},
    )
    return request.model_copy(update=updates)


def service(engine: FakeEngine, cache=None, **kwargs) -> QueryService:
    return QueryService(
        engine,
        cache or MemoryCache(),
        60,
        300,
        1000,
        singleflight_wait_seconds=0.05,
        **kwargs,
    )


def test_second_identical_query_uses_shared_result_cache() -> None:
    engine = FakeEngine()
    subject = service(engine)
    first = subject.execute(query_request())
    second = subject.execute(query_request())
    assert first.cached is False
    assert second.cached is True
    assert engine.calls == 1


def test_declared_dataset_version_skips_spark_execution_on_exact_cache_hit() -> None:
    engine = FakeEngine()
    subject = service(engine)
    request = QueryRequest(
        sql="select * from orders",
        sources={
            "orders": SourceSpec(
                path="orders.csv",
                format=DataFormat.CSV,
                version="2026-09-07",
            )
        },
    )
    subject.execute(request)
    second = subject.execute(request)
    assert second.cached is True
    assert engine.prepare_calls == 2
    assert engine.calls == 1


def test_file_version_change_invalidates_result_cache() -> None:
    engine = FakeEngine()
    subject = service(engine)
    subject.execute(query_request())
    engine.source_version = "v2"
    second = subject.execute(query_request())
    assert second.cached is False
    assert engine.calls == 2


def test_uncacheable_remote_source_skips_exact_result_cache() -> None:
    engine = FakeEngine()
    engine.cache_safe = False
    subject = service(engine)
    subject.execute(query_request())
    subject.execute(query_request())
    assert engine.calls == 2


def test_zero_ttl_disables_result_cache() -> None:
    engine = FakeEngine()
    subject = service(engine)
    request = query_request(cache_ttl_seconds=0)
    subject.execute(request)
    subject.execute(request)
    assert engine.calls == 2


def test_inline_result_row_limit_is_enforced() -> None:
    subject = QueryService(FakeEngine(), NullCache(), 60, 300, 10)
    with pytest.raises(ValueError, match="max_rows"):
        subject.execute(query_request(max_rows=11))


def test_undeclared_table_is_rejected_before_spark() -> None:
    engine = FakeEngine()
    subject = service(engine)
    with pytest.raises(ValueError, match="undeclared source"):
        subject.execute(query_request(sql="select * from secrets"))
    assert engine.prepare_calls == 0


def test_timeout_has_global_ceiling() -> None:
    subject = service(FakeEngine(), max_query_timeout_seconds=30)
    with pytest.raises(ValueError, match="timeout_seconds"):
        subject.execute(query_request(timeout_seconds=31))


def test_live_singleflight_owner_blocks_duplicate_spark_work() -> None:
    engine = FakeEngine()
    subject = QueryService(
        engine,
        BusySingleFlightCache(),
        60,
        300,
        1000,
        singleflight_wait_seconds=0.01,
    )
    request = QueryRequest(
        sql="select * from orders",
        sources={
            "orders": SourceSpec(
                path="orders.csv",
                format=DataFormat.CSV,
                version="v1",
            )
        },
    )

    from dalmatian.errors import BusyError

    with pytest.raises(BusyError, match="equivalent query"):
        subject.execute(request)
    assert engine.calls == 0
    assert engine.prepare_calls == 1


def test_admission_limit_rejects_excess_concurrent_query() -> None:
    from dalmatian.errors import BusyError

    engine = BlockingEngine()
    subject = service(
        engine,
        max_concurrent_queries=1,
        admission_wait_seconds=0.01,
    )
    owner = threading.Thread(target=subject.execute, args=(query_request(),))
    owner.start()
    assert engine.started.wait(timeout=1)
    try:
        with pytest.raises(BusyError, match="concurrency limit"):
            subject.execute(query_request(sql="select count(*) from orders"))
    finally:
        engine.release.set()
        owner.join(timeout=2)
    assert not owner.is_alive()


def test_partial_sync_stream_is_recorded_as_cancelled() -> None:
    metadata = TrackingMetadata()
    subject = service(FakeEngine(), metadata=metadata)
    request = query_request(output=OutputSpec(mode="stream", format=OutputFormat.CSV))
    _, iterator, _ = subject.stream(request)
    assert next(iterator) == "value\n"
    iterator.close()
    assert metadata.statuses[-1] == "cancelled"
