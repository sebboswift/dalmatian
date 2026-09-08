from pathlib import Path

import pytest

from dalmatian.datasets import PreparedQuery, ResolvedSource
from dalmatian.errors import QueryRejected
from dalmatian.execution import ExecutionContext
from dalmatian.models import DataFormat, OutputFormat, OutputSpec, QueryRequest, SourceSpec
from dalmatian.spark import SparkEngine
from dalmatian.storage import StorageRegistry

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def engine(tmp_path_factory: pytest.TempPathFactory) -> SparkEngine:
    root = tmp_path_factory.mktemp("data")
    exports = tmp_path_factory.mktemp("exports")
    nested = root / "files" / "region" / "eu" / "2026" / "09"
    nested.mkdir(parents=True)
    (nested / "orders.csv").write_text("id,total\n1,10\n2,20\n")
    old = root / "files" / "region" / "eu" / "2025" / "09"
    old.mkdir(parents=True)
    (old / "old.csv").write_text("id,total\n3,999\n")
    instance = SparkEngine(
        master="local[2]",
        app_name="dalmatian-test",
        data_root=root,
        storage=StorageRegistry({"local": exports.as_uri()}),
        dataset_cache_entries=4,
        dataset_cache_ttl_seconds=60,
    )
    yield instance
    instance.close()


def run(engine: SparkEngine, request: QueryRequest):
    return engine.execute(
        engine.prepare(request, request.sql),
        ExecutionContext(timeout_seconds=30),
    )


def csv_source() -> SourceSpec:
    return SourceSpec.model_validate(
        {
            "path": "files/**/*.csv",
            "format": "csv",
            "schema": {"id": "long", "total": "long"},
        }
    )


def test_csv_recursive_glob(engine: SparkEngine) -> None:
    result = run(
        engine,
        QueryRequest(
            sql="select sum(total) as total from orders",
            sources={"orders": csv_source()},
        ),
    )
    assert result.rows == [{"total": 1029}]
    assert not hasattr(result, "row_count")


def test_unresolved_column_is_a_client_rejection(engine: SparkEngine) -> None:
    request = QueryRequest(
        sql="select missing_column from orders",
        sources={"orders": csv_source()},
    )
    with pytest.raises(QueryRejected, match="query analysis failed"):
        run(engine, request)


def test_nested_recursive_glob_preserves_directory_filter(engine: SparkEngine) -> None:
    source = SourceSpec.model_validate(
        {
            "path": "files/**/2026/**/*.csv",
            "format": "csv",
            "schema": {"id": "long", "total": "long"},
        }
    )
    result = run(
        engine,
        QueryRequest(
            sql="select sum(total) as total from orders",
            sources={"orders": source},
        ),
    )
    assert result.rows == [{"total": 30}]


def test_uncached_source_is_registered_in_query_session(engine: SparkEngine) -> None:
    request = QueryRequest(
        sql="select sum(total) as total from orders",
        sources={"orders": csv_source()},
    )
    prepared = PreparedQuery(
        request=request,
        sources={
            "orders": ResolvedSource(
                uri=f"{(engine.data_root / 'files').as_uri()}/**/*.csv",
                fingerprint="uncached-test",
                cache_safe=False,
            )
        },
        normalized_sql=request.sql,
    )
    result = engine.execute(prepared, ExecutionContext(timeout_seconds=30))
    assert result.rows == [{"total": 1029}]


def test_different_queries_reuse_hot_dataset(engine: SparkEngine) -> None:
    request = QueryRequest(
        sql="select max(total) as value from orders",
        sources={"orders": csv_source()},
    )
    run(engine, request)
    hits_before = engine.datasets.hits
    run(engine, request.model_copy(update={"sql": "select min(total) as value from orders"}))
    assert engine.datasets.hits > hits_before


def test_json_and_parquet(engine: SparkEngine) -> None:
    root = engine.data_root
    (root / "events.json").write_text('{"kind":"open","count":2}\n')
    json_result = run(
        engine,
        QueryRequest(
            sql="select kind, count from events",
            sources={"events": SourceSpec(path="events.json", format=DataFormat.JSON)},
        ),
    )
    assert json_result.rows == [{"kind": "open", "count": 2}]

    frame = engine.spark.createDataFrame([(1, "Ada"), (2, "Grace")], ["id", "name"])
    parquet_path = root / "people.parquet"
    frame.write.mode("overwrite").parquet(str(parquet_path))
    parquet_result = run(
        engine,
        QueryRequest(
            sql="select count(*) as count from people",
            sources={"people": SourceSpec(path="people.parquet", format=DataFormat.PARQUET)},
        ),
    )
    assert parquet_result.rows == [{"count": 2}]


def test_csv_stream_does_not_precount(engine: SparkEngine) -> None:
    request = QueryRequest.model_validate(
        {
            "sql": "select id, total from orders order by id",
            "sources": {
                "orders": {
                    "path": "files/**/*.csv",
                    "format": "csv",
                    "schema": {"id": "long", "total": "long"},
                }
            },
            "output": {"mode": "stream", "format": "csv"},
        }
    )
    media_type, rows = engine.stream(
        engine.prepare(request, request.sql),
        ExecutionContext(timeout_seconds=30),
    )
    assert media_type == "text/csv"
    assert "".join(rows).splitlines() == ["id,total", "1,10", "2,20", "3,999"]


def test_store_writes_attempt_scoped_parquet_and_manifest(engine: SparkEngine) -> None:
    request = QueryRequest(
        sql="select id, total from orders",
        sources={"orders": csv_source()},
        output=OutputSpec(
            mode="store",
            format=OutputFormat.PARQUET,
            location="local",
            path="run-1",
        ),
    )
    result = run(engine, request)
    assert result.type == "stored"
    assert "_dalmatian_data" in result.data_uri
    assert "/_dalmatian_manifests/" in result.manifest_uri
    assert Path(result.data_uri.removeprefix("file://")).exists()
    assert Path(result.manifest_uri.removeprefix("file://")).exists()
