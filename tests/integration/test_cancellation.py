import threading
import time
from pathlib import Path

import pytest

from dalmatian.errors import QueryCancelled
from dalmatian.execution import ExecutionContext
from dalmatian.models import DataFormat, QueryRequest, SourceSpec
from dalmatian.spark import SparkEngine
from dalmatian.storage import StorageRegistry

pytestmark = pytest.mark.integration


def test_running_spark_job_group_is_actually_cancelled(tmp_path: Path) -> None:
    data = tmp_path / "data"
    exports = tmp_path / "exports"
    data.mkdir()
    exports.mkdir()
    engine = SparkEngine(
        master="local[2]",
        app_name="dalmatian-cancellation",
        data_root=data,
        storage=StorageRegistry({"local": exports.as_uri()}, data_root=data),
        dataset_cache_entries=2,
        dataset_cache_ttl_seconds=60,
    )
    numbers = data / "numbers.parquet"
    engine.spark.range(20_000).repartition(4).write.parquet(str(numbers))
    request = QueryRequest(
        sql="select sum(a.id * b.id) as value from numbers a cross join numbers b",
        sources={"numbers": SourceSpec(path="numbers.parquet", format=DataFormat.PARQUET)},
    )
    context = ExecutionContext(query_id="cancel-running-spark", timeout_seconds=60)
    errors: list[Exception] = []

    def execute() -> None:
        try:
            engine.execute(engine.prepare(request, request.sql), context)
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=execute)
    try:
        thread.start()
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if engine.spark.sparkContext.statusTracker().getActiveJobsIds():
                break
            time.sleep(0.05)
        else:
            pytest.fail("Spark query did not start in time")

        engine.cancel(context.query_id)
        thread.join(timeout=15)
        assert not thread.is_alive()
        assert len(errors) == 1
        assert isinstance(errors[0], QueryCancelled)
        assert engine.ping()
    finally:
        engine.close()
