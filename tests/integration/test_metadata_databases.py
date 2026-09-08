import os
import time

import pytest

from dalmatian.metadata import SqlMetadataStore, upgrade_metadata
from dalmatian.models import InlineResult

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "env_name",
    ["DALMATIAN_TEST_POSTGRES_URL", "DALMATIAN_TEST_MYSQL_URL"],
)
def test_metadata_backend_round_trip(env_name: str) -> None:
    url = os.getenv(env_name)
    if not url:
        pytest.skip(f"{env_name} is not set")
    upgrade_metadata(url)
    store = SqlMetadataStore(url)
    run_id = f"integration-{time.time_ns()}"
    try:
        store.record_invocation(
            run_id,
            "sync",
            '{"sql":"select 1","output":{"mode":"inline","format":"json"}}',
        )
        store.update_invocation(
            run_id,
            "succeeded",
            result=InlineResult(columns=["value"], rows=[{"value": 1}], truncated=False),
        )
        with store.engine.connect() as conn:
            row = conn.execute(
                store.invocations.select().where(store.invocations.c.id == run_id)
            ).mappings().one()
        assert row["status"] == "succeeded"
        assert row["rows_returned"] == 1
        assert row["finished_at"] is not None
        assert "result_json" not in row
        assert '"rows"' not in row["request_json"]
        assert row["bytes_returned"] > 0
    finally:
        store.close()
