from pathlib import Path

from dalmatian.metadata import SqlMetadataStore, upgrade_metadata
from dalmatian.models import InlineResult


def test_sql_metadata_tracks_invocations_and_storage_locations(tmp_path: Path) -> None:
    url = f"sqlite+pysqlite:///{tmp_path / 'metadata.db'}"
    upgrade_metadata(url)
    store = SqlMetadataStore(url)
    try:
        request_json = '{"sql":"select 1","output":{"mode":"inline","format":"json"}}'
        store.record_invocation("run-1", "sync", request_json)
        result = InlineResult(columns=["ok"], rows=[{"ok": True}], truncated=False)
        store.update_invocation("run-1", "succeeded", result=result)
        store.put_storage_location("lake", "s3a://bucket/results")
        assert store.storage_location("lake") == "s3a://bucket/results"
        assert store.storage_locations() == {"lake": "s3a://bucket/results"}
        store.delete_storage_location("lake")
        assert store.storage_location("lake") is None

        with store.engine.connect() as conn:
            row = conn.execute(store.invocations.select()).mappings().one()
        assert row["rows_returned"] == 1
        assert row["bytes_returned"] > 0
        assert "result_json" not in row
    finally:
        store.close()
