from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from dalmatian.errors import MetadataSchemaError
from dalmatian.models import InlineResult, QueryResult, StoredResult


class MetadataStore(Protocol):
    def record_invocation(self, invocation_id: str, kind: str, request_json: str) -> None: ...
    def update_invocation(
        self,
        invocation_id: str,
        status: str,
        result: QueryResult | None = None,
        error: str | None = None,
        error_class: str | None = None,
        attempts: int | None = None,
    ) -> None: ...
    def storage_location(self, name: str) -> str | None: ...
    def storage_locations(self) -> dict[str, str]: ...
    def put_storage_location(self, name: str, uri: str) -> None: ...
    def delete_storage_location(self, name: str) -> None: ...
    def close(self) -> None: ...


class NullMetadataStore:
    def record_invocation(self, invocation_id: str, kind: str, request_json: str) -> None:
        return None

    def update_invocation(
        self,
        invocation_id: str,
        status: str,
        result: QueryResult | None = None,
        error: str | None = None,
        error_class: str | None = None,
        attempts: int | None = None,
    ) -> None:
        return None

    def storage_location(self, name: str) -> str | None:
        return None

    def storage_locations(self) -> dict[str, str]:
        return {}

    def put_storage_location(self, name: str, uri: str) -> None:
        return None

    def delete_storage_location(self, name: str) -> None:
        return None

    def close(self) -> None:
        return None


def _migration_config(url: str):
    from alembic.config import Config

    script_location = Path(__file__).with_name("migrations")
    config = Config()
    config.set_main_option("script_location", str(script_location))
    config.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    return config


def upgrade_metadata(url: str, revision: str = "head") -> None:
    from alembic import command

    command.upgrade(_migration_config(url), revision)


def current_metadata_revision(url: str) -> str | None:
    from sqlalchemy import create_engine, inspect, text

    engine = create_engine(url, pool_pre_ping=True)
    try:
        if not inspect(engine).has_table("alembic_version"):
            return None
        with engine.connect() as conn:
            result = conn.execute(text("select version_num from alembic_version"))
            return result.scalar_one_or_none()
    finally:
        engine.dispose()


def _assert_schema_current(url: str) -> None:
    from alembic.script import ScriptDirectory

    config = _migration_config(url)
    head = ScriptDirectory.from_config(config).get_current_head()
    current = current_metadata_revision(url)
    if current != head:
        raise MetadataSchemaError(
            "metadata database is not migrated; run 'dalmatian-admin metadata-upgrade'"
        )


class SqlMetadataStore:
    def __init__(self, url: str, *, require_current_schema: bool = True) -> None:
        from sqlalchemy import (
            BigInteger,
            Boolean,
            Column,
            DateTime,
            Integer,
            MetaData,
            String,
            Table,
            Text,
            create_engine,
        )

        if require_current_schema:
            _assert_schema_current(url)
        self.engine = create_engine(url, pool_pre_ping=True)
        self.meta = MetaData()
        self.invocations = Table(
            "dalmatian_invocations",
            self.meta,
            Column("id", String(64), primary_key=True),
            Column("kind", String(16), nullable=False),
            Column("status", String(32), nullable=False),
            Column("attempts", Integer, nullable=False),
            Column("request_json", Text, nullable=False),
            Column("output_mode", String(16)),
            Column("output_format", String(16)),
            Column("output_location", String(128)),
            Column("output_path", Text),
            Column("cached", Boolean),
            Column("rows_returned", BigInteger),
            Column("bytes_returned", BigInteger),
            Column("truncated", Boolean),
            Column("manifest_uri", Text),
            Column("data_uri", Text),
            Column("source_versions_json", Text),
            Column("error_class", String(255)),
            Column("error_message", Text),
            Column("created_at", DateTime(timezone=True), nullable=False),
            Column("started_at", DateTime(timezone=True)),
            Column("finished_at", DateTime(timezone=True)),
            Column("duration_ms", BigInteger),
            Column("updated_at", DateTime(timezone=True), nullable=False),
        )
        self.locations = Table(
            "dalmatian_storage_locations",
            self.meta,
            Column("name", String(128), primary_key=True),
            Column("uri", Text, nullable=False),
            Column("created_at", DateTime(timezone=True), nullable=False),
            Column("updated_at", DateTime(timezone=True), nullable=False),
        )

    def record_invocation(self, invocation_id: str, kind: str, request_json: str) -> None:
        from sqlalchemy import insert

        request = json.loads(request_json)
        output = request.get("output") or {}
        now = datetime.now(UTC)
        with self.engine.begin() as conn:
            conn.execute(
                insert(self.invocations).values(
                    id=invocation_id,
                    kind=kind,
                    status="queued" if kind == "async" else "running",
                    attempts=0,
                    request_json=request_json,
                    output_mode=output.get("mode", "inline"),
                    output_format=output.get("format", "json"),
                    output_location=output.get("location"),
                    output_path=output.get("path"),
                    created_at=now,
                    started_at=now if kind == "sync" else None,
                    updated_at=now,
                )
            )

    @staticmethod
    def _result_values(result: QueryResult) -> dict[str, object]:
        if isinstance(result, InlineResult):
            encoded = result.model_dump_json().encode()
            return {
                "cached": result.cached,
                "rows_returned": len(result.rows),
                "bytes_returned": len(encoded),
                "truncated": result.truncated,
                "output_mode": "inline",
                "output_format": "json",
            }
        if isinstance(result, StoredResult):
            return {
                "output_mode": "store",
                "output_format": result.format.value,
                "output_location": result.location,
                "output_path": result.path,
                "manifest_uri": result.manifest_uri,
                "data_uri": result.data_uri,
                "source_versions_json": json.dumps(result.source_versions, sort_keys=True),
            }
        return {}

    def update_invocation(
        self,
        invocation_id: str,
        status: str,
        result: QueryResult | None = None,
        error: str | None = None,
        error_class: str | None = None,
        attempts: int | None = None,
    ) -> None:
        from sqlalchemy import select, update

        now = datetime.now(UTC)
        values: dict[str, object] = {"status": status, "updated_at": now}
        if result is not None:
            values.update(self._result_values(result))
        if error is not None:
            values["error_message"] = error
        if error_class is not None:
            values["error_class"] = error_class
        if attempts is not None:
            values["attempts"] = attempts
        if status == "running":
            values["started_at"] = now
        if status in {"succeeded", "failed", "cancelled"}:
            values["finished_at"] = now

        with self.engine.begin() as conn:
            started_at = conn.execute(
                select(self.invocations.c.started_at).where(self.invocations.c.id == invocation_id)
            ).scalar_one_or_none()
            if status in {"succeeded", "failed", "cancelled"} and started_at is not None:
                if started_at.tzinfo is None:
                    started_at = started_at.replace(tzinfo=UTC)
                values["duration_ms"] = max(0, int((now - started_at).total_seconds() * 1000))
            conn.execute(
                update(self.invocations)
                .where(self.invocations.c.id == invocation_id)
                .values(**values)
            )

    def storage_location(self, name: str) -> str | None:
        from sqlalchemy import select

        with self.engine.connect() as conn:
            return conn.execute(
                select(self.locations.c.uri).where(self.locations.c.name == name)
            ).scalar_one_or_none()

    def storage_locations(self) -> dict[str, str]:
        from sqlalchemy import select

        with self.engine.connect() as conn:
            return dict(conn.execute(select(self.locations.c.name, self.locations.c.uri)).all())

    def put_storage_location(self, name: str, uri: str) -> None:
        from sqlalchemy import select, update

        now = datetime.now(UTC)
        with self.engine.begin() as conn:
            exists = conn.execute(
                select(self.locations.c.name).where(self.locations.c.name == name)
            ).first()
            if exists:
                conn.execute(
                    update(self.locations)
                    .where(self.locations.c.name == name)
                    .values(uri=uri, updated_at=now)
                )
            else:
                conn.execute(
                    self.locations.insert().values(
                        name=name,
                        uri=uri,
                        created_at=now,
                        updated_at=now,
                    )
                )

    def delete_storage_location(self, name: str) -> None:
        from sqlalchemy import delete

        with self.engine.begin() as conn:
            conn.execute(delete(self.locations).where(self.locations.c.name == name))

    def close(self) -> None:
        self.engine.dispose()


def build_metadata_store(url: str | None) -> MetadataStore:
    return SqlMetadataStore(url) if url else NullMetadataStore()
