from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class DataFormat(StrEnum):
    CSV = "csv"
    PARQUET = "parquet"
    JSON = "json"


class DeliveryMode(StrEnum):
    INLINE = "inline"
    STREAM = "stream"
    STORE = "store"


class OutputFormat(StrEnum):
    JSON = "json"
    CSV = "csv"
    JSONL = "jsonl"
    PARQUET = "parquet"


class WriteMode(StrEnum):
    ERROR = "error"
    OVERWRITE = "overwrite"
    APPEND = "append"


class SourceSpec(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    path: str = Field(min_length=1)
    format: DataFormat
    location: str | None = Field(default=None, min_length=1)
    options: dict[str, str] = Field(default_factory=dict)
    version: str | None = Field(default=None, min_length=1)
    schema_definition: dict[str, str] | str | None = Field(
        default=None,
        alias="schema",
        serialization_alias="schema",
    )
    infer_schema: bool = False

    @model_validator(mode="after")
    def validate_schema_options(self) -> SourceSpec:
        if self.infer_schema and self.format != DataFormat.CSV:
            raise ValueError("infer_schema is only valid for csv sources")
        return self


class OutputSpec(BaseModel):
    mode: DeliveryMode = DeliveryMode.INLINE
    format: OutputFormat = OutputFormat.JSON
    location: str | None = None
    path: str | None = None
    write_mode: WriteMode = WriteMode.ERROR
    options: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_delivery(self) -> OutputSpec:
        if self.mode == DeliveryMode.INLINE:
            if self.format != OutputFormat.JSON:
                raise ValueError("inline output format must be json")
            if self.location is not None or self.path is not None:
                raise ValueError("inline output does not accept a storage destination")
            return self
        if self.mode == DeliveryMode.STREAM:
            if self.format not in {OutputFormat.CSV, OutputFormat.JSONL}:
                raise ValueError("stream output format must be csv or jsonl")
            if self.location is not None or self.path is not None:
                raise ValueError("stream output does not accept a storage destination")
            return self
        if self.location is None or self.path is None:
            raise ValueError("stored output requires location and path")
        if self.format == OutputFormat.JSON:
            raise ValueError("stored output uses jsonl, csv, or parquet; json is inline only")
        if self.path.startswith("/") or ".." in self.path.split("/"):
            raise ValueError("stored output path must be relative and cannot contain '..'")
        return self


class QueryRequest(BaseModel):
    sql: str = Field(min_length=1)
    sources: dict[str, SourceSpec] = Field(min_length=1)
    output: OutputSpec = Field(default_factory=OutputSpec)
    max_rows: int = Field(default=1000, ge=1)
    cache_ttl_seconds: int | None = Field(default=None, ge=0)
    timeout_seconds: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def validate_request(self) -> QueryRequest:
        for name in self.sources:
            if not name.isidentifier():
                raise ValueError(f"source name must be a valid identifier: {name}")
        if self.output.mode != DeliveryMode.INLINE and "max_rows" in self.model_fields_set:
            raise ValueError("max_rows is only valid for inline output; use SQL LIMIT otherwise")
        if self.output.mode != DeliveryMode.INLINE and "cache_ttl_seconds" in self.model_fields_set:
            raise ValueError("cache_ttl_seconds is only valid for inline output")
        return self


class InlineResult(BaseModel):
    type: Literal["inline"] = "inline"
    query_id: str | None = None
    columns: list[str]
    rows: list[dict[str, Any]]
    truncated: bool
    cached: bool = False


class StoredResult(BaseModel):
    type: Literal["stored"] = "stored"
    query_id: str
    format: OutputFormat
    location: str
    path: str
    manifest_uri: str
    data_uri: str
    partitions: int
    source_versions: dict[str, str]


QueryResult = InlineResult | StoredResult


class ExplainResult(BaseModel):
    query_id: str
    normalized_sql: str
    source_versions: dict[str, str]
    plan: str


class SourceColumn(BaseModel):
    name: str
    type: str
    nullable: bool


class SourceInspection(BaseModel):
    location: str
    resolved_uri: str
    fingerprint: str | None
    cache_safe: bool
    columns: list[SourceColumn]


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    RETRYING = "retrying"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobRecord(BaseModel):
    id: str
    status: JobStatus
    attempts: int = 0
    result: QueryResult | None = None
    error: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    next_attempt_at: datetime | None = None


class StorageLocationInfo(BaseModel):
    name: str
    uri: str
    source: Literal["config", "database"]
