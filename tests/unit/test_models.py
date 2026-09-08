import pytest
from pydantic import ValidationError

from dalmatian.models import DataFormat, QueryRequest, SourceSpec


def base_payload() -> dict[str, object]:
    return {
        "sql": "select * from orders",
        "sources": {"orders": {"path": "x.csv", "format": "csv"}},
    }


def test_source_alias_must_be_identifier() -> None:
    with pytest.raises(ValidationError):
        QueryRequest(
            sql="select 1",
            sources={"bad-name": SourceSpec(path="x.csv", format=DataFormat.CSV)},
        )


def test_stream_requires_csv_or_jsonl() -> None:
    payload = base_payload() | {"output": {"mode": "stream", "format": "parquet"}}
    with pytest.raises(ValidationError):
        QueryRequest.model_validate(payload)


def test_store_requires_named_location_and_relative_path() -> None:
    payload = base_payload() | {
        "output": {"mode": "store", "format": "parquet", "location": "lake"}
    }
    with pytest.raises(ValidationError):
        QueryRequest.model_validate(payload)


def test_stored_json_is_called_jsonl() -> None:
    payload = base_payload() | {
        "output": {"mode": "store", "format": "json", "location": "lake", "path": "run"}
    }
    with pytest.raises(ValidationError, match="jsonl"):
        QueryRequest.model_validate(payload)


def test_max_rows_is_inline_only() -> None:
    payload = base_payload() | {
        "max_rows": 10,
        "output": {"mode": "stream", "format": "csv"},
    }
    with pytest.raises(ValidationError):
        QueryRequest.model_validate(payload)


def test_cache_ttl_is_inline_only() -> None:
    payload = base_payload() | {
        "cache_ttl_seconds": 30,
        "output": {"mode": "stream", "format": "csv"},
    }
    with pytest.raises(ValidationError):
        QueryRequest.model_validate(payload)


def test_csv_schema_is_explicit_and_inference_is_opt_in() -> None:
    source = SourceSpec.model_validate(
        {"path": "x.csv", "format": "csv", "schema": {"id": "long", "name": "string"}}
    )
    assert source.schema_definition == {"id": "long", "name": "string"}
    assert source.infer_schema is False


def test_infer_schema_is_csv_only() -> None:
    with pytest.raises(ValidationError):
        SourceSpec(path="x.json", format=DataFormat.JSON, infer_schema=True)
