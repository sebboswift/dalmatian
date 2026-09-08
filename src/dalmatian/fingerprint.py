import hashlib
import json

from dalmatian.models import QueryRequest, SourceSpec
from dalmatian.sql import validate_and_normalize


def _hash(payload: object) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()


def _declared_source_version(source: SourceSpec) -> str:
    return _hash(
        {
            "location": source.location,
            "path": source.path,
            "format": source.format.value,
            "options": source.options,
            "schema": source.schema_definition,
            "infer_schema": source.infer_schema,
            "version": source.version,
        }
    )


def declared_source_versions(request: QueryRequest) -> dict[str, str] | None:
    if any(source.version is None for source in request.sources.values()):
        return None
    return {name: _declared_source_version(source) for name, source in request.sources.items()}


def dataset_affinity_key(request: QueryRequest) -> str:
    payload = {
        name: {
            "location": source.location,
            "path": source.path,
            "format": source.format.value,
            "options": source.options,
            "schema": source.schema_definition,
            "infer_schema": source.infer_schema,
            "version": source.version,
        }
        for name, source in sorted(request.sources.items())
    }
    return _hash(payload)


def query_fingerprint(request: QueryRequest, source_versions: dict[str, str]) -> str:
    payload = request.model_dump(mode="json", exclude={"cache_ttl_seconds", "sql"})
    payload["sql"] = validate_and_normalize(request.sql, set(request.sources))
    payload["source_versions"] = source_versions
    return _hash(payload)


def request_payload_fingerprint(request: QueryRequest) -> str:
    return _hash(request.model_dump(mode="json"))
