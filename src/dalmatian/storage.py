from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from urllib.parse import urlsplit

from dalmatian.metadata import MetadataStore


class UnknownStorageLocation(ValueError):
    pass


@dataclass(frozen=True)
class ResolvedDestination:
    location: str
    path: str
    uri: str


class OutputPublicationRegistry(Protocol):
    def publish(
        self,
        publication_key: str,
        query_id: str,
        attempt_id: str,
        manifest_uri: str,
        write_mode: str,
    ) -> bool: ...

    def current(self, publication_key: str) -> dict[str, str] | None: ...

    def close(self) -> None: ...


class MemoryOutputPublicationRegistry:
    def __init__(self) -> None:
        self.values: dict[str, dict[str, str]] = {}

    def publish(
        self,
        publication_key: str,
        query_id: str,
        attempt_id: str,
        manifest_uri: str,
        write_mode: str,
    ) -> bool:
        if write_mode == "append":
            current = self.values.setdefault(publication_key, {"exists": "1"})
            current.update(
                {
                    "last_append_query_id": query_id,
                    "last_append_attempt_id": attempt_id,
                    "last_append_manifest_uri": manifest_uri,
                }
            )
            return True
        current = self.values.get(publication_key)
        if write_mode == "error" and current is not None and current.get("query_id") != query_id:
            return False
        self.values[publication_key] = {
            "exists": "1",
            "query_id": query_id,
            "attempt_id": attempt_id,
            "manifest_uri": manifest_uri,
        }
        return True

    def current(self, publication_key: str) -> dict[str, str] | None:
        value = self.values.get(publication_key)
        return dict(value) if value is not None else None

    def close(self) -> None:
        return None


class ValkeyOutputPublicationRegistry:
    _PUBLISH_SCRIPT = """
local key = KEYS[1]
local mode = ARGV[1]
local query_id = ARGV[2]
if mode == 'append' then
  redis.call('HSET', key,
    'exists', '1',
    'last_append_query_id', query_id,
    'last_append_attempt_id', ARGV[3],
    'last_append_manifest_uri', ARGV[4],
    'updated_at_ms', ARGV[5])
  return 1
end
if mode == 'error' and redis.call('EXISTS', key) == 1 then
  local existing_query = redis.call('HGET', key, 'query_id') or ''
  if existing_query ~= query_id then return 0 end
end
redis.call('HSET', key,
  'exists', '1',
  'query_id', query_id,
  'attempt_id', ARGV[3],
  'manifest_uri', ARGV[4],
  'updated_at_ms', ARGV[5])
return 1
"""

    def __init__(self, url: str) -> None:
        import valkey

        self.client = valkey.from_url(url, decode_responses=True)
        self._publish = self.client.register_script(self._PUBLISH_SCRIPT)

    def publish(
        self,
        publication_key: str,
        query_id: str,
        attempt_id: str,
        manifest_uri: str,
        write_mode: str,
    ) -> bool:
        import time

        return bool(
            self._publish(
                keys=[publication_key],
                args=[write_mode, query_id, attempt_id, manifest_uri, int(time.time() * 1000)],
            )
        )

    def current(self, publication_key: str) -> dict[str, str] | None:
        value = self.client.hgetall(publication_key)
        return value or None

    def close(self) -> None:
        self.client.close()


class StorageRegistry:
    supported_schemes = {"file", "s3a", "gs", "abfss"}

    def __init__(
        self,
        locations: dict[str, str],
        metadata: MetadataStore | None = None,
        source_locations: dict[str, str] | None = None,
        data_root: Path | None = None,
        publication_registry: OutputPublicationRegistry | None = None,
        publication_prefix: str = "dalmatian:outputs",
    ) -> None:
        self._configured = {
            name: self._validated_root(name, uri)
            for name, uri in locations.items()
        }
        self._sources = {
            name: self._validated_root(name, uri)
            for name, uri in (source_locations or {}).items()
        }
        if data_root is not None:
            self._sources.setdefault("local", data_root.resolve().as_uri())
        self.metadata = metadata
        self.publication_registry = publication_registry or MemoryOutputPublicationRegistry()
        self.publication_prefix = publication_prefix.rstrip(":")

    def _validated_root(self, name: str, configured_root: str) -> str:
        root = self._normalize_root(configured_root).rstrip("/")
        scheme = urlsplit(root).scheme
        if scheme not in self.supported_schemes:
            raise ValueError(f"unsupported storage scheme for {name}: {scheme or 'missing'}")
        return root

    @staticmethod
    def _normalize_root(root: str) -> str:
        if root.startswith("s3://"):
            return f"s3a://{root.removeprefix('s3://')}"
        return root

    @staticmethod
    def _clean(path: str) -> str:
        if path.startswith("/") or ".." in path.split("/"):
            raise ValueError("storage path must be relative and cannot contain '..'")
        clean = path.strip("/")
        if not clean:
            raise ValueError("storage path cannot be empty")
        return clean

    def _root(self, location: str) -> tuple[str, str]:
        if location in self._configured:
            return self._configured[location], "config"
        if self.metadata is not None:
            value = self.metadata.storage_location(location)
            if value:
                return self._validated_root(location, value), "database"
        raise UnknownStorageLocation(f"unknown storage location: {location}")

    def resolve(self, location: str, path: str) -> ResolvedDestination:
        root, _ = self._root(location)
        clean = self._clean(path)
        return ResolvedDestination(location=location, path=clean, uri=f"{root}/{clean}")

    def resolve_source(self, location: str | None, path: str) -> ResolvedDestination:
        name = location or "local"
        root = self._sources.get(name)
        if root is None:
            if location is not None:
                db_root, _ = self._root(name)
                root = db_root
            else:
                raise UnknownStorageLocation("local source root is not configured")
        clean = self._clean(path)
        return ResolvedDestination(location=name, path=clean, uri=f"{root}/{clean}")

    def locations(self) -> list[tuple[str, str, str]]:
        values = [(name, uri, "config") for name, uri in sorted(self._configured.items())]
        if self.metadata is not None:
            for name, uri in sorted(self.metadata.storage_locations().items()):
                if name not in self._configured:
                    values.append((name, self._normalize_root(uri), "database"))
        return values

    def publication_key(self, destination: ResolvedDestination) -> str:
        raw = f"{destination.location}\0{destination.path}".encode()
        return f"{self.publication_prefix}:{hashlib.sha256(raw).hexdigest()}"

    def publish_sync(
        self,
        destination: ResolvedDestination,
        query_id: str,
        attempt_id: str,
        manifest_uri: str,
        write_mode: str,
    ) -> bool:
        return self.publication_registry.publish(
            self.publication_key(destination),
            query_id,
            attempt_id,
            manifest_uri,
            write_mode,
        )

    @staticmethod
    def manifest_payload(**values: object) -> bytes:
        return (json.dumps(values, sort_keys=True, separators=(",", ":")) + "\n").encode()

    def close(self) -> None:
        self.publication_registry.close()
