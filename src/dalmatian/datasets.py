from __future__ import annotations

import hashlib
import json
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlsplit

from dalmatian.models import QueryRequest, SourceSpec
from dalmatian.paths import resolve_paths
from dalmatian.storage import ResolvedDestination, StorageRegistry


@dataclass(frozen=True)
class ResolvedSource:
    uri: str
    fingerprint: str
    cache_safe: bool
    read_uris: tuple[str, ...] | None = None


@dataclass(frozen=True)
class PreparedQuery:
    request: QueryRequest
    sources: dict[str, ResolvedSource]
    normalized_sql: str

    @property
    def source_versions(self) -> dict[str, str]:
        return {name: source.fingerprint for name, source in self.sources.items()}

    @property
    def cache_safe(self) -> bool:
        return all(source.cache_safe for source in self.sources.values())


def _hash(payload: object) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()


def resolve_source(source: SourceSpec, storage: StorageRegistry, data_root: Path) -> ResolvedSource:
    if source.location is None:
        if source.path.startswith("/") or ".." in source.path.split("/"):
            raise ValueError("source path must be relative and cannot contain '..'")
        clean = source.path.strip("/")
        destination = ResolvedDestination("local", clean, f"{data_root.resolve().as_uri()}/{clean}")
    else:
        destination = storage.resolve_source(source.location, source.path)
    if source.version is not None:
        return ResolvedSource(
            uri=destination.uri,
            fingerprint=_hash(
                {
                    "uri": destination.uri,
                    "format": source.format.value,
                    "options": source.options,
                    "schema": source.schema_definition,
                    "infer_schema": source.infer_schema,
                    "version": source.version,
                }
            ),
            cache_safe=True,
        )

    parsed = urlsplit(destination.uri)
    if parsed.scheme != "file":
        # Remote object stores do not expose a cheap, reliable directory version.
        # Spark receives the glob directly. Cross-query caches stay off
        # unless the caller declares a version.
        return ResolvedSource(
            uri=destination.uri,
            fingerprint=_hash({"uri": destination.uri, "uncached": True}),
            cache_safe=False,
        )

    local_path = Path(unquote(parsed.path))
    try:
        relative = local_path.relative_to(data_root.resolve())
    except ValueError:
        # A named local source may live outside data_root. Resolve it under
        # its own root without weakening the data_root traversal guard.
        root = local_path
        while root != root.parent and any(ch in root.name for ch in "*?["):
            root = root.parent
        if any(ch in str(local_path) for ch in "*?["):
            matches = sorted(
                Path(value)
                for value in __import__("glob").glob(str(local_path), recursive=True)
            )
        else:
            matches = [local_path]
    else:
        matches = [Path(value) for value in resolve_paths(str(relative), data_root)]

    files: list[tuple[str, int, int]] = []
    for path in matches:
        candidates = [path] if path.is_file() else sorted(p for p in path.rglob("*") if p.is_file())
        for candidate in candidates:
            stat = candidate.stat()
            files.append((str(candidate), stat.st_size, stat.st_mtime_ns))
    if not files:
        raise ValueError(f"source matched no files: {source.path}")
    return ResolvedSource(
        uri=destination.uri,
        fingerprint=_hash(
            {
                "format": source.format.value,
                "options": source.options,
                "schema": source.schema_definition,
                "infer_schema": source.infer_schema,
                "files": files,
            }
        ),
        cache_safe=True,
        read_uris=(
            tuple(Path(candidate).resolve().as_uri() for candidate, _, _ in files)
            if any(char in source.path for char in "*?[{")
            else None
        ),
    )


@dataclass
class _Entry[T]:
    value: T
    expires_at: float
    release: Callable[[T], None]


class DatasetCache[T]:
    def __init__(self, max_entries: int, ttl_seconds: int) -> None:
        if max_entries < 1:
            raise ValueError("max_entries must be at least 1")
        if ttl_seconds < 1:
            raise ValueError("ttl_seconds must be at least 1")
        self.max_entries = max_entries
        self.ttl_seconds = ttl_seconds
        self._entries: OrderedDict[str, _Entry[T]] = OrderedDict()
        self._lock = threading.RLock()
        self._key_locks: dict[str, threading.Lock] = {}
        self.hits = 0
        self.misses = 0

    def get_or_load(
        self,
        key: str,
        loader: Callable[[], T],
        release: Callable[[T], None],
    ) -> tuple[T, bool]:
        cached = self._get(key)
        if cached is not None:
            return cached, True
        key_lock = self._lock_for(key)
        with key_lock:
            cached = self._get(key)
            if cached is not None:
                return cached, True
            value = loader()
            self._put(key, value, release)
            self.misses += 1
            return value, False

    def _get(self, key: str) -> T | None:
        now = time.monotonic()
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if entry.expires_at <= now:
                self._entries.pop(key)
                entry.release(entry.value)
                return None
            self._entries.move_to_end(key)
            self.hits += 1
            return entry.value

    def _put(self, key: str, value: T, release: Callable[[T], None]) -> None:
        expires_at = time.monotonic() + self.ttl_seconds
        with self._lock:
            old = self._entries.pop(key, None)
            if old is not None:
                old.release(old.value)
            self._entries[key] = _Entry(value=value, expires_at=expires_at, release=release)
            while len(self._entries) > self.max_entries:
                _, evicted = self._entries.popitem(last=False)
                evicted.release(evicted.value)

    def _lock_for(self, key: str) -> threading.Lock:
        with self._lock:
            return self._key_locks.setdefault(key, threading.Lock())

    def close(self) -> None:
        with self._lock:
            entries = list(self._entries.values())
            self._entries.clear()
        for entry in entries:
            entry.release(entry.value)
