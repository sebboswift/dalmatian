from __future__ import annotations

import threading
import time
import uuid
import zlib
from contextlib import suppress
from typing import Protocol

from dalmatian.models import InlineResult


class ResultCache(Protocol):
    def get(self, key: str) -> InlineResult | None: ...
    def set(self, key: str, value: InlineResult, ttl_seconds: int) -> bool: ...
    def acquire(self, key: str, ttl_seconds: int) -> str | None: ...
    def release(self, key: str, token: str) -> None: ...
    def wait(self, key: str, timeout_seconds: float) -> InlineResult | None: ...
    def ping(self) -> bool: ...


class NullCache:
    def get(self, key: str) -> InlineResult | None:
        return None

    def set(self, key: str, value: InlineResult, ttl_seconds: int) -> bool:
        return False

    def acquire(self, key: str, ttl_seconds: int) -> str | None:
        return uuid.uuid4().hex

    def release(self, key: str, token: str) -> None:
        return None

    def wait(self, key: str, timeout_seconds: float) -> InlineResult | None:
        return None

    def ping(self) -> bool:
        return True


class MemoryCache(NullCache):
    def __init__(self) -> None:
        self.values: dict[str, InlineResult] = {}
        self._locks: set[str] = set()
        self._mutex = threading.Lock()

    def get(self, key: str) -> InlineResult | None:
        return self.values.get(key)

    def set(self, key: str, value: InlineResult, ttl_seconds: int) -> bool:
        self.values[key] = value
        return True

    def acquire(self, key: str, ttl_seconds: int) -> str | None:
        with self._mutex:
            if key in self._locks:
                return None
            self._locks.add(key)
            return key

    def release(self, key: str, token: str) -> None:
        with self._mutex:
            self._locks.discard(key)

    def wait(self, key: str, timeout_seconds: float) -> InlineResult | None:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            value = self.get(key)
            if value is not None:
                return value
            time.sleep(0.005)
        return None


class ValkeyResultCache:
    _RELEASE_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""

    def __init__(
        self,
        url: str,
        prefix: str = "dalmatian:cache",
        max_bytes: int = 4 * 1024 * 1024,
    ) -> None:
        import valkey

        self.client = valkey.from_url(url, decode_responses=False)
        self.prefix = prefix
        self.max_bytes = max_bytes
        self._release = self.client.register_script(self._RELEASE_SCRIPT)

    def _key(self, key: str) -> str:
        return f"{self.prefix}:{key}"

    def _lock_key(self, key: str) -> str:
        return f"{self.prefix}:lock:{key}"

    def get(self, key: str) -> InlineResult | None:
        raw = self.client.get(self._key(key))
        if raw is None:
            return None
        try:
            return InlineResult.model_validate_json(zlib.decompress(raw))
        except (ValueError, TypeError, zlib.error):
            with suppress(Exception):
                self.client.delete(self._key(key))
            return None

    def set(self, key: str, value: InlineResult, ttl_seconds: int) -> bool:
        if ttl_seconds <= 0:
            return False
        raw = zlib.compress(value.model_dump_json().encode(), level=3)
        if len(raw) > self.max_bytes:
            return False
        self.client.setex(self._key(key), ttl_seconds, raw)
        return True

    def acquire(self, key: str, ttl_seconds: int) -> str | None:
        token = uuid.uuid4().hex
        acquired = self.client.set(self._lock_key(key), token.encode(), nx=True, ex=ttl_seconds)
        return token if acquired else None

    def release(self, key: str, token: str) -> None:
        self._release(keys=[self._lock_key(key)], args=[token])

    def wait(self, key: str, timeout_seconds: float) -> InlineResult | None:
        deadline = time.monotonic() + timeout_seconds
        delay = 0.01
        while time.monotonic() < deadline:
            value = self.get(key)
            if value is not None:
                return value
            time.sleep(delay)
            delay = min(delay * 1.5, 0.25)
        return None

    def ping(self) -> bool:
        return bool(self.client.ping())

    def close(self) -> None:
        self.client.close()
