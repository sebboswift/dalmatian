from __future__ import annotations

import logging
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Protocol

from dalmatian.cache import ResultCache
from dalmatian.datasets import PreparedQuery
from dalmatian.errors import BusyError, QueryRejected
from dalmatian.execution import ExecutionContext
from dalmatian.fingerprint import dataset_affinity_key, query_fingerprint
from dalmatian.metadata import MetadataStore, NullMetadataStore
from dalmatian.models import (
    DeliveryMode,
    ExplainResult,
    InlineResult,
    QueryRequest,
    QueryResult,
    SourceInspection,
    SourceSpec,
)
from dalmatian.observability import CACHE_RESULT, OUTPUT_BYTES, query_timer
from dalmatian.sql import validate_and_normalize

logger = logging.getLogger(__name__)


class QueryEngine(Protocol):
    def prepare(self, request: QueryRequest, normalized_sql: str) -> PreparedQuery: ...
    def execute(self, prepared: PreparedQuery, context: ExecutionContext) -> QueryResult: ...
    def stream(
        self,
        prepared: PreparedQuery,
        context: ExecutionContext,
    ) -> tuple[str, Iterator[str]]: ...
    def explain(self, prepared: PreparedQuery, context: ExecutionContext) -> str: ...
    def inspect(self, source: SourceSpec) -> SourceInspection: ...
    def cancel(self, query_id: str) -> None: ...
    def ping(self) -> bool: ...


class QueryService:
    def __init__(
        self,
        engine: QueryEngine,
        cache: ResultCache,
        default_cache_ttl_seconds: int,
        max_cache_ttl_seconds: int,
        result_row_limit: int,
        max_concurrent_queries: int = 4,
        admission_wait_seconds: float = 0.25,
        default_query_timeout_seconds: int = 300,
        max_query_timeout_seconds: int = 3600,
        singleflight_wait_seconds: float = 15.0,
        singleflight_lock_seconds: int = 120,
        metadata: MetadataStore | None = None,
    ) -> None:
        self.engine = engine
        self.cache = cache
        self.default_cache_ttl_seconds = default_cache_ttl_seconds
        self.max_cache_ttl_seconds = max_cache_ttl_seconds
        self.result_row_limit = result_row_limit
        self.admission_wait_seconds = admission_wait_seconds
        self.default_query_timeout_seconds = default_query_timeout_seconds
        self.max_query_timeout_seconds = max_query_timeout_seconds
        self.singleflight_wait_seconds = singleflight_wait_seconds
        self.singleflight_lock_seconds = singleflight_lock_seconds
        self.metadata = metadata or NullMetadataStore()
        self._admission = threading.BoundedSemaphore(max_concurrent_queries)

    def _ttl(self, request: QueryRequest) -> int:
        ttl = (
            self.default_cache_ttl_seconds
            if request.cache_ttl_seconds is None
            else request.cache_ttl_seconds
        )
        if ttl > self.max_cache_ttl_seconds:
            raise QueryRejected(f"cache_ttl_seconds cannot exceed {self.max_cache_ttl_seconds}")
        return ttl

    def _timeout(self, request: QueryRequest) -> int:
        timeout = request.timeout_seconds or self.default_query_timeout_seconds
        if timeout > self.max_query_timeout_seconds:
            raise QueryRejected(f"timeout_seconds cannot exceed {self.max_query_timeout_seconds}")
        return timeout

    def _validate(self, request: QueryRequest) -> str:
        if request.output.mode == DeliveryMode.INLINE and request.max_rows > self.result_row_limit:
            raise QueryRejected(f"max_rows cannot exceed {self.result_row_limit}")
        return validate_and_normalize(request.sql, set(request.sources))

    @contextmanager
    def _slot(self):
        if not self._admission.acquire(timeout=self.admission_wait_seconds):
            raise BusyError("query concurrency limit reached")
        try:
            yield
        finally:
            self._admission.release()

    def _context(self, request: QueryRequest, context: ExecutionContext | None) -> ExecutionContext:
        timeout = self._timeout(request)
        if context is None:
            return ExecutionContext(timeout_seconds=timeout)
        return ExecutionContext(
            query_id=context.query_id,
            attempt_id=context.attempt_id,
            attempt_number=context.attempt_number,
            timeout_seconds=timeout,
            can_commit=context.can_commit,
            publish_output=context.publish_output,
            kind=context.kind,
        )

    def execute(
        self,
        request: QueryRequest,
        context: ExecutionContext | None = None,
    ) -> QueryResult:
        normalized_sql = self._validate(request)
        context = self._context(request, context)
        is_sync = context.kind == "sync"
        if is_sync:
            self._record(context.query_id, "sync", request)

        try:
            with self._slot(), query_timer(request.output.mode.value):
                result = self._execute_admitted(request, normalized_sql, context)
        except Exception as exc:
            if is_sync:
                self._update(
                    context.query_id,
                    "failed",
                    error=str(exc),
                    error_class=type(exc).__name__,
                )
            raise
        if is_sync:
            self._update(context.query_id, "succeeded", result=result)
        return result

    def _execute_admitted(
        self,
        request: QueryRequest,
        normalized_sql: str,
        context: ExecutionContext,
    ) -> QueryResult:
        ttl = self._ttl(request)
        cacheable = request.output.mode == DeliveryMode.INLINE and ttl > 0
        key: str | None = None
        prepared = self.engine.prepare(request, normalized_sql)

        if cacheable and prepared.cache_safe:
            key = query_fingerprint(
                request.model_copy(update={"sql": normalized_sql}),
                prepared.source_versions,
            )
            cached = self._cache_get(key, context.query_id)
            if cached is not None:
                return cached

        lock_token: str | None = None
        if key is not None:
            lock_seconds = max(self.singleflight_lock_seconds, context.timeout_seconds + 30)
            lock_token = self.cache.acquire(key, lock_seconds)
            if lock_token is None:
                cached = self.cache.wait(key, self.singleflight_wait_seconds)
                if cached is not None:
                    CACHE_RESULT.labels(result="coalesced").inc()
                    return cached.model_copy(update={"cached": True, "query_id": context.query_id})
                lock_token = self.cache.acquire(key, lock_seconds)
                if lock_token is None:
                    raise BusyError("equivalent query is already running")

        try:
            result = self.engine.execute(prepared, context)
            if isinstance(result, InlineResult):
                result = result.model_copy(update={"cached": False, "query_id": context.query_id})
                OUTPUT_BYTES.labels(mode="inline").inc(len(result.model_dump_json().encode()))
                if key is not None:
                    self.cache.set(key, result, ttl)
            return result
        finally:
            if key is not None and lock_token is not None:
                self.cache.release(key, lock_token)

    def _cache_get(self, key: str, query_id: str) -> InlineResult | None:
        cached = self.cache.get(key)
        CACHE_RESULT.labels(result="hit" if cached is not None else "miss").inc()
        if cached is None:
            return None
        return cached.model_copy(update={"cached": True, "query_id": query_id})

    def stream(
        self,
        request: QueryRequest,
        context: ExecutionContext | None = None,
    ) -> tuple[str, Iterator[str], str]:
        normalized_sql = self._validate(request)
        if request.output.mode != DeliveryMode.STREAM:
            raise QueryRejected("request output mode is not stream")
        context = self._context(request, context)
        is_sync = context.kind == "sync"
        if is_sync:
            self._record(context.query_id, "sync", request)
        if not self._admission.acquire(timeout=self.admission_wait_seconds):
            if is_sync:
                self._update(context.query_id, "failed", error="query concurrency limit reached")
            raise BusyError("query concurrency limit reached")
        try:
            prepared = self.engine.prepare(request, normalized_sql)
            media_type, rows = self.engine.stream(prepared, context)
        except Exception as exc:
            self._admission.release()
            if is_sync:
                self._update(
                    context.query_id,
                    "failed",
                    error=str(exc),
                    error_class=type(exc).__name__,
                )
            raise

        def guarded() -> Iterator[str]:
            finished = False
            failed = False
            try:
                with query_timer("stream"):
                    for chunk in rows:
                        OUTPUT_BYTES.labels(mode="stream").inc(len(chunk.encode()))
                        yield chunk
                finished = True
            except Exception as exc:
                failed = True
                if is_sync:
                    self._update(
                        context.query_id,
                        "failed",
                        error=str(exc),
                        error_class=type(exc).__name__,
                    )
                raise
            finally:
                if is_sync and finished:
                    self._update(context.query_id, "succeeded")
                elif is_sync and not failed:
                    self._update(
                        context.query_id,
                        "cancelled",
                        error="stream consumer disconnected before completion",
                    )
                self.engine.cancel(context.query_id)
                self._admission.release()

        return media_type, guarded(), context.query_id

    def affinity_key(self, request: QueryRequest) -> str:
        self._validate(request)
        return dataset_affinity_key(request)

    def explain(self, request: QueryRequest) -> ExplainResult:
        normalized = self._validate(request)
        context = ExecutionContext(
            query_id=str(uuid.uuid4()),
            timeout_seconds=self._timeout(request),
        )
        with self._slot():
            prepared = self.engine.prepare(request, normalized)
            plan = self.engine.explain(prepared, context)
        return ExplainResult(
            query_id=context.query_id,
            normalized_sql=normalized,
            source_versions=prepared.source_versions,
            plan=plan,
        )

    def inspect(self, source: SourceSpec) -> SourceInspection:
        with self._slot():
            return self.engine.inspect(source)

    def cancel(self, query_id: str) -> None:
        self.engine.cancel(query_id)

    def ready(self) -> bool:
        return self.cache.ping() and self.engine.ping()

    def _record(self, query_id: str, kind: str, request: QueryRequest) -> None:
        try:
            self.metadata.record_invocation(query_id, kind, request.model_dump_json())
        except Exception:
            logger.exception("metadata write failed", extra={"query_id": query_id})

    def _update(
        self,
        query_id: str,
        status: str,
        result: QueryResult | None = None,
        error: str | None = None,
        error_class: str | None = None,
    ) -> None:
        try:
            self.metadata.update_invocation(
                query_id,
                status,
                result=result,
                error=error,
                error_class=error_class,
            )
        except Exception:
            logger.exception("metadata write failed", extra={"query_id": query_id})

    def close(self) -> None:
        close_engine = getattr(self.engine, "close", None)
        if close_engine is not None:
            close_engine()
        close_cache = getattr(self.cache, "close", None)
        if close_cache is not None:
            close_cache()
