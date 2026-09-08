from __future__ import annotations

import csv
import io
import json
import os
import re
import threading
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path

from dalmatian.datasets import DatasetCache, PreparedQuery, ResolvedSource, resolve_source
from dalmatian.errors import CommitFenced, QueryCancelled, QueryRejected, QueryTimeout
from dalmatian.execution import ExecutionContext
from dalmatian.models import (
    DataFormat,
    DeliveryMode,
    InlineResult,
    OutputFormat,
    QueryRequest,
    SourceColumn,
    SourceInspection,
    SourceSpec,
    StoredResult,
)
from dalmatian.observability import CACHE_DATASET
from dalmatian.storage import StorageRegistry


class SparkEngine:
    def __init__(
        self,
        master: str,
        app_name: str,
        data_root: Path,
        storage: StorageRegistry,
        dataset_cache_entries: int = 32,
        dataset_cache_ttl_seconds: int = 1800,
        max_inline_bytes: int = 8 * 1024 * 1024,
        spark_packages: str = "",
        spark_config: dict[str, str] | None = None,
        spark_app_max_cores: int = 4,
        spark_executor_cores: int = 2,
        spark_executor_memory: str = "2g",
        spark_executor_instances: int | None = None,
    ) -> None:
        from pyspark.sql import SparkSession

        self.data_root = data_root
        self.storage = storage
        self.max_inline_bytes = max_inline_bytes
        builder = (
            SparkSession.builder.appName(app_name)
            .master(master)
            .config("spark.scheduler.mode", "FAIR")
            .config("spark.cores.max", str(spark_app_max_cores))
            .config("spark.executor.cores", str(spark_executor_cores))
            .config("spark.executor.memory", spark_executor_memory)
        )
        # Kubernetes pod hostnames are not necessarily resolvable unless the pod
        # belongs to a headless service.  The chart supplies the pod IP through
        # the Downward API so standalone Spark executors can reach the driver.
        if spark_local_ip := os.environ.get("SPARK_LOCAL_IP"):
            builder = (
                builder.config("spark.driver.host", spark_local_ip)
                .config("spark.driver.bindAddress", spark_local_ip)
            )
        if spark_executor_instances is not None:
            builder = builder.config("spark.executor.instances", str(spark_executor_instances))
        if spark_packages:
            builder = builder.config("spark.jars.packages", spark_packages)
        for key, value in (spark_config or {}).items():
            builder = builder.config(key, value)
        self.spark = builder.getOrCreate()
        self.datasets: DatasetCache[str] = DatasetCache(
            max_entries=dataset_cache_entries,
            ttl_seconds=dataset_cache_ttl_seconds,
        )

    def prepare(self, request: QueryRequest, normalized_sql: str) -> PreparedQuery:
        sources = {
            name: resolve_source(source, self.storage, self.data_root)
            for name, source in request.sources.items()
        }
        return PreparedQuery(request=request, sources=sources, normalized_sql=normalized_sql)

    @staticmethod
    def _schema(source: SourceSpec) -> str | None:
        if source.schema_definition is None:
            return None
        if isinstance(source.schema_definition, str):
            return source.schema_definition
        return ", ".join(f"`{name}` {dtype}" for name, dtype in source.schema_definition.items())

    @staticmethod
    def _read_target(uri: str) -> tuple[str, dict[str, str]]:
        if "**" not in uri:
            return uri, {}
        prefix, suffix = uri.split("**", 1)
        base = prefix.rstrip("/")
        trailing = suffix.lstrip("/")
        options = {"recursiveFileLookup": "true"}
        basename = trailing.rsplit("/", 1)[-1] if trailing else ""
        if basename and "**" not in basename:
            options["pathGlobFilter"] = basename
        return base, options

    @staticmethod
    def _glob_regex(uri: str) -> str | None:
        if "**" not in uri:
            return None
        _, suffix = uri.split("**", 1)
        trailing = suffix.lstrip("/")
        if not trailing or ("/" not in trailing and "**" not in trailing):
            return None

        parts: list[str] = []
        index = 0
        while index < len(uri):
            if uri.startswith("**/", index):
                parts.append("(?:.*/)?")
                index += 3
                continue
            if uri.startswith("**", index):
                parts.append(".*")
                index += 2
                continue
            char = uri[index]
            if char == "*":
                parts.append("[^/]*")
            elif char == "?":
                parts.append("[^/]")
            else:
                parts.append(re.escape(char))
            index += 1
        return "^" + "".join(parts) + "$"

    def _apply_path_filter(self, frame, uri: str):
        pattern = self._glob_regex(uri)
        if pattern is None:
            return frame
        from pyspark.sql import functions as F

        return frame.where(F.input_file_name().rlike(pattern))

    def _read_source(self, source: SourceSpec, resolved: ResolvedSource, session=None):
        spark = session or self.spark
        if resolved.read_uris is None:
            target, recursive_options = self._read_target(resolved.uri)
        else:
            target, recursive_options = list(resolved.read_uris), {}
        read_options = {
            "dalmatianSourceFingerprint": resolved.fingerprint,
            **source.options,
            **recursive_options,
        }
        reader = spark.read.options(**read_options)
        schema = self._schema(source)
        if schema:
            reader = reader.schema(schema)
        if source.format == DataFormat.CSV:
            options = {
                "header": "true",
                "dalmatianSourceFingerprint": resolved.fingerprint,
                **source.options,
                **recursive_options,
            }
            if schema is None:
                options["inferSchema"] = str(source.infer_schema).lower()
            csv_reader = spark.read.options(**options)
            if schema:
                csv_reader = csv_reader.schema(schema)
            frame = csv_reader.csv(target)
            return self._apply_path_filter(frame, resolved.uri)
        if source.format == DataFormat.PARQUET:
            frame = reader.parquet(*target) if isinstance(target, list) else reader.parquet(target)
            return self._apply_path_filter(frame, resolved.uri)
        if source.format == DataFormat.JSON:
            frame = reader.json(target)
            return self._apply_path_filter(frame, resolved.uri)
        raise ValueError(f"unsupported source format: {source.format}")

    def _load_cached_view(self, source: SourceSpec, resolved: ResolvedSource) -> str:
        def load() -> str:
            from pyspark import StorageLevel

            view_name = f"_dalmatian_{resolved.fingerprint[:24]}"
            # SQL's in-memory columnar cache can exceed executor heap while materializing
            # compressed sources. Disk persistence keeps warm local reuse without making
            # source size an executor-memory correctness constraint.
            frame = self._read_source(source, resolved).persist(StorageLevel.DISK_ONLY)
            frame.createOrReplaceGlobalTempView(view_name)
            return view_name

        hits_before = self.datasets.hits
        view_name, hit = self.datasets.get_or_load(
            resolved.fingerprint,
            load,
            self._release_cached_view,
        )
        cache_result = "hit" if hit or self.datasets.hits > hits_before else "miss"
        CACHE_DATASET.labels(result=cache_result).inc()
        return view_name

    def _release_cached_view(self, view_name: str) -> None:
        with suppress(Exception):
            self.spark.table(f"global_temp.{view_name}").unpersist(blocking=False)
        with suppress(Exception):
            self.spark.catalog.dropGlobalTempView(view_name)

    def _frame(self, prepared: PreparedQuery):
        session = self.spark.newSession()
        for name, source in prepared.request.sources.items():
            resolved = prepared.sources[name]
            if resolved.cache_safe:
                global_view = self._load_cached_view(source, resolved)
                session.table(f"global_temp.{global_view}").createOrReplaceTempView(name)
            else:
                self._read_source(source, resolved, session=session).createOrReplaceTempView(name)
        return session.sql(prepared.normalized_sql)

    @contextmanager
    def _query_scope(self, context: ExecutionContext):
        sc = self.spark.sparkContext
        timed_out = threading.Event()

        def timeout() -> None:
            timed_out.set()
            sc.cancelJobGroup(context.query_id)

        timer = threading.Timer(context.timeout_seconds, timeout)
        sc.setJobGroup(
            context.query_id,
            f"Dalmatian query {context.query_id}",
            interruptOnCancel=True,
        )
        timer.start()
        try:
            yield
        except Exception as exc:
            if timed_out.is_set():
                raise QueryTimeout(f"query exceeded {context.timeout_seconds} seconds") from exc
            if "cancel" in str(exc).lower():
                raise QueryCancelled("query was cancelled") from exc
            from pyspark.errors import AnalysisException

            if isinstance(exc, AnalysisException):
                detail = str(exc).splitlines()[0][:500]
                raise QueryRejected(f"query analysis failed: {detail}") from exc
            raise
        finally:
            timer.cancel()
            sc.setLocalProperty("spark.jobGroup.id", None)

    def execute(
        self,
        prepared: PreparedQuery,
        context: ExecutionContext,
    ) -> InlineResult | StoredResult:
        output = prepared.request.output
        with self._query_scope(context):
            if output.mode == DeliveryMode.INLINE:
                return self._inline(prepared, context)
            if output.mode == DeliveryMode.STORE:
                return self._store(prepared, context)
        raise ValueError("stream output must use the streaming execution path")

    def _inline(self, prepared: PreparedQuery, context: ExecutionContext) -> InlineResult:
        frame = self._frame(prepared)
        max_rows = prepared.request.max_rows
        rows: list[dict[str, object]] = []
        encoded_bytes = 0
        truncated = False
        iterator = frame.limit(max_rows + 1).toJSON().toLocalIterator(prefetchPartitions=True)
        for index, raw in enumerate(iterator):
            if index >= max_rows:
                truncated = True
                break
            encoded_bytes += len(raw.encode())
            if encoded_bytes > self.max_inline_bytes:
                raise QueryRejected("inline result exceeds byte limit; use stream or store output")
            rows.append(json.loads(raw))
        return InlineResult(
            query_id=context.query_id,
            columns=frame.columns,
            rows=rows,
            truncated=truncated,
        )

    def stream(
        self,
        prepared: PreparedQuery,
        context: ExecutionContext,
    ) -> tuple[str, Iterator[str]]:
        output = prepared.request.output
        if output.mode != DeliveryMode.STREAM:
            raise ValueError("request output mode is not stream")
        def jsonl() -> Iterator[str]:
            with self._query_scope(context):
                frame = self._frame(prepared)
                for row in frame.toJSON().toLocalIterator(prefetchPartitions=True):
                    yield f"{row}\n"

        def csv_rows() -> Iterator[str]:
            with self._query_scope(context):
                frame = self._frame(prepared)
                yield from self._csv_rows(frame)

        if output.format == OutputFormat.JSONL:
            return "application/x-ndjson", jsonl()
        if output.format == OutputFormat.CSV:
            return "text/csv", csv_rows()
        raise ValueError("stream output format must be csv or jsonl")

    @staticmethod
    def _csv_rows(frame) -> Iterator[str]:
        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow(frame.columns)
        yield buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)
        for row in frame.toLocalIterator(prefetchPartitions=True):
            writer.writerow(list(row))
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate(0)

    def _store(self, prepared: PreparedQuery, context: ExecutionContext) -> StoredResult:
        output = prepared.request.output
        assert output.location is not None and output.path is not None
        if output.format == OutputFormat.JSON:
            raise ValueError("stored json uses jsonl format; json is reserved for inline output")
        logical = self.storage.resolve(output.location, output.path)
        data_uri = f"{logical.uri}/_dalmatian_data/{context.query_id}/{context.attempt_id}"
        manifest_uri = (
            f"{logical.uri}/_dalmatian_manifests/{context.query_id}/{context.attempt_id}.json"
        )
        frame = self._frame(prepared)
        partitions = frame.rdd.getNumPartitions()
        writer = frame.write.mode("errorifexists").options(**output.options)
        if output.format == OutputFormat.PARQUET:
            writer.parquet(data_uri)
        elif output.format == OutputFormat.CSV:
            writer.csv(data_uri)
        elif output.format == OutputFormat.JSONL:
            writer.json(data_uri)
        else:
            raise ValueError(f"unsupported stored output format: {output.format}")

        if not context.can_commit():
            raise CommitFenced("job lease changed before output publication")

        result = StoredResult(
            query_id=context.query_id,
            format=output.format,
            location=logical.location,
            path=logical.path,
            manifest_uri=manifest_uri,
            data_uri=data_uri,
            partitions=partitions,
            source_versions=prepared.source_versions,
        )
        manifest = {
            "version": 2,
            "query_id": context.query_id,
            "attempt_id": context.attempt_id,
            "attempt_number": context.attempt_number,
            "created_at": datetime.now(UTC).isoformat(),
            "result": result.model_dump(mode="json"),
        }
        self._write_manifest(
            manifest_uri,
            self.storage.manifest_payload(**manifest),
            overwrite=False,
        )

        publication_key = self.storage.publication_key(logical)
        if context.publish_output is not None:
            published = context.publish_output(
                publication_key,
                manifest_uri,
                output.write_mode.value,
            )
        else:
            published = int(
                self.storage.publish_sync(
                    logical,
                    context.query_id,
                    context.attempt_id,
                    manifest_uri,
                    output.write_mode.value,
                )
            )
        if published < 0:
            raise CommitFenced("job lease changed during output publication")
        if published == 0:
            raise FileExistsError(f"stored output already exists: {logical.path}")
        return result

    def _hadoop_path(self, uri: str):
        jvm = self.spark.sparkContext._jvm
        conf = self.spark.sparkContext._jsc.hadoopConfiguration()
        path = jvm.org.apache.hadoop.fs.Path(uri)
        fs = path.getFileSystem(conf)
        return jvm, fs, path

    def _write_manifest(self, uri: str, payload: bytes, overwrite: bool) -> None:
        _, fs, path = self._hadoop_path(uri)
        parent = path.getParent()
        if parent is not None:
            fs.mkdirs(parent)
        stream = fs.create(path, overwrite)
        try:
            stream.write(bytearray(payload))
        finally:
            stream.close()

    def explain(self, prepared: PreparedQuery, context: ExecutionContext) -> str:
        with self._query_scope(context):
            frame = self._frame(prepared)
            return frame._jdf.queryExecution().toString()

    def inspect(self, source: SourceSpec) -> SourceInspection:
        resolved = resolve_source(source, self.storage, self.data_root)
        frame = self._read_source(source, resolved)
        return SourceInspection(
            location=source.location or "local",
            resolved_uri=resolved.uri,
            fingerprint=resolved.fingerprint if resolved.cache_safe else None,
            cache_safe=resolved.cache_safe,
            columns=[
                SourceColumn(
                    name=field.name,
                    type=field.dataType.simpleString(),
                    nullable=field.nullable,
                )
                for field in frame.schema.fields
            ],
        )

    def cancel(self, query_id: str) -> None:
        self.spark.sparkContext.cancelJobGroup(query_id)

    def ping(self) -> bool:
        return not bool(self.spark.sparkContext._jsc.sc().isStopped())

    def close(self) -> None:
        self.datasets.close()
        self.spark.stop()
