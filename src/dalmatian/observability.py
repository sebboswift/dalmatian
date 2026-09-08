from __future__ import annotations

import json
import logging
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime

from prometheus_client import Counter, Gauge, Histogram

QUERY_SECONDS = Histogram("dalmatian_query_duration_seconds", "Query execution time", ["mode"])
QUEUE_WAIT_SECONDS = Histogram("dalmatian_job_queue_wait_seconds", "Async queue wait time")
ACTIVE_QUERIES = Gauge("dalmatian_active_queries", "Queries currently executing", ["mode"])
CACHE_RESULT = Counter("dalmatian_result_cache_total", "Exact result cache lookups", ["result"])
CACHE_DATASET = Counter("dalmatian_dataset_cache_total", "Spark dataset cache lookups", ["result"])
STALE_RECOVERIES = Counter("dalmatian_stale_job_recoveries_total", "Recovered stale jobs")
OUTPUT_BYTES = Counter(
    "dalmatian_output_bytes_total",
    "Bytes returned by inline or stream output",
    ["mode"],
)
JOB_OUTCOMES = Counter("dalmatian_job_outcomes_total", "Async job terminal outcomes", ["status"])
JOB_QUEUE_DEPTH = Gauge("dalmatian_job_queue_depth", "Queued, retrying, and running async jobs")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        standard = {
            "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
            "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
            "created", "msecs", "relativeCreated", "thread", "threadName",
            "processName", "process", "taskName",
        }
        for name, value in record.__dict__.items():
            if name in standard or name.startswith("_"):
                continue
            if isinstance(value, (str, int, float, bool)) or value is None:
                payload[name] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"))


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())


@contextmanager
def query_timer(mode: str) -> Iterator[None]:
    ACTIVE_QUERIES.labels(mode=mode).inc()
    started = time.monotonic()
    try:
        yield
    finally:
        QUERY_SECONDS.labels(mode=mode).observe(time.monotonic() - started)
        ACTIVE_QUERIES.labels(mode=mode).dec()
