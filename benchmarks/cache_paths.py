from __future__ import annotations

import argparse
import json
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path

import httpx


def post(client: httpx.Client, path: str, payload: dict, headers: dict[str, str] | None = None):
    started = time.perf_counter()
    response = client.post(path, json=payload, headers=headers)
    elapsed = time.perf_counter() - started
    response.raise_for_status()
    return elapsed, response


def median_run(
    client: httpx.Client,
    path: str,
    payload: dict,
    headers: dict[str, str],
    repeats: int,
) -> float:
    values = [post(client, path, payload, headers)[0] for _ in range(repeats)]
    return statistics.median(values)


def concurrent_run(
    url: str,
    payload: dict,
    headers: dict[str, str],
    callers: int,
) -> tuple[list[float], list[str]]:
    barrier = threading.Barrier(callers)

    def call() -> tuple[float, str]:
        with httpx.Client(base_url=url, timeout=3600) as client:
            barrier.wait()
            elapsed, response = post(client, "/v1/query", payload, headers)
            return elapsed, response.headers.get("X-Dalmatian-Result-Cache", "missing")

    with ThreadPoolExecutor(max_workers=callers) as executor:
        outcomes = list(executor.map(lambda _index: call(), range(callers)))
    return [outcome[0] for outcome in outcomes], [outcome[1] for outcome in outcomes]


def main() -> None:
    parser = argparse.ArgumentParser(description="Measure Dalmatian cache paths over HTTP")
    parser.add_argument("request", type=Path, help="Inline request JSON for the baseline query")
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument(
        "--warm-sql",
        required=True,
        help="Different SQL over the same sources. Its exact repeat is measured next.",
    )
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument(
        "--concurrent-sql",
        required=True,
        help="A third, uncached SQL query used for the single-flight concurrency measurement.",
    )
    parser.add_argument("--callers", type=int, default=20)
    args = parser.parse_args()

    base = json.loads(args.request.read_text())
    if (base.get("output") or {}).get("mode", "inline") != "inline":
        raise SystemExit("benchmark request must use inline output")
    warm = deepcopy(base)
    warm["sql"] = args.warm_sql
    concurrent = deepcopy(base)
    concurrent["sql"] = args.concurrent_sql

    with httpx.Client(base_url=args.url, timeout=3600) as client:
        _, affinity_response = post(client, "/v1/query/affinity", base)
        affinity = affinity_response.json()["key"]
        headers = {"X-Dalmatian-Affinity-Key": affinity}

        cold_seconds, cold_response = post(client, "/v1/query", base, headers)
        warm_dataset_seconds, warm_response = post(client, "/v1/query", warm, headers)
        exact_seconds = median_run(client, "/v1/query", warm, headers, args.repeats)
        concurrent_seconds, concurrent_cache = concurrent_run(
            args.url,
            concurrent,
            headers,
            args.callers,
        )

    print(f"affinity_key={affinity}")
    print(
        "cold_query_seconds="
        f"{cold_seconds:.4f} cache={cold_response.headers.get('X-Dalmatian-Result-Cache')}"
    )
    print(
        "warm_dataset_new_sql_seconds="
        f"{warm_dataset_seconds:.4f} cache={warm_response.headers.get('X-Dalmatian-Result-Cache')}"
    )
    print(f"exact_result_cache_median_seconds={exact_seconds:.4f}")
    print(f"concurrent_callers={args.callers}")
    print(f"concurrent_wall_seconds={max(concurrent_seconds):.4f}")
    print(f"concurrent_cache_misses={concurrent_cache.count('miss')}")
    print(f"concurrent_cache_followers={concurrent_cache.count('hit')}")


if __name__ == "__main__":
    main()
