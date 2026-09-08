# Cache-path benchmark

Generate a scan-sized local Parquet dataset (5 GiB by default):

```bash
poetry run python benchmarks/generate_parquet.py data/benchmark-parquet --target-gib 5
```

Start Dalmatian with a clean process, use a versioned source, and set API admission to at least
the caller count. The single-flight wait must exceed the expected cold-query duration. Then run:

```bash
poetry run python benchmarks/cache_paths.py request.json \
  --warm-sql 'select count(*) as rows, min(id) as first_id from events' \
  --concurrent-sql 'select count(*) as rows, sum(length(payload)) as bytes from events' \
  --callers 20
```

The output covers a cold query, different SQL against the same warm source set, the median exact
result-cache hit, and simultaneous callers for a third uncached query. The concurrent measurement
should report one miss plus cache-backed followers. Use an affinity-aware ingress or one API replica
so dataset-cache measurements reach the same Spark application.
