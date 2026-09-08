<h1>
  Dalmatian
  <img src="docs/dalmatian.png" height="30" alt="Dalmatian logo">
</h1>

<p>
  <img alt="Python 3.12" src="https://img.shields.io/badge/python-3.12-3776AB?logo=python&logoColor=white">
  <img alt="FastAPI" src="https://img.shields.io/badge/FastAPI-0.116+-009688?logo=fastapi&logoColor=white">
  <img alt="PySpark" src="https://img.shields.io/badge/PySpark-4.2-E25A1C?logo=apachespark&logoColor=white">
  <img alt="Valkey" src="https://img.shields.io/badge/Valkey-8.x-FF4438?logo=redis&logoColor=white">
</p>

Dalmatian is an HTTP service for running read-only Spark SQL over CSV, Parquet, and JSON datasets.
It is built for application-owned data services: an API needs to query files, return bounded results,
stream larger responses, or run work asynchronously without every caller having to manage Spark,
job state, caching, retries, and output storage itself.

Dalmatian is designed to run as a real internal service, not as a notebook wrapper. The repository
includes health checks, Prometheus metrics, JSON logs, admission control, query deadlines,
idempotent async submission, lease-based retries, Kubernetes probes and resource controls,
non-root containers, optional NetworkPolicies, and a Helm chart.

> 🛡️ **Authentication and TLS are intentionally not implemented in the application.** Put Dalmatian behind your normal internal ingress, API gateway, service mesh, or identity-aware proxy.

## Where Dalmatian fits

Use Dalmatian when:

- several applications need the same file-backed query API;
- the data is large enough that Spark is useful;
- repeated queries or repeated reads of the same dataset should be reused;
- some queries need asynchronous execution, retries, cancellation, or stored output;
- operators need health checks, metrics, bounded concurrency, and Kubernetes deployment controls;
- you want callers to use approved dataset and output locations instead of arbitrary storage URIs.

Dalmatian is probably not the right tool when:

- one process can query local files directly;
- all work fits comfortably on one machine and an embedded engine is enough;
- you need federation across many databases and catalogs;
- you need a general-purpose BI/JDBC/ODBC query platform;
- you need application-level authentication or tenant authorization built into the query service.

## Dalmatian, DuckDB, and Trino

DuckDB is the closest alternative when the problem is "run analytical SQL over files". The main
difference is where the query engine lives and how much service infrastructure you need around it.

|                               | Dalmatian                                       | DuckDB                                                     | Trino                                                |
| ----------------------------- | ----------------------------------------------- | ---------------------------------------------------------- | ---------------------------------------------------- |
| **Best fit**                  | HTTP service for querying files with Spark      | Embedded analytics inside an application or on one machine | Distributed SQL across many data sources             |
| **Deployment model**          | Standalone service                              | Embedded library / local process                           | Distributed service                                  |
| **Execution**                 | Spark, including multi-worker execution         | Primarily single-machine                                   | Distributed workers                                  |
| **File formats**              | CSV, Parquet, JSON                              | CSV, Parquet, JSON                                         | Supported through configured catalogs/connectors     |
| **HTTP API**                  | Built in                                        | Requires an application layer                              | Typically accessed through SQL clients and protocols |
| **Caching and dataset reuse** | Built in with Valkey and Spark persistence      | Managed within the application/process                     | Depends on the surrounding infrastructure            |
| **Async query handling**      | Jobs, retries, leases and cancellation built in | Requires an application layer                              | Usually handled outside the query engine             |
| **Result delivery**           | JSON, CSV/JSONL streaming, or stored output     | Determined by the application                              | Determined by the client/protocol                    |
| **Federated queries**         | Not a primary use case                          | Limited                                                    | Core use case                                        |
| **BI / JDBC ecosystem**       | Not a primary use case                          | Supported, but not the main deployment model               | Core use case                                        |


A practical rule:
- choose **Dalmatian** when you want an HTTP service boundary, Spark-scale execution, and the
  operational pieces around repeated and asynchronous file queries;
- choose **DuckDB** when the caller can run the query in-process or on one machine;
- choose **Trino** when you are building a shared SQL platform with catalogs, federation, and many
  users or tools.

Dalmatian is deliberately narrower than Trino and more service-oriented than DuckDB.

## Quick start

The easiest way to try Dalmatian is Docker Compose. The repository includes a tiny CSV dataset at
`data/example/orders.csv`.

### 1. Start the stack

```bash
docker compose up --build
```

This starts the API, Valkey, a Spark master, and two Spark workers.

Useful local endpoints:

```text
API              http://localhost:8000
OpenAPI          http://localhost:8000/docs
Liveness         http://localhost:8000/health/live
Readiness        http://localhost:8000/health/ready
Metrics          http://localhost:8000/metrics
Spark master UI  http://localhost:8080
```

### 2. Run a query

```bash
curl -s http://localhost:8000/v1/query \
  -H 'content-type: application/json' \
  -d '{
    "sql": "select country, sum(total) as revenue from orders group by country order by revenue desc",
    "sources": {
      "orders": {
        "path": "example/orders.csv",
        "format": "csv",
        "infer_schema": true
      }
    }
  }'
```

A normal inline response looks like this:

```json
{
  "type": "inline",
  "query_id": "2ff1d3ca-6034-4374-9457-ceafe107b28f",
  "columns": ["country", "revenue"],
  "rows": [
    {"country": "US", "revenue": 210.0},
    {"country": "DE", "revenue": 120.5},
    {"country": "FI", "revenue": 89.9},
    {"country": "NL", "revenue": 42.0}
  ],
  "truncated": false,
  "cached": false
}
```

Every synchronous response includes `X-Dalmatian-Query-Id`. Spark uses the same ID as its job group,
which makes cancellation and log correlation straightforward.

### 3. Try the API docs

Open `http://localhost:8000/docs` to inspect the request models and call endpoints without writing a
client first.

## How it works

A source alias in a request becomes a temporary Spark SQL view for that query.

<img src="docs/flowchart.png" alt="Application Flowchart">

There are two kinds of distribution:

1. Spark distributes one query across executors.
2. Valkey distributes independent asynchronous jobs across long-lived Dalmatian workers.

Async queues use source-based affinity shards. Repeat jobs prefer the worker shard that previously
handled the same source set, which improves the chance that the worker already has a persisted
DataFrame. Idle workers can steal from other shards, so affinity does not reserve capacity.

## Query inputs

Supported source formats:

- CSV
- Parquet
- JSON

Local sources are resolved below `DALMATIAN_DATA_ROOT`.

Named remote roots are configured with `DALMATIAN_SOURCE_LOCATIONS`:

```json
{
  "lake": "s3a://company-data/analytics",
  "gcs": "gs://company-data/analytics",
  "azure": "abfss://analytics@company.dfs.core.windows.net"
}
```

A request refers to the configured name rather than supplying an arbitrary remote root:

```json
{
  "path": "events/**/*.parquet",
  "format": "parquet",
  "location": "lake",
  "version": "events-run-1842"
}
```

Recursive local `**` patterns are resolved to exact files before Spark reads them. Remote patterns
use Spark recursive file lookup and Dalmatian preserves path constraints after `**` so a pattern such
as `files/**/2026/**/*.parquet` cannot silently read a neighboring year.

### CSV schemas

CSV schema inference is opt-in because inference adds another data pass.

For repeat or large workloads, provide a schema:

```json
{
  "path": "orders/**/*.csv",
  "format": "csv",
  "schema": {
    "order_id": "long",
    "customer_id": "long",
    "total": "decimal(18,2)",
    "created_at": "timestamp"
  }
}
```

For a small convenience-oriented input:

```json
{
  "path": "small-file.csv",
  "format": "csv",
  "infer_schema": true
}
```

Parquet carries its own schema. JSON can also receive an explicit Spark DDL or field-to-type schema.

## Result delivery

Dalmatian has three output modes.

| Mode | Sync | Async | Formats | Use it for |
| --- | --- | --- | --- | --- |
| `inline` | Yes | Yes | JSON | Small bounded API results |
| `stream` | Yes | No | CSV, JSONL | Larger results consumed incrementally |
| `store` | Yes | Yes | CSV, JSONL, Parquet | Durable distributed output |

Inline execution reads at most `max_rows + 1` records. It does not launch a second Spark action just
to calculate a total row count.

### Stream a result

```json
{
  "output": {
    "mode": "stream",
    "format": "jsonl"
  }
}
```

CSV writes one header followed by rows. JSONL writes one object per line. If the client disconnects,
Dalmatian closes the generator and cancels the Spark job group.

### Store a result

Store mode writes only to operator-approved locations.

```json
{
  "output": {
    "mode": "store",
    "format": "parquet",
    "location": "analytics",
    "path": "exports/customer-revenue/2026-09-07",
    "write_mode": "overwrite"
  }
}
```

Stored output returns artifact metadata instead of collecting rows into the API process.

```json
{
  "type": "stored",
  "query_id": "job-or-query-id",
  "format": "parquet",
  "location": "analytics",
  "path": "exports/customer-revenue/2026-09-07",
  "manifest_uri": "s3a://bucket/exports/customer-revenue/2026-09-07/_dalmatian_manifests/job-or-query-id/attempt-id.json",
  "data_uri": "s3a://bucket/exports/customer-revenue/2026-09-07/_dalmatian_data/job-or-query-id/attempt-id",
  "partitions": 24,
  "source_versions": {
    "orders": "..."
  }
}
```

`json` is reserved for inline API output. Spark file JSON output is line-oriented, so stored JSON uses
the explicit `jsonl` name.

## Repeat-query acceleration

Dalmatian uses two cache layers because an identical query and a new query over the same files are
not the same optimization problem.

### Exact result cache

Inline results can be compressed and stored in Valkey. The fingerprint includes normalized SQL,
source configuration, source versions, and the inline result shape.

A hit skips Spark completely.

Entries have a compressed byte limit. Oversized results are returned normally and are not inserted
into Valkey.

SQL is parsed and canonicalized before fingerprinting, so formatting-only differences normally map
to the same result key.

### Hot Spark dataset cache

Each long-lived API process and async worker keeps a bounded LRU of source DataFrames persisted with
Spark `DISK_ONLY` storage.

That helps when the SQL changes but the source files do not. The dataset cache is lazy; Dalmatian
does not force a `count()` just to warm it.

With multiple API replicas, dataset reuse is best-effort unless repeat synchronous requests reach the
same pod. `POST /v1/query/affinity` returns a stable source-only key and query responses include
`X-Dalmatian-Affinity-Key`. The Helm ingress can hash that header for sticky dataset locality.

Exact-result caching does not require pod affinity because Valkey is shared.

### Dataset versions

Local sources can be fingerprinted from file metadata when no explicit version is supplied.

For remote object storage, callers should provide a stable `version` such as a release ID, partition
build ID, table snapshot ID, or upstream run ID. Dalmatian does not recursively list a large object
store tree on every request to prove that nothing changed.

Remote sources without a version are treated as cache-unsafe across requests. Spark still reads them,
but Dalmatian skips cross-request exact-result and persisted-dataset reuse for that source.

### Single-flight cache misses

An exact cache miss takes a short Valkey lease around the query fingerprint. If many callers request
the same cold result at once, one caller computes it while followers briefly wait for the cached
answer. A follower that still sees a live owner after the wait receives `429` rather than starting
duplicate Spark work.

## Asynchronous jobs

Submit a job:

```bash
curl -i http://localhost:8000/v1/jobs \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: customer-revenue-2026-09-07' \
  -d @request.json
```

`Idempotency-Key` is optional. Reusing a key with the same request returns the existing job. Reusing
it with a different request returns `409`.

The API returns `202 Accepted` and:

```text
Location: /v1/jobs/<job-id>
```

Poll or cancel the job:

```bash
curl -s http://localhost:8000/v1/jobs/<job-id>
curl -X DELETE http://localhost:8000/v1/jobs/<job-id>
```

Job states are:

```text
queued -> running -> succeeded
                  -> failed
                  -> cancelled

running -> retrying -> queued
```

Workers claim jobs with a lease and heartbeat while Spark is active. Completion is ownership-fenced
in Valkey, so a worker that loses its lease cannot write the terminal job state or publish a stored
result for an attempt it no longer owns.

Transient connection, executor-loss, and fetch failures are retry candidates. Invalid SQL, invalid
request configuration, output conflicts, and query deadlines fail without a retry. Exhausted jobs
are added to a dead-letter list in Valkey.

A worker receiving `SIGTERM` stops claiming new work and uses the Kubernetes grace period for its
current action. If the process is killed, the lease expires and another worker can retry the job.

## Stored-output safety

Asynchronous delivery is at-least-once. A worker can disappear after Spark has started writing but
before Valkey records the final job state. Dalmatian avoids writing retry attempts directly into one
mutable output directory.

Each attempt writes to an immutable path:

```text
<logical output>/_dalmatian_data/<job_id>/<attempt_id>/
```

It also writes an immutable manifest under:

```text
_dalmatian_manifests/<query_id>/<attempt_id>.json
```

The logical destination is published through Valkey. For async jobs, publication verifies running
state, worker ownership, and attempt number before moving the logical pointer. A stale worker may
finish an object-store write, but it cannot make that attempt authoritative after ownership changes.

`error` publishes only when the logical destination is unused, while allowing the same async job to
retry idempotently. `overwrite` moves the logical pointer to the new immutable manifest. `append`
records a run-specific manifest without moving the current overwrite pointer.

Old immutable attempt data should be cleaned up by a storage lifecycle policy after your retention
window.

## Query limits and SQL boundary

Each request can set `timeout_seconds` up to the operator-configured maximum. Dalmatian assigns the
query ID as the Spark job group and cancels that group when the deadline expires or an async job is
cancelled.

Each API and worker process has a bounded query semaphore. When all slots are busy, a new sync
request waits only for `DALMATIAN_ADMISSION_WAIT_SECONDS` and then receives `429` instead of growing
an unbounded queue inside one Spark driver.

Async submission also has a queue-depth ceiling.

Dalmatian parses SQL before giving it to Spark. Requests are limited to one read-only query and may
reference only the source aliases declared in the request plus CTE names created by that query. That
blocks direct DDL/DML and direct catalog-qualified access.

This SQL boundary is not authentication. Treat the API as an internal service and enforce identity,
network access, TLS, and tenant policy outside Dalmatian.

## Explain and inspect

Get a Spark plan without executing the query action:

```bash
curl -s http://localhost:8000/v1/query/explain \
  -H 'content-type: application/json' \
  -d @request.json
```

Inspect a source schema without counting rows:

```bash
curl -s http://localhost:8000/v1/sources/inspect \
  -H 'content-type: application/json' \
  -d '{
    "path": "sales/**/*.parquet",
    "format": "parquet"
  }'
```

## Optional PostgreSQL or MySQL metadata

Valkey remains the live coordination system. PostgreSQL or MySQL is optional and is used for durable
invocation history and centrally managed output locations.

Set a SQLAlchemy URL:

```bash
DALMATIAN_METADATA_URL=postgresql+psycopg://dalmatian:secret@postgres/dalmatian
```

or:

```bash
DALMATIAN_METADATA_URL=mysql+pymysql://dalmatian:secret@mysql/dalmatian
```

When enabled, Dalmatian records invocation IDs, status, attempt count, request JSON, scalar result
metadata, error information, output destination, and timestamps. Inline result bodies are not copied
into SQL.

The database can also provide named storage locations through `dalmatian_storage_locations`. Static
environment locations win when the same name exists in both places.

Run migrations before starting an application process with metadata enabled:

```bash
poetry run dalmatian-admin metadata-upgrade
poetry run dalmatian-admin metadata-current
```

Manage database-backed locations with:

```bash
poetry run dalmatian-admin storage-add analytics s3a://company-results/dalmatian
poetry run dalmatian-admin storage-list
poetry run dalmatian-admin storage-delete analytics
```

SQL metadata is an audit/config projection. Job leasing and correctness do not depend on a
cross-system transaction between the database and Valkey.

## Observability

`GET /metrics` exposes Prometheus metrics including:

```text
dalmatian_result_cache_total
dalmatian_dataset_cache_total
dalmatian_job_queue_depth
dalmatian_job_queue_wait_seconds
dalmatian_query_duration_seconds
dalmatian_active_queries
dalmatian_stale_job_recoveries_total
dalmatian_output_bytes_total
dalmatian_job_outcomes_total
```

Application logs are JSON and include query or job identifiers where available. Spark uses the same
query ID as its job group.

## Cloud storage

Dalmatian delegates S3A, GCS, and ADLS Gen2 I/O to Spark's Hadoop filesystem layer.

Supported URI schemes:

```text
file://
s3:// or s3a://
gs://
abfss://
```

`DALMATIAN_SPARK_PACKAGES` installs connector JARs when the Spark image does not already contain
them. `DALMATIAN_SPARK_CONFIG` supplies Spark and `spark.hadoop.*` settings.

Use workload identity, IAM roles, mounted secrets, or a Hadoop credential provider for cloud
credentials. Query bodies never carry cloud credentials.

## Production deployment

Dalmatian includes the controls expected from an internal service, but a production deployment still
needs an operator-owned environment around it.

Before exposing it to internal callers:

- put the API behind your standard authentication and TLS layer;
- use an external or highly available Valkey service for job and cache state;
- use an intentionally sized Spark cluster rather than the small bundled demo cluster;
- configure CPU and memory requests/limits for API, workers, and Spark executors;
- keep query concurrency and maximum timeouts bounded;
- use workload identity or secret references for cloud and database credentials;
- enable and narrow the NetworkPolicy if your cluster uses network isolation;
- run metadata migrations explicitly when PostgreSQL/MySQL metadata is enabled;
- supply stable versions for remote datasets that should participate in caches;
- scrape API and worker Prometheus metrics and alert on queue depth, failures, latency, and stale-job recoveries;
- apply object-storage lifecycle rules to old immutable attempt output;
- test your real data volume, query mix, and failure modes before choosing replica and Spark capacity.

The bundled Spark and Valkey components are meant for development, evaluation, or small controlled
environments. The Helm chart can disable them and point Dalmatian at external services.

### Kubernetes with Helm

```bash
helm upgrade --install dalmatian ./helm/dalmatian \
  --set data.existingClaim=my-data-rwx
```

The chart includes:

- API Deployment;
- async worker StatefulSet with stable affinity ordinals;
- optional standalone Spark master/workers;
- optional standalone Valkey with persistence;
- PVC hooks for local input/output;
- PodDisruptionBudgets;
- service account annotations for workload identity;
- optional NetworkPolicy;
- optional API CPU HPA;
- optional worker HPA using an external queue-depth metric;
- resource requests and limits;
- startup, readiness, and liveness probes;
- topology, affinity, toleration, and node-selector hooks;
- config checksum pod annotations.

The bundled Spark workers provide four cores in total. Each API or worker Spark application is capped
at two cores by default. Increase `config.sparkAppMaxCores` only with a matching cluster-capacity
plan.

When SQL metadata is enabled, `metadataMigrations.enabled=true` runs the pre-install/pre-upgrade
Alembic job. `config.metadataUrlSecret` can reference a Kubernetes Secret so database credentials do
not have to live in Helm release values.

See `helm/dalmatian/values.yaml` for deployment settings and comments.

## Configuration

For local and Compose configuration, start with `.env.example`.

The most important groups are:

| Area | Settings |
| --- | --- |
| Spark | `DALMATIAN_SPARK_MASTER`, executor cores/memory, application core cap |
| Coordination | `DALMATIAN_VALKEY_URL`, queue name, job TTL, lease and retry settings |
| Inputs | `DALMATIAN_DATA_ROOT`, `DALMATIAN_SOURCE_LOCATIONS` |
| Outputs | `DALMATIAN_STORAGE_LOCATIONS`, publication prefix |
| Result limits | row limit, inline byte limit, query timeout |
| Caching | result-cache TTL/size, dataset-cache entries/TTL |
| Concurrency | maximum concurrent queries, admission wait, queue depth |
| Metadata | `DALMATIAN_METADATA_URL` |
| Cloud connectors | `DALMATIAN_SPARK_PACKAGES`, `DALMATIAN_SPARK_CONFIG` |
| Logging/metrics | log level and worker metrics host/port |

Pydantic validates bounds on the runtime settings. Helm values map the same operational controls into
Kubernetes.

## Development

Requirements:

- Python 3.12
- Java 17
- Poetry

Install and run the fast test suite:

```bash
poetry install
poetry run pytest -m "not integration"
poetry run ruff check src tests benchmarks
```

Run integration tests when Spark and the required services are available:

```bash
poetry run pytest -m integration
```

CI also checks Poetry metadata, Helm rendering/linting, Docker Compose configuration, and the Docker
image build.

## Reproducible benchmark

The repository contains benchmark scripts under `benchmarks/`.

One recorded local run used:

- 5.001 GiB of Parquet data;
- 404 files and 40.4 million rows;
- Docker with 8 CPUs and 8 GiB RAM;
- two Spark workers with 2 cores / 2 GiB each;
- a Dalmatian application cap of 2 cores and 2 GiB executor memory.

Recorded results:

- cold dataset, new query: 70.9569 s;
- warm disk-persisted dataset, different query: 11.4718 s;
- exact Valkey result-cache hit: 4.4 ms median over 5 requests;
- 20 concurrent identical callers: 42.5698 s total, with 1 execution and 19 followers.

These are local validation numbers, not capacity promises. Recreate the dataset and run the same
workload with:

```text
benchmarks/generate_parquet.py
benchmarks/request.5g.json
benchmarks/cache_paths.py
```

## Implementation invariants

A few rules drive the implementation:

- no hidden result `count()` action;
- a cache hit must be tied to a known dataset version;
- a remote dataset without a version is never assumed unchanged;
- a stale async worker cannot complete a job owned by another worker;
- a stale async worker cannot publish stored output;
- retries write to unique attempt paths;
- query bodies cannot choose arbitrary storage roots;
- CSV schema inference is opt-in;
- large outputs have a stream or store path instead of unbounded JSON collection;
- Valkey coordinates work; optional SQL metadata does not participate in leases.

## License

MIT. See [LICENSE](LICENSE).
