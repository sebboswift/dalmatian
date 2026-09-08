from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_env_example_explains_operational_settings() -> None:
    text = (ROOT / ".env.example").read_text()
    assert "# Valkey is the coordination plane" in text
    assert "# Admission control" in text
    assert "DALMATIAN_METADATA_URL=" in text


def test_helm_values_explain_cloud_and_queue_settings() -> None:
    text = (ROOT / "helm" / "dalmatian" / "values.yaml").read_text()
    assert "# Named remote input roots" in text
    assert "# Async queue retention" in text
    assert "metadataUrl:" in text


def test_env_and_helm_document_fencing_metrics_and_spark_budgets() -> None:
    env = (ROOT / ".env.example").read_text()
    values = (ROOT / "helm" / "dalmatian" / "values.yaml").read_text()
    assert "DALMATIAN_SPARK_APP_MAX_CORES" in env
    assert "DALMATIAN_OUTPUT_PUBLICATION_PREFIX" in env
    assert "DALMATIAN_IDEMPOTENCY_TTL_SECONDS" in env
    assert "workerMetricsPort" in values
    assert "metadataMigrations:" in values
    assert "metadataUrlSecret:" in values
    assert "datasetAffinity:" in values


def test_bundled_cluster_default_prevents_one_application_taking_every_core() -> None:
    env = (ROOT / ".env.example").read_text()
    compose = (ROOT / "docker-compose.yml").read_text()
    values = (ROOT / "helm" / "dalmatian" / "values.yaml").read_text()
    assert "DALMATIAN_SPARK_APP_MAX_CORES=2" in env
    assert "DALMATIAN_SPARK_APP_MAX_CORES:-2" in compose
    assert "sparkAppMaxCores: 2" in values


def test_compose_passes_documented_operator_tuning_to_application_containers() -> None:
    compose = (ROOT / "docker-compose.yml").read_text()
    expected = {
        "DALMATIAN_DEFAULT_CACHE_TTL_SECONDS",
        "DALMATIAN_RESULT_CACHE_MAX_BYTES",
        "DALMATIAN_DATASET_CACHE_ENTRIES",
        "DALMATIAN_MAX_CONCURRENT_QUERIES",
        "DALMATIAN_ADMISSION_WAIT_SECONDS",
        "DALMATIAN_JOB_LEASE_SECONDS",
        "DALMATIAN_WORKER_REAP_INTERVAL_SECONDS",
        "DALMATIAN_WORKER_MAX_ATTEMPTS",
        "DALMATIAN_WORKER_RETRY_BASE_SECONDS",
        "DALMATIAN_WORKER_RETRY_MAX_SECONDS",
        "DALMATIAN_MAX_QUEUE_DEPTH",
        "DALMATIAN_WORKER_METRICS_PORT",
    }
    for name in expected:
        assert f"{name}:" in compose


def test_dalmatian_image_matches_bundled_spark_shared_volume_identity() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text()
    values = (ROOT / "helm" / "dalmatian" / "values.yaml").read_text()
    assert "useradd --uid 185 --gid 185" in dockerfile
    assert "runAsUser: 185" in values
    assert "runAsGroup: 185" in values
    assert "fsGroup: 185" in values


def test_helm_uses_pod_ip_for_spark_master_and_string_byte_limits() -> None:
    values = (ROOT / "helm" / "dalmatian" / "values.yaml").read_text()
    master = (ROOT / "helm" / "dalmatian" / "templates" / "spark-master.yaml").read_text()
    assert 'resultCacheMaxBytes: "4194304"' in values
    assert 'maxInlineBytes: "8388608"' in values
    assert 'fieldPath: status.podIP' in master
    assert '- "$(SPARK_LOCAL_IP)"' in master
