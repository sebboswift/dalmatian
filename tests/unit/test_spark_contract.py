from pathlib import Path

SPARK_MODULE = Path(__file__).resolve().parents[2] / "src" / "dalmatian" / "spark.py"


def test_query_path_does_not_precount_results() -> None:
    source = SPARK_MODULE.read_text()
    assert ".count(" not in source


def test_dataset_reuse_does_not_require_source_to_fit_executor_heap() -> None:
    source = SPARK_MODULE.read_text()
    assert "StorageLevel.DISK_ONLY" in source
    assert "StorageLevel.MEMORY_AND_DISK" not in source


def test_kubernetes_driver_advertises_its_pod_ip() -> None:
    source = SPARK_MODULE.read_text()
    assert 'os.environ.get("SPARK_LOCAL_IP")' in source
    assert 'config("spark.driver.host", spark_local_ip)' in source
    assert 'config("spark.driver.bindAddress", spark_local_ip)' in source


def test_spark_analysis_errors_are_client_rejections() -> None:
    source = SPARK_MODULE.read_text()
    assert "isinstance(exc, AnalysisException)" in source
    assert 'raise QueryRejected(f"query analysis failed: {detail}")' in source


def test_recursive_glob_translates_to_spark_recursive_lookup() -> None:
    from dalmatian.spark import SparkEngine

    target, options = SparkEngine._read_target("s3a://bucket/files/**/*.parquet")
    assert target == "s3a://bucket/files"
    assert options == {"recursiveFileLookup": "true", "pathGlobFilter": "*.parquet"}

    target, options = SparkEngine._read_target("s3a://bucket/files/**/**")
    assert target == "s3a://bucket/files"
    assert options == {"recursiveFileLookup": "true"}


def test_nested_recursive_glob_keeps_directory_constraints() -> None:
    from dalmatian.spark import SparkEngine

    target, options = SparkEngine._read_target(
        "s3a://bucket/files/**/2026/**/*.parquet"
    )
    assert target == "s3a://bucket/files"
    assert options == {
        "recursiveFileLookup": "true",
        "pathGlobFilter": "*.parquet",
    }
    pattern = SparkEngine._glob_regex("s3a://bucket/files/**/2026/**/*.parquet")
    assert pattern is not None

    import re

    assert re.match(pattern, "s3a://bucket/files/region/eu/2026/09/orders.parquet")
    assert re.match(pattern, "s3a://bucket/files/2026/orders.parquet")
    assert not re.match(pattern, "s3a://bucket/files/2025/09/orders.parquet")
