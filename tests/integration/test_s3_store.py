import os
from pathlib import Path

import pytest

from dalmatian.execution import ExecutionContext
from dalmatian.models import OutputFormat, OutputSpec, QueryRequest, SourceSpec
from dalmatian.spark import SparkEngine
from dalmatian.storage import StorageRegistry

pytestmark = pytest.mark.integration


def test_minio_s3a_store_round_trip(tmp_path: Path) -> None:
    root = os.getenv("DALMATIAN_TEST_S3_ROOT")
    packages = os.getenv("DALMATIAN_TEST_SPARK_PACKAGES")
    if not root or not packages:
        pytest.skip("DALMATIAN_TEST_S3_ROOT and DALMATIAN_TEST_SPARK_PACKAGES are required")

    data = tmp_path / "data"
    data.mkdir()
    (data / "orders.csv").write_text("id,total\n1,10\n2,20\n")
    endpoint = os.getenv("DALMATIAN_TEST_S3_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("DALMATIAN_TEST_S3_ACCESS_KEY", "dalmatian")
    secret_key = os.getenv("DALMATIAN_TEST_S3_SECRET_KEY", "dalmatian-secret")
    spark_config = {
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.access.key": access_key,
        "spark.hadoop.fs.s3a.secret.key": secret_key,
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
    }
    engine = SparkEngine(
        master="local[2]",
        app_name="dalmatian-minio-test",
        data_root=data,
        storage=StorageRegistry(
            {"s3": root},
            source_locations={"s3": root},
            data_root=data,
        ),
        spark_packages=packages,
        spark_config=spark_config,
        spark_app_max_cores=2,
        spark_executor_cores=1,
        spark_executor_memory="1g",
    )
    try:
        request = QueryRequest(
            sql="select id, total from orders order by id",
            sources={
                "orders": SourceSpec.model_validate(
                    {
                        "path": "orders.csv",
                        "format": "csv",
                        "schema": {"id": "long", "total": "long"},
                    }
                )
            },
            output=OutputSpec(
                mode="store",
                format=OutputFormat.PARQUET,
                location="s3",
                path="integration/orders",
                write_mode="overwrite",
            ),
        )
        result = engine.execute(
            engine.prepare(request, request.sql),
            ExecutionContext(timeout_seconds=60),
        )
        _, fs, manifest_path = engine._hadoop_path(result.manifest_uri)
        assert fs.exists(manifest_path)
        _, fs, data_path = engine._hadoop_path(result.data_uri)
        assert fs.exists(data_path)

        remote_path = result.data_uri.removeprefix(f"{root.rstrip('/')}/")
        remote_request = QueryRequest(
            sql="select count(*) as rows, sum(total) as total from remote_orders",
            sources={
                "remote_orders": SourceSpec(
                    location="s3",
                    path=remote_path,
                    format="parquet",
                    version=result.query_id,
                )
            },
        )
        remote_result = engine.execute(
            engine.prepare(remote_request, remote_request.sql),
            ExecutionContext(timeout_seconds=60),
        )
        assert remote_result.rows == [{"rows": 2, "total": 30}]
    finally:
        engine.close()
