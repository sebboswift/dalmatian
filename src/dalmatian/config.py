from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DALMATIAN_",
        env_file=".env",
        env_ignore_empty=True,
    )

    spark_master: str = "local[*]"
    spark_app_name: str = "dalmatian"
    spark_packages: str = ""
    spark_config: dict[str, str] | None = None
    spark_app_max_cores: int = Field(default=2, ge=1)
    spark_executor_cores: int = Field(default=2, ge=1)
    spark_executor_memory: str = "2g"
    spark_executor_instances: int | None = Field(default=None, ge=1)

    valkey_url: str = "valkey://localhost:6379/0"
    data_root: Path = Path("./data")
    source_locations: dict[str, str] = Field(default_factory=dict)
    storage_locations: dict[str, str] = Field(
        default_factory=lambda: {"local": "file:///exports"}
    )
    output_publication_prefix: str = "dalmatian:outputs"
    metadata_url: str | None = None

    default_cache_ttl_seconds: int = Field(default=300, ge=0)
    max_cache_ttl_seconds: int = Field(default=3600, ge=0)
    result_row_limit: int = Field(default=10000, ge=1)
    result_cache_max_bytes: int = Field(default=4 * 1024 * 1024, ge=1024)
    result_cache_singleflight_wait_seconds: float = Field(default=15.0, ge=0.1)
    result_cache_lock_seconds: int = Field(default=120, ge=1)
    dataset_cache_entries: int = Field(default=32, ge=1)
    dataset_cache_ttl_seconds: int = Field(default=1800, ge=1)

    max_concurrent_queries: int = Field(default=4, ge=1)
    admission_wait_seconds: float = Field(default=0.25, ge=0)
    default_query_timeout_seconds: int = Field(default=300, ge=1)
    max_query_timeout_seconds: int = Field(default=3600, ge=1)
    max_inline_bytes: int = Field(default=8 * 1024 * 1024, ge=1024)

    queue_name: str = "dalmatian:jobs"
    job_ttl_seconds: int = Field(default=86400, ge=60)
    idempotency_ttl_seconds: int = Field(default=86400, ge=60)
    job_lease_seconds: int = Field(default=60, ge=10)
    worker_reserve_timeout_seconds: int = Field(default=2, ge=1, le=30)
    worker_reap_interval_seconds: int = Field(default=15, ge=1)
    worker_max_attempts: int = Field(default=3, ge=1)
    worker_retry_base_seconds: float = Field(default=2.0, ge=0.1)
    worker_retry_max_seconds: float = Field(default=60.0, ge=0.1)
    worker_affinity_shards: int = Field(default=8, ge=1, le=256)
    worker_affinity_shard: int | None = Field(default=None, ge=0)
    max_queue_depth: int = Field(default=10000, ge=1)

    worker_metrics_host: str = "0.0.0.0"
    worker_metrics_port: int = Field(default=9090, ge=1, le=65535)

    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
