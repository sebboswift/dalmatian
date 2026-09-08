from functools import lru_cache

from dalmatian.cache import ValkeyResultCache
from dalmatian.config import get_settings
from dalmatian.jobs import ValkeyJobQueue
from dalmatian.metadata import MetadataStore, build_metadata_store
from dalmatian.service import QueryService
from dalmatian.spark import SparkEngine
from dalmatian.storage import StorageRegistry, ValkeyOutputPublicationRegistry


@lru_cache
def get_metadata_store() -> MetadataStore:
    return build_metadata_store(get_settings().metadata_url)


@lru_cache
def get_output_publication_registry() -> ValkeyOutputPublicationRegistry:
    return ValkeyOutputPublicationRegistry(get_settings().valkey_url)


@lru_cache
def get_storage_registry() -> StorageRegistry:
    settings = get_settings()
    return StorageRegistry(
        settings.storage_locations,
        metadata=get_metadata_store(),
        source_locations=settings.source_locations,
        data_root=settings.data_root,
        publication_registry=get_output_publication_registry(),
        publication_prefix=settings.output_publication_prefix,
    )


@lru_cache
def get_query_service() -> QueryService:
    settings = get_settings()
    engine = SparkEngine(
        master=settings.spark_master,
        app_name=settings.spark_app_name,
        data_root=settings.data_root,
        storage=get_storage_registry(),
        dataset_cache_entries=settings.dataset_cache_entries,
        dataset_cache_ttl_seconds=settings.dataset_cache_ttl_seconds,
        max_inline_bytes=settings.max_inline_bytes,
        spark_packages=settings.spark_packages,
        spark_config=settings.spark_config or {},
        spark_app_max_cores=settings.spark_app_max_cores,
        spark_executor_cores=settings.spark_executor_cores,
        spark_executor_memory=settings.spark_executor_memory,
        spark_executor_instances=settings.spark_executor_instances,
    )
    return QueryService(
        engine=engine,
        cache=ValkeyResultCache(
            settings.valkey_url,
            max_bytes=settings.result_cache_max_bytes,
        ),
        default_cache_ttl_seconds=settings.default_cache_ttl_seconds,
        max_cache_ttl_seconds=settings.max_cache_ttl_seconds,
        result_row_limit=settings.result_row_limit,
        max_concurrent_queries=settings.max_concurrent_queries,
        admission_wait_seconds=settings.admission_wait_seconds,
        default_query_timeout_seconds=settings.default_query_timeout_seconds,
        max_query_timeout_seconds=settings.max_query_timeout_seconds,
        singleflight_wait_seconds=settings.result_cache_singleflight_wait_seconds,
        singleflight_lock_seconds=settings.result_cache_lock_seconds,
        metadata=get_metadata_store(),
    )


@lru_cache
def get_job_queue() -> ValkeyJobQueue:
    settings = get_settings()
    return ValkeyJobQueue(
        url=settings.valkey_url,
        queue_name=settings.queue_name,
        job_ttl_seconds=settings.job_ttl_seconds,
        lease_seconds=settings.job_lease_seconds,
        max_attempts=settings.worker_max_attempts,
        affinity_shards=settings.worker_affinity_shards,
        max_queue_depth=settings.max_queue_depth,
        idempotency_ttl_seconds=settings.idempotency_ttl_seconds,
        retry_base_seconds=settings.worker_retry_base_seconds,
        retry_max_seconds=settings.worker_retry_max_seconds,
        metadata=get_metadata_store(),
    )


def shutdown_dependencies() -> None:
    if get_query_service.cache_info().currsize:
        get_query_service().close()
        get_query_service.cache_clear()
    if get_job_queue.cache_info().currsize:
        get_job_queue().close()
        get_job_queue.cache_clear()
    if get_storage_registry.cache_info().currsize:
        get_storage_registry().close()
        get_storage_registry.cache_clear()
    get_output_publication_registry.cache_clear()
    if get_metadata_store.cache_info().currsize:
        get_metadata_store().close()
        get_metadata_store.cache_clear()
