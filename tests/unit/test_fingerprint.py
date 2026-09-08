from dalmatian.fingerprint import dataset_affinity_key, declared_source_versions, query_fingerprint
from dalmatian.models import DataFormat, QueryRequest, SourceSpec


def request(ttl: int, sql: str = "select * from orders") -> QueryRequest:
    return QueryRequest(
        sql=sql,
        sources={"orders": SourceSpec(path="orders/*.parquet", format=DataFormat.PARQUET)},
        cache_ttl_seconds=ttl,
    )


def test_fingerprint_ignores_cache_ttl() -> None:
    versions = {"orders": "v1"}
    assert query_fingerprint(request(10), versions) == query_fingerprint(request(60), versions)


def test_fingerprint_normalizes_formatting() -> None:
    versions = {"orders": "v1"}
    one = request(10, "select * from orders")
    two = request(10, "select   *\nfrom orders;")
    assert query_fingerprint(one, versions) == query_fingerprint(two, versions)


def test_fingerprint_changes_with_query() -> None:
    versions = {"orders": "v1"}
    assert query_fingerprint(request(10), versions) != query_fingerprint(
        request(10, "select count(*) from orders"), versions
    )


def test_fingerprint_changes_with_dataset_version() -> None:
    assert query_fingerprint(request(10), {"orders": "v1"}) != query_fingerprint(
        request(10), {"orders": "v2"}
    )


def test_declared_source_version_can_be_hashed_without_file_resolution() -> None:
    value = QueryRequest(
        sql="select * from orders",
        sources={
            "orders": SourceSpec(
                path="orders/**/*.parquet",
                format=DataFormat.PARQUET,
                version="snapshot-42",
            )
        },
    )
    versions = declared_source_versions(value)
    assert versions is not None
    assert set(versions) == {"orders"}


def test_affinity_is_source_based_not_sql_based() -> None:
    assert dataset_affinity_key(request(10, "select * from orders")) == dataset_affinity_key(
        request(10, "select count(*) from orders")
    )
