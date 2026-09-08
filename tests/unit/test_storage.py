import pytest

from dalmatian.storage import StorageRegistry, UnknownStorageLocation


def test_named_location_resolves_under_configured_root() -> None:
    storage = StorageRegistry({"lake": "s3a://bucket/dalmatian"})

    result = storage.resolve("lake", "exports/run-1")

    assert result.uri == "s3a://bucket/dalmatian/exports/run-1"


def test_unknown_location_is_rejected() -> None:
    storage = StorageRegistry({"lake": "gs://bucket/dalmatian"})
    with pytest.raises(UnknownStorageLocation):
        storage.resolve("other", "run-1")


def test_output_path_cannot_escape_root() -> None:
    storage = StorageRegistry({"lake": "abfss://container@account.dfs.core.windows.net/dalmatian"})
    with pytest.raises(ValueError):
        storage.resolve("lake", "../other")


def test_s3_root_is_normalized_to_s3a() -> None:
    storage = StorageRegistry({"lake": "s3://bucket/dalmatian"})
    assert storage.resolve("lake", "run-1").uri == "s3a://bucket/dalmatian/run-1"


def test_unsupported_storage_scheme_is_rejected() -> None:
    with pytest.raises(ValueError):
        StorageRegistry({"bad": "https://example.com/results"})


def test_error_publication_is_atomic_for_logical_destination() -> None:
    from dalmatian.storage import MemoryOutputPublicationRegistry

    registry = MemoryOutputPublicationRegistry()
    storage = StorageRegistry(
        {"local": "file:///exports"},
        publication_registry=registry,
        publication_prefix="dalmatian:test:outputs",
    )
    destination = storage.resolve("local", "daily/orders")
    assert storage.publish_sync(destination, "query-1", "a1", "file:///m1", "error") is True
    assert storage.publish_sync(destination, "query-2", "a2", "file:///m2", "error") is False
    assert storage.publish_sync(destination, "query-2", "a2", "file:///m2", "overwrite") is True
    current = registry.current(storage.publication_key(destination))
    assert current is not None
    assert current["manifest_uri"] == "file:///m2"


def test_append_marks_destination_as_existing_without_moving_current_pointer() -> None:
    from dalmatian.storage import MemoryOutputPublicationRegistry

    registry = MemoryOutputPublicationRegistry()
    storage = StorageRegistry(
        {"local": "file:///exports"},
        publication_registry=registry,
        publication_prefix="dalmatian:test:outputs",
    )
    destination = storage.resolve("local", "daily/orders")
    assert storage.publish_sync(destination, "query-1", "a1", "file:///m1", "append") is True
    current = registry.current(storage.publication_key(destination))
    assert current is not None
    assert current["last_append_manifest_uri"] == "file:///m1"
    assert storage.publish_sync(destination, "query-2", "a2", "file:///m2", "error") is False
