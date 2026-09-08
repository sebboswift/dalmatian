from dalmatian.datasets import DatasetCache


def test_dataset_cache_reuses_loaded_value() -> None:
    cache: DatasetCache[str] = DatasetCache(max_entries=2, ttl_seconds=60)
    loads = 0
    released: list[str] = []

    def load() -> str:
        nonlocal loads
        loads += 1
        return "cached-view"

    first, first_hit = cache.get_or_load("dataset-v1", load, released.append)
    second, second_hit = cache.get_or_load("dataset-v1", load, released.append)

    assert first == second == "cached-view"
    assert first_hit is False
    assert second_hit is True
    assert loads == 1
    assert released == []


def test_dataset_cache_releases_lru_entry() -> None:
    cache: DatasetCache[str] = DatasetCache(max_entries=1, ttl_seconds=60)
    released: list[str] = []

    cache.get_or_load("one", lambda: "view-one", released.append)
    cache.get_or_load("two", lambda: "view-two", released.append)

    assert released == ["view-one"]
