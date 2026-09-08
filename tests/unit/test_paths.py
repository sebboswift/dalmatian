from pathlib import Path

import pytest

from dalmatian.paths import NoFilesMatched, PathOutsideDataRoot, resolve_paths


def test_recursive_glob_matches_nested_files(tmp_path: Path) -> None:
    nested = tmp_path / "files" / "2026" / "09"
    nested.mkdir(parents=True)
    target = nested / "orders.csv"
    target.write_text("id\n1\n")

    matches = resolve_paths("files/**/*.csv", tmp_path)

    assert matches == [str(target.resolve())]


def test_double_recursive_segments_match_nested_files(tmp_path: Path) -> None:
    nested = tmp_path / "files" / "year" / "month"
    nested.mkdir(parents=True)
    target = nested / "events.json"
    target.write_text('\n{"id": 1}\n')

    matches = resolve_paths("files/**/**", tmp_path)

    assert str(target.resolve()) in matches


def test_path_cannot_escape_data_root(tmp_path: Path) -> None:
    with pytest.raises(PathOutsideDataRoot):
        resolve_paths("../secret.csv", tmp_path)


def test_no_match_is_an_error(tmp_path: Path) -> None:
    with pytest.raises(NoFilesMatched):
        resolve_paths("files/**/*.json", tmp_path)
