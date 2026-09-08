from __future__ import annotations

import glob
from pathlib import Path


class PathOutsideDataRoot(ValueError):
    pass


class NoFilesMatched(ValueError):
    pass


def _static_prefix(pattern: str) -> str:
    wildcard_positions = [pattern.find(char) for char in "*?[{" if char in pattern]
    if not wildcard_positions:
        return pattern
    return pattern[: min(position for position in wildcard_positions if position >= 0)]


def resolve_paths(pattern: str, data_root: Path) -> list[str]:
    root = data_root.resolve()
    candidate = Path(pattern)
    absolute_pattern = candidate if candidate.is_absolute() else root / candidate

    prefix = Path(_static_prefix(str(absolute_pattern)) or str(root)).resolve()
    if not prefix.is_relative_to(root):
        raise PathOutsideDataRoot(f"path must stay under {root}")

    has_glob = any(char in str(absolute_pattern) for char in "*?[{")
    if not has_glob:
        resolved = absolute_pattern.resolve()
        if not resolved.is_relative_to(root):
            raise PathOutsideDataRoot(f"path must stay under {root}")
        if not resolved.exists():
            raise NoFilesMatched(f"no file or directory found for {pattern}")
        return [str(resolved)]

    matches = [Path(path).resolve() for path in glob.glob(str(absolute_pattern), recursive=True)]
    files = sorted({path for path in matches if path.is_file() and path.is_relative_to(root)})
    if not files:
        raise NoFilesMatched(f"no files matched {pattern}")
    return [str(path) for path in files]
