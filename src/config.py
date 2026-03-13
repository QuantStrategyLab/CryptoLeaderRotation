from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, MutableMapping, Optional

try:
    import yaml
except ModuleNotFoundError as exc:  # pragma: no cover - import guard
    raise ModuleNotFoundError(
        "PyYAML is required to load config/default.yaml. Install dependencies from requirements.txt."
    ) from exc

from .utils import ensure_directory


@dataclass(frozen=True)
class ProjectPaths:
    project_root: Path
    raw_dir: Path
    cache_dir: Path
    processed_dir: Path
    models_dir: Path
    reports_dir: Path
    output_dir: Path


def _deep_merge(base: MutableMapping[str, Any], updates: Mapping[str, Any]) -> MutableMapping[str, Any]:
    for key, value in updates.items():
        if isinstance(value, Mapping) and isinstance(base.get(key), MutableMapping):
            _deep_merge(base[key], value)
        else:
            base[key] = value
    return base


def get_project_root() -> Path:
    """Resolve the repository root from the package location."""
    return Path(__file__).resolve().parent.parent


def build_project_paths(config: Mapping[str, Any], project_root: Optional[Path] = None) -> ProjectPaths:
    """Build absolute project paths from relative config entries."""
    root = project_root or get_project_root()
    data_cfg = config["data"]
    paths = ProjectPaths(
        project_root=root,
        raw_dir=ensure_directory(root / data_cfg["raw_dir"]),
        cache_dir=ensure_directory(root / data_cfg["cache_dir"]),
        processed_dir=ensure_directory(root / data_cfg["processed_dir"]),
        models_dir=ensure_directory(root / data_cfg["models_dir"]),
        reports_dir=ensure_directory(root / data_cfg["reports_dir"]),
        output_dir=ensure_directory(root / data_cfg["output_dir"]),
    )
    return paths


def load_config(
    path: Optional[str | Path] = None,
    overrides: Optional[Mapping[str, Any]] = None,
    project_root: Optional[Path] = None,
) -> dict[str, Any]:
    """Load YAML config, merge runtime overrides, and attach absolute paths."""
    root = project_root or get_project_root()
    config_path = Path(path) if path is not None else root / "config" / "default.yaml"
    with config_path.open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle) or {}
    merged = deepcopy(config)
    if overrides:
        _deep_merge(merged, overrides)
    merged["paths"] = build_project_paths(merged, root)
    return merged

