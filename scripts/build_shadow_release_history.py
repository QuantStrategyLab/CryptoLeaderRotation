#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_config
from src.pipeline import run_research_pipeline
from src.shadow import build_shadow_release_history, summarize_shadow_release_history
from src.utils import ensure_directory, get_logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a local monthly shadow release history for downstream end-to-end replay."
    )
    parser.add_argument("--config", default="config/default.yaml", help="Path to the YAML config file.")
    parser.add_argument("--universe-mode", default=None, help="Optional research universe mode override.")
    parser.add_argument("--target-mode", default=None, help="Optional label target override, e.g. future_rank_pct_60.")
    parser.add_argument("--output-subdir", default="shadow_releases", help="Subdirectory under data/output/ for the shadow release history.")
    parser.add_argument("--activation-lag-days", type=int, default=None, help="Override the activation lag in trading days.")
    parser.add_argument(
        "--include-selection-meta",
        action="store_true",
        help="Include additive per-symbol selection metadata such as final_score/confidence.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    overrides = {}
    if args.target_mode:
        overrides = {"labels": {"target_mode": args.target_mode}}

    config = load_config(args.config, overrides=overrides)
    logger = get_logger("build_shadow_release_history")
    result = run_research_pipeline(config, universe_mode=args.universe_mode)

    shadow_cfg = config.get("shadow_replay", {})
    output_dir = ensure_directory(config["paths"].output_dir / args.output_subdir)
    activation_lag_days = (
        int(args.activation_lag_days)
        if args.activation_lag_days is not None
        else int(shadow_cfg.get("activation_lag_days", 1))
    )
    include_selection_meta = bool(args.include_selection_meta or shadow_cfg.get("include_selection_meta", False))
    selection_meta_fields = (
        list(shadow_cfg.get("selection_meta_fields", []))
        if include_selection_meta
        else None
    )

    index_table = build_shadow_release_history(
        panel=result["panel"],
        metadata=result["metadata"],
        config=config,
        output_dir=output_dir,
        cadence=str(shadow_cfg.get("cadence", "monthly")),
        activation_lag_days=activation_lag_days,
        selection_meta_fields=selection_meta_fields,
    )
    summary = summarize_shadow_release_history(index_table)
    summary_path = output_dir / "release_summary.csv"
    summary.to_csv(summary_path, index=False)

    logger.info("Shadow release history saved to %s", output_dir)
    logger.info("Release count: %s", int(summary.iloc[0]["release_count"]) if not summary.empty else 0)
    if not index_table.empty:
        logger.info("Head:\n%s", index_table.head().to_string(index=False))
    logger.info("Summary saved to %s", summary_path)


if __name__ == "__main__":
    main()
