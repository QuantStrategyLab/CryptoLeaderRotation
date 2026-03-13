#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_config
from src.features import MODEL_FEATURE_COLUMNS
from src.models import fit_predict_models
from src.pipeline import prepare_research_panel
from src.ranking import build_final_scores, latest_ranking_snapshot
from src.utils import get_logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect one historical snapshot in detail.")
    parser.add_argument("date", help="Snapshot date, e.g. 2024-03-31")
    parser.add_argument("--config", default="config/default.yaml", help="Path to the YAML config file.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    logger = get_logger("debug_single_date_snapshot")
    snapshot_date = pd.Timestamp(args.date)

    panel, _ = prepare_research_panel(config, as_of_date=snapshot_date)
    dates = list(panel.index.get_level_values("date").unique().sort_values())
    if snapshot_date not in dates:
        raise ValueError(f"Requested date {snapshot_date.date()} is not available in local history.")

    horizons = [int(h) for h in config["labels"]["horizons"]]
    max_horizon = max(horizons)
    latest_position = dates.index(snapshot_date)
    train_end_position = max(0, latest_position - max_horizon)
    train_end_date = dates[train_end_position]
    train_start_position = max(0, train_end_position - int(config["walkforward"]["train_window_days"]) + 1)
    train_start_date = dates[train_start_position]

    feature_columns = [column for column in MODEL_FEATURE_COLUMNS if column in panel.columns]
    date_index = panel.index.get_level_values("date")
    train_mask = (
        (date_index >= train_start_date)
        & (date_index <= train_end_date)
        & panel["in_universe"]
        & panel["blended_target"].notna()
    )
    score_mask = (date_index == snapshot_date) & panel["in_universe"]
    result = fit_predict_models(panel.loc[train_mask], panel.loc[score_mask], feature_columns, config)
    panel = panel.join(result.predictions, how="left")
    panel = build_final_scores(panel, config)

    snapshot = latest_ranking_snapshot(panel, snapshot_date)
    output_path = config["paths"].output_dir / f"debug_snapshot_{snapshot_date.strftime('%Y%m%d')}.csv"
    snapshot.reset_index().to_csv(output_path, index=False)

    display_columns = [
        "final_score",
        "rule_score",
        "linear_score",
        "ml_score",
        "regime",
        "confidence",
        "selected_flag",
        "roc20",
        "roc60",
        "roc120",
        "rs_combo",
        "trend_persist_90",
        "avg_quote_vol_90",
    ]
    logger.info("Snapshot exported to %s", output_path)
    logger.info("Top rows:\n%s", snapshot[display_columns].head(20).to_string())


if __name__ == "__main__":
    main()
