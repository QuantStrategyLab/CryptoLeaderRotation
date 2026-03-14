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
from src.evaluation import evaluate_live_pool_shadow, summarize_live_pool_shadow
from src.pipeline import run_research_pipeline
from src.shadow import build_shadow_release_history
from src.utils import ensure_directory, get_logger


DEFAULT_PROFILES = (
    ("baseline_blended_rank", "blended_rank_pct"),
    ("challenger_rank_60", "future_rank_pct_60"),
    ("challenger_topk_60", "future_topk_label_60"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run small upstream challenger-target experiments.")
    parser.add_argument("--config", default="config/default.yaml", help="Path to the YAML config file.")
    parser.add_argument("--universe-mode", default=None, help="Optional research universe mode override.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger = get_logger("run_challenger_experiments")
    base_config = load_config(args.config)
    reports_dir = base_config["paths"].reports_dir
    releases_root = ensure_directory(base_config["paths"].output_dir / "challenger_shadow_releases")
    shadow_cfg = base_config.get("shadow_replay", {})

    rows = []
    for profile_name, target_mode in DEFAULT_PROFILES:
        config = load_config(args.config, overrides={"labels": {"target_mode": target_mode}})
        result = run_research_pipeline(config, universe_mode=args.universe_mode)
        performance = result["performance_table"].set_index("strategy")
        leader_metrics = result["leader_metrics"].set_index("horizon")
        shadow_detail = evaluate_live_pool_shadow(
            result["panel"],
            score_column="final_score",
            config=config,
            rebalance_frequency=str(config.get("release", {}).get("cadence", "monthly")),
            pool_size=int(config["export"]["live_pool_size"]),
        )
        shadow_summary = summarize_live_pool_shadow(shadow_detail).iloc[0].to_dict()
        shadow_dir = ensure_directory(releases_root / profile_name)
        release_index = build_shadow_release_history(
            panel=result["panel"],
            metadata=result["metadata"],
            config=config,
            output_dir=shadow_dir,
            cadence=str(shadow_cfg.get("cadence", "monthly")),
            activation_lag_days=int(shadow_cfg.get("activation_lag_days", 1)),
            selection_meta_fields=list(shadow_cfg.get("selection_meta_fields", [])),
        )

        rows.append(
            {
                "profile": profile_name,
                "target_mode": target_mode,
                "research_cagr": float(performance.loc["final_score", "CAGR"]),
                "research_sharpe": float(performance.loc["final_score", "Sharpe"]),
                "wf_h30_precision": float(leader_metrics.loc["30", "Precision@N"]),
                "wf_h60_precision": float(leader_metrics.loc["60", "Precision@N"]),
                "wf_h90_precision": float(leader_metrics.loc["90", "Precision@N"]),
                "wf_h30_capture": float(leader_metrics.loc["30", "Leader Capture Rate"]),
                "wf_h60_capture": float(leader_metrics.loc["60", "Leader Capture Rate"]),
                "wf_h90_capture": float(leader_metrics.loc["90", "Leader Capture Rate"]),
                "monthly_h30_precision": float(shadow_summary.get("h30_precision", float("nan"))),
                "monthly_h60_precision": float(shadow_summary.get("h60_precision", float("nan"))),
                "monthly_h90_precision": float(shadow_summary.get("h90_precision", float("nan"))),
                "monthly_h30_capture": float(shadow_summary.get("h30_leader_capture", float("nan"))),
                "monthly_h60_capture": float(shadow_summary.get("h60_leader_capture", float("nan"))),
                "monthly_h90_capture": float(shadow_summary.get("h90_leader_capture", float("nan"))),
                "release_count": int(len(release_index)),
                "release_index_path": str((shadow_dir / "release_index.csv").relative_to(base_config["paths"].project_root)),
            }
        )
        logger.info(
            "Profile %s | target=%s | research_sharpe=%.4f | wf_h60_precision=%.4f | monthly_h60_precision=%.4f",
            profile_name,
            target_mode,
            rows[-1]["research_sharpe"],
            rows[-1]["wf_h60_precision"],
            rows[-1]["monthly_h60_precision"],
        )

    summary = ensure_directory(reports_dir)
    summary_path = summary / "challenger_experiment_summary.csv"
    pd.DataFrame(rows).to_csv(summary_path, index=False)
    logger.info("Challenger experiment summary saved to %s", summary_path)


if __name__ == "__main__":
    main()
