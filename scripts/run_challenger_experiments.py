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


SLICE_METRICS = [
    "pool_stability",
    "pool_churn",
    "h30_precision",
    "h60_precision",
    "h90_precision",
    "h30_leader_capture",
    "h60_leader_capture",
    "h90_leader_capture",
    "h30_pool_mean_future_return",
    "h60_pool_mean_future_return",
    "h90_pool_mean_future_return",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run small upstream challenger-target experiments.")
    parser.add_argument("--config", default="config/default.yaml", help="Path to the YAML config file.")
    parser.add_argument("--universe-mode", default=None, help="Optional research universe mode override.")
    return parser.parse_args()


def build_regime_lookup(panel: pd.DataFrame) -> pd.DataFrame:
    columns = ["date", "regime", "regime_confidence"]
    available = [column for column in columns if column == "date" or column in panel.columns]
    if "regime" not in available:
        return pd.DataFrame()
    lookup = (
        panel.reset_index()[available]
        .drop_duplicates(subset=["date"])
        .set_index("date")
        .sort_index()
    )
    return lookup


def summarize_shadow_slices(detail_table: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
    if detail_table.empty:
        return pd.DataFrame()

    agg_map = {"rebalance_date": "count"}
    agg_map.update({metric: "mean" for metric in SLICE_METRICS if metric in detail_table.columns})
    grouped = (
        detail_table.groupby(group_columns, dropna=False)
        .agg(agg_map)
        .reset_index()
        .rename(columns={"rebalance_date": "evaluation_dates"})
    )
    return grouped


def main() -> None:
    args = parse_args()
    logger = get_logger("run_challenger_experiments")
    base_config = load_config(args.config)
    reports_dir = base_config["paths"].reports_dir
    releases_root = ensure_directory(base_config["paths"].output_dir / "challenger_shadow_releases")
    shadow_cfg = base_config.get("shadow_replay", {})

    rows = []
    shadow_details = []
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
        regime_lookup = build_regime_lookup(result["panel"])
        if not shadow_detail.empty:
            shadow_detail = shadow_detail.copy()
            shadow_detail["profile"] = profile_name
            shadow_detail["target_mode"] = target_mode
            shadow_detail["rebalance_date"] = pd.to_datetime(shadow_detail["rebalance_date"]).dt.normalize()
            shadow_detail["rebalance_year"] = shadow_detail["rebalance_date"].dt.year
            if not regime_lookup.empty:
                shadow_detail = shadow_detail.merge(
                    regime_lookup,
                    left_on="rebalance_date",
                    right_index=True,
                    how="left",
                )
            shadow_details.append(shadow_detail)
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

    if shadow_details:
        combined_detail = pd.concat(shadow_details, ignore_index=True)
        detail_path = summary / "challenger_monthly_shadow_detail.csv"
        by_year_path = summary / "challenger_monthly_shadow_by_year.csv"
        by_regime_path = summary / "challenger_monthly_shadow_by_regime.csv"
        combined_detail.to_csv(detail_path, index=False)
        summarize_shadow_slices(
            combined_detail,
            ["profile", "target_mode", "rebalance_year"],
        ).to_csv(by_year_path, index=False)
        summarize_shadow_slices(
            combined_detail.loc[combined_detail["regime"].notna()].copy()
            if "regime" in combined_detail.columns
            else pd.DataFrame(),
            ["profile", "target_mode", "regime"],
        ).to_csv(by_regime_path, index=False)
        logger.info("Challenger monthly shadow detail saved to %s", detail_path)
        logger.info("Challenger yearly shadow slices saved to %s", by_year_path)
        logger.info("Challenger regime shadow slices saved to %s", by_regime_path)


if __name__ == "__main__":
    main()
