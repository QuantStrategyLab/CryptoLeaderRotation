#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.backtest import run_backtest_suite, run_single_backtest, run_walkforward_scoring
from src.config import load_config
from src.evaluation import evaluate_leader_selection, leader_metrics_to_frame
from src.external_data import merge_histories_with_external
from src.features import MODEL_FEATURE_COLUMNS
from src.pipeline import build_live_pool_outputs, prepare_research_panel
from src.ranking import build_final_scores
from src.universe import latest_universe_snapshot
from src.utils import get_logger, load_local_histories, read_json, write_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a fair Binance-only vs external-data comparison and save summary artifacts."
    )
    parser.add_argument("--config", default="config/default.yaml", help="Path to the YAML config file.")
    return parser.parse_args()


def build_variant_config(config_path: str, variant_name: str, external_enabled: bool) -> dict[str, Any]:
    overrides = {
        "data": {
            "reports_dir": f"data/reports/{variant_name}",
            "output_dir": f"data/output/{variant_name}",
        },
        "external_data": {
            "enabled": external_enabled,
            "providers": {
                "cryptocompare_pre_binance": {
                    "enabled": external_enabled,
                },
                "exchange_archive_crosscheck": {
                    "enabled": external_enabled,
                }
            },
        },
    }
    return load_config(config_path, overrides=overrides)


def first_scored_date(panel: pd.DataFrame) -> pd.Timestamp:
    mask = panel["in_universe"] & panel["final_score"].notna()
    if not mask.any():
        raise ValueError("No scored dates were generated for the comparison panel.")
    return pd.Timestamp(panel.loc[mask].index.get_level_values("date").min()).normalize()


def build_validation_table(
    panel: pd.DataFrame,
    window_summary: pd.DataFrame,
    config: dict[str, Any],
    evaluation_start: pd.Timestamp,
) -> pd.DataFrame:
    rows = []
    for record in window_summary.to_dict("records"):
        test_start = pd.Timestamp(record["test_start"]).normalize()
        test_end = pd.Timestamp(record["test_end"]).normalize()
        if test_start < evaluation_start:
            continue
        date_index = panel.index.get_level_values("date")
        window_panel = panel.loc[(date_index >= test_start) & (date_index <= test_end)].copy()
        if window_panel.empty:
            continue

        leader_metrics = evaluate_leader_selection(
            window_panel,
            score_column="final_score",
            config=config,
            start_date=test_start,
            end_date=test_end,
        )
        backtest = run_single_backtest(window_panel, "final_score", config)
        rows.append(
            {
                **record,
                "h30_precision": leader_metrics["30"]["Precision@N"],
                "h30_recall": leader_metrics["30"]["Recall@N"],
                "h30_capture": leader_metrics["30"]["Leader Capture Rate"],
                "h60_precision": leader_metrics["60"]["Precision@N"],
                "h60_recall": leader_metrics["60"]["Recall@N"],
                "h60_capture": leader_metrics["60"]["Leader Capture Rate"],
                "h90_precision": leader_metrics["90"]["Precision@N"],
                "h90_recall": leader_metrics["90"]["Recall@N"],
                "h90_capture": leader_metrics["90"]["Leader Capture Rate"],
                "window_cagr": backtest.metrics["CAGR"],
                "window_sharpe": backtest.metrics["Sharpe"],
                "window_max_drawdown": backtest.metrics["Max Drawdown"],
                "window_turnover": backtest.metrics["Turnover"],
            }
        )
    return pd.DataFrame(rows)


def summarize_walkforward(validation_table: pd.DataFrame) -> dict[str, float]:
    if validation_table.empty:
        return {
            "H30 Precision": float("nan"),
            "H30 Recall": float("nan"),
            "H30 Leader Capture": float("nan"),
            "H60 Precision": float("nan"),
            "H60 Recall": float("nan"),
            "H60 Leader Capture": float("nan"),
            "H90 Precision": float("nan"),
            "H90 Recall": float("nan"),
            "H90 Leader Capture": float("nan"),
            "Mean Window Sharpe": float("nan"),
            "Mean Window Turnover": float("nan"),
        }
    return {
        "H30 Precision": float(validation_table["h30_precision"].mean()),
        "H30 Recall": float(validation_table["h30_recall"].mean()),
        "H30 Leader Capture": float(validation_table["h30_capture"].mean()),
        "H60 Precision": float(validation_table["h60_precision"].mean()),
        "H60 Recall": float(validation_table["h60_recall"].mean()),
        "H60 Leader Capture": float(validation_table["h60_capture"].mean()),
        "H90 Precision": float(validation_table["h90_precision"].mean()),
        "H90 Recall": float(validation_table["h90_recall"].mean()),
        "H90 Leader Capture": float(validation_table["h90_capture"].mean()),
        "Mean Window Sharpe": float(validation_table["window_sharpe"].mean()),
        "Mean Window Turnover": float(validation_table["window_turnover"].mean()),
    }


def build_performance_table(backtests: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for name, result in backtests.items():
        row = {"strategy": name}
        row.update(result.metrics)
        rows.append(row)
    return pd.DataFrame(rows)


def run_variant_scoring(config: dict[str, Any]) -> dict[str, Any]:
    panel, metadata = prepare_research_panel(config, universe_mode="broad_liquid", purpose="research")
    feature_columns = [column for column in MODEL_FEATURE_COLUMNS if column in panel.columns]
    panel, window_summary = run_walkforward_scoring(panel, feature_columns, config)
    panel = build_final_scores(panel, config)
    broad_latest_date = pd.Timestamp(panel.index.get_level_values("date").max()).normalize()
    return {
        "panel": panel,
        "metadata": metadata,
        "window_summary": window_summary,
        "first_scored_date": first_scored_date(panel),
        "broad_latest_date": broad_latest_date,
        "broad_latest_universe": latest_universe_snapshot(panel, broad_latest_date),
    }


def evaluate_variant(
    variant_name: str,
    config: dict[str, Any],
    scored: dict[str, Any],
    evaluation_start: pd.Timestamp,
) -> dict[str, Any]:
    reports_dir = config["paths"].reports_dir
    output_dir = config["paths"].output_dir
    panel = scored["panel"]
    date_index = panel.index.get_level_values("date")
    evaluation_panel = panel.loc[date_index >= evaluation_start].copy()

    backtests = run_backtest_suite(evaluation_panel, config)
    performance_table = build_performance_table(backtests)
    leader_metrics = leader_metrics_to_frame(
        evaluate_leader_selection(evaluation_panel, "final_score", config, start_date=evaluation_start)
    )
    validation_table = build_validation_table(panel, scored["window_summary"], config, evaluation_start)
    walkforward_summary = summarize_walkforward(validation_table)

    performance_table.to_csv(reports_dir / "performance_summary.csv", index=False)
    leader_metrics.to_csv(reports_dir / "leader_metrics.csv", index=False)
    validation_table.to_csv(reports_dir / "walkforward_validation_summary.csv", index=False)
    scored["window_summary"].to_csv(reports_dir / "walkforward_windows.csv", index=False)

    live_result = build_live_pool_outputs(config, universe_mode="core_major")
    latest_universe = read_json(output_dir / "latest_universe.json", default={})
    latest_ranking = pd.read_csv(output_dir / "latest_ranking.csv")
    live_pool = read_json(output_dir / "live_pool.json", default={})

    return {
        "variant": variant_name,
        "evaluation_start": evaluation_start,
        "performance_table": performance_table,
        "leader_metrics": leader_metrics,
        "validation_table": validation_table,
        "walkforward_summary": walkforward_summary,
        "broad_latest_universe": scored["broad_latest_universe"],
        "broad_latest_date": scored["broad_latest_date"],
        "core_latest_universe": latest_universe.get("symbols", []),
        "core_latest_date": latest_universe.get("as_of_date"),
        "latest_ranking": latest_ranking,
        "live_pool": live_pool,
        "live_result": live_result,
    }


def build_symbol_coverage_report(
    config_path: str,
    representative_symbols: list[str],
) -> pd.DataFrame:
    base_config = build_variant_config(config_path, "binance_only_coverage", external_enabled=False)
    external_config = build_variant_config(config_path, "external_data_coverage", external_enabled=True)
    histories = load_local_histories(
        base_config["paths"].raw_dir,
        symbols=representative_symbols,
        start_date=base_config["data"]["start_date"],
        end_date=base_config["data"]["end_date"],
    )
    merged_histories, merge_summary = merge_histories_with_external(histories, external_config)

    rows = []
    for symbol in representative_symbols:
        binance_history = histories.get(symbol, pd.DataFrame())
        merged_history = merged_histories.get(symbol, pd.DataFrame())
        if binance_history.empty:
            continue
        binance_dates = pd.to_datetime(binance_history["date"]).sort_values().reset_index(drop=True)
        merged_dates = pd.to_datetime(merged_history["date"]).sort_values().reset_index(drop=True) if not merged_history.empty else binance_dates
        earliest_binance = pd.Timestamp(binance_dates.min()).normalize()
        earliest_merged = pd.Timestamp(merged_dates.min()).normalize()
        base_row = {
            "symbol": symbol,
            "binance_only_start": earliest_binance.date().isoformat(),
            "external_merged_start": earliest_merged.date().isoformat(),
            "extended_into_pre_binance": bool(earliest_merged < earliest_binance),
        }
        if not merge_summary.empty and symbol in set(merge_summary["symbol"]):
            base_row.update(merge_summary.loc[merge_summary["symbol"] == symbol].iloc[0].to_dict())
        source_counts = merged_history["data_source"].value_counts().to_dict() if not merged_history.empty else {"binance": len(binance_history)}
        base_row["source_counts"] = json.dumps(source_counts, ensure_ascii=False, sort_keys=True)
        rows.append(base_row)
    return pd.DataFrame(rows)


def build_summary_table(
    binance_result: dict[str, Any],
    external_result: dict[str, Any],
    coverage_table: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for metric in ["CAGR", "Sharpe", "Max Drawdown", "Turnover"]:
        binance_value = float(
            binance_result["performance_table"].loc[
                binance_result["performance_table"]["strategy"] == "final_score", metric
            ].iloc[0]
        )
        external_value = float(
            external_result["performance_table"].loc[
                external_result["performance_table"]["strategy"] == "final_score", metric
            ].iloc[0]
        )
        rows.append(
            {
                "section": "research_final_score",
                "item": metric,
                "binance_only": binance_value,
                "external_data": external_value,
                "delta_external_minus_binance": external_value - binance_value,
            }
        )

    for horizon in ["30", "60", "90"]:
        for metric in ["Precision@N", "Recall@N", "Leader Capture Rate"]:
            binance_value = float(
                binance_result["leader_metrics"].loc[
                    binance_result["leader_metrics"]["horizon"].astype(str) == horizon, metric
                ].iloc[0]
            )
            external_value = float(
                external_result["leader_metrics"].loc[
                    external_result["leader_metrics"]["horizon"].astype(str) == horizon, metric
                ].iloc[0]
            )
            rows.append(
                {
                    "section": "leader_metrics",
                    "item": f"H{horizon} {metric}",
                    "binance_only": binance_value,
                    "external_data": external_value,
                    "delta_external_minus_binance": external_value - binance_value,
                }
            )

    for metric in [
        "H30 Precision",
        "H30 Recall",
        "H30 Leader Capture",
        "H60 Precision",
        "H60 Recall",
        "H60 Leader Capture",
        "H90 Precision",
        "H90 Recall",
        "H90 Leader Capture",
        "Mean Window Sharpe",
        "Mean Window Turnover",
    ]:
        binance_value = float(binance_result["walkforward_summary"][metric])
        external_value = float(external_result["walkforward_summary"][metric])
        rows.append(
            {
                "section": "walkforward_summary",
                "item": metric,
                "binance_only": binance_value,
                "external_data": external_value,
                "delta_external_minus_binance": external_value - binance_value,
            }
        )

    rows.extend(
        [
            {
                "section": "latest_universe",
                "item": "broad_liquid_count",
                "binance_only": len(binance_result["broad_latest_universe"]),
                "external_data": len(external_result["broad_latest_universe"]),
                "delta_external_minus_binance": len(external_result["broad_latest_universe"])
                - len(binance_result["broad_latest_universe"]),
            },
            {
                "section": "latest_universe",
                "item": "core_major_count",
                "binance_only": len(binance_result["core_latest_universe"]),
                "external_data": len(external_result["core_latest_universe"]),
                "delta_external_minus_binance": len(external_result["core_latest_universe"])
                - len(binance_result["core_latest_universe"]),
            },
            {
                "section": "live_pool",
                "item": "symbols",
                "binance_only": "|".join(binance_result["live_pool"].get("symbols", [])),
                "external_data": "|".join(external_result["live_pool"].get("symbols", [])),
                "delta_external_minus_binance": "",
            },
            {
                "section": "external_quality",
                "item": "approved_core_symbols",
                "binance_only": 0,
                "external_data": int(coverage_table["final_decision"].eq("approved_core").sum()),
                "delta_external_minus_binance": int(coverage_table["final_decision"].eq("approved_core").sum()),
            },
            {
                "section": "external_quality",
                "item": "approved_cautious_symbols",
                "binance_only": 0,
                "external_data": int(coverage_table["final_decision"].eq("approved_cautious").sum()),
                "delta_external_minus_binance": int(coverage_table["final_decision"].eq("approved_cautious").sum()),
            },
            {
                "section": "external_quality",
                "item": "cautious_holdout_symbols",
                "binance_only": 0,
                "external_data": int(coverage_table["final_decision"].eq("cautious_holdout").sum()),
                "delta_external_minus_binance": int(coverage_table["final_decision"].eq("cautious_holdout").sum()),
            },
            {
                "section": "external_quality",
                "item": "rejected_symbols",
                "binance_only": 0,
                "external_data": int(coverage_table["final_decision"].eq("rejected").sum()),
                "delta_external_minus_binance": int(coverage_table["final_decision"].eq("rejected").sum()),
            },
        ]
    )

    for row in coverage_table.to_dict("records"):
        rows.append(
            {
                "section": "data_coverage",
                "item": row["symbol"],
                "binance_only": row["binance_rows"],
                "external_data": row["merged_rows"],
                "delta_external_minus_binance": row["merged_rows"] - row["binance_rows"],
            }
        )

    return pd.DataFrame(rows)


def main() -> None:
    args = parse_args()
    logger = get_logger("compare_external_data")

    baseline_config = build_variant_config(args.config, "binance_only", external_enabled=False)
    external_config = build_variant_config(args.config, "external_data", external_enabled=True)
    representative_symbols = list(
        external_config["external_data"]["providers"]["cryptocompare_pre_binance"].get("symbols", [])
    )

    coverage_table = build_symbol_coverage_report(args.config, representative_symbols)
    scored_binance = run_variant_scoring(baseline_config)
    scored_external = run_variant_scoring(external_config)
    common_evaluation_start = max(scored_binance["first_scored_date"], scored_external["first_scored_date"])

    logger.info("Fair comparison evaluation start date: %s", common_evaluation_start.date())
    binance_result = evaluate_variant("binance_only", baseline_config, scored_binance, common_evaluation_start)
    external_result = evaluate_variant("external_data", external_config, scored_external, common_evaluation_start)

    summary_table = build_summary_table(binance_result, external_result, coverage_table)
    root_reports_dir = baseline_config["paths"].project_root / "data" / "reports"
    coverage_table.to_csv(root_reports_dir / "external_data_symbol_coverage.csv", index=False)
    coverage_table.to_csv(root_reports_dir / "external_data_quality_report.csv", index=False)
    summary_table.to_csv(root_reports_dir / "binance_only_vs_external_data_summary.csv", index=False)

    comparison_manifest = {
        "evaluation_start": common_evaluation_start.date().isoformat(),
        "binance_only_reports_dir": str(baseline_config["paths"].reports_dir),
        "external_data_reports_dir": str(external_config["paths"].reports_dir),
        "binance_only_output_dir": str(baseline_config["paths"].output_dir),
        "external_data_output_dir": str(external_config["paths"].output_dir),
        "representative_symbols": representative_symbols,
    }
    write_json(root_reports_dir / "external_data_comparison_manifest.json", comparison_manifest)

    logger.info("Saved external-data comparison summary to %s", root_reports_dir)
    logger.info("Representative symbol coverage:\n%s", coverage_table.to_string(index=False))
    logger.info("Comparison summary head:\n%s", summary_table.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
