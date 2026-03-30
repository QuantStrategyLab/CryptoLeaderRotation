#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.compare_external_data import evaluate_variant, run_variant_scoring
from src.config import load_config
from src.external_data import merge_histories_with_external
from src.utils import get_logger, load_local_histories, read_json


DEFAULT_PROFILES: dict[str, dict[str, list[str]]] = {
    "current_core_plus_doge": {
        "core": ["BTCUSDT", "ETHUSDT", "XRPUSDT", "LTCUSDT", "BCHUSDT", "TRXUSDT", "ADAUSDT", "SOLUSDT"],
        "cautious": ["DOGEUSDT"],
    },
    "core_only_no_doge": {
        "core": ["BTCUSDT", "ETHUSDT", "XRPUSDT", "LTCUSDT", "BCHUSDT", "TRXUSDT", "ADAUSDT", "SOLUSDT"],
        "cautious": [],
    },
    "core_no_doge_no_bch": {
        "core": ["BTCUSDT", "ETHUSDT", "XRPUSDT", "LTCUSDT", "TRXUSDT", "ADAUSDT", "SOLUSDT"],
        "cautious": [],
    },
    "core_no_doge_no_sol": {
        "core": ["BTCUSDT", "ETHUSDT", "XRPUSDT", "LTCUSDT", "BCHUSDT", "TRXUSDT", "ADAUSDT"],
        "cautious": [],
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run or resume external-data profile sweep.")
    parser.add_argument("--config", default="config/default.yaml", help="Path to YAML config.")
    parser.add_argument(
        "--reports-root",
        default="data/reports/external_profile_sweep_venv",
        help="Directory used to store per-profile reports and sweep summary.",
    )
    parser.add_argument(
        "--outputs-root",
        default="data/output/external_profile_sweep_venv",
        help="Directory used to store per-profile live outputs.",
    )
    parser.add_argument(
        "--profiles",
        nargs="*",
        default=list(DEFAULT_PROFILES.keys()),
        help="Subset of profile names to run.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Recompute profiles even if per-profile artifacts already exist.",
    )
    return parser.parse_args()


def build_cfg(
    config_path: str,
    reports_root: str,
    outputs_root: str,
    variant: str,
    external_enabled: bool,
    core: list[str] | None = None,
    cautious: list[str] | None = None,
) -> dict[str, Any]:
    overrides = {
        "data": {
            "reports_dir": f"{reports_root}/{variant}",
            "output_dir": f"{outputs_root}/{variant}",
        },
        "external_data": {
            "enabled": external_enabled,
            "providers": {
                "cryptocompare_pre_binance": {"enabled": external_enabled},
                "exchange_archive_crosscheck": {"enabled": external_enabled},
            },
        },
    }
    if core is not None:
        overrides["external_data"]["core_backfill_whitelist"] = core
    if cautious is not None:
        overrides["external_data"]["cautious_backfill_whitelist"] = cautious
    return load_config(config_path, overrides=overrides)


def profile_complete(reports_root: Path, outputs_root: Path, profile: str) -> bool:
    required_paths = [
        reports_root / profile / "performance_summary.csv",
        reports_root / profile / "leader_metrics.csv",
        reports_root / profile / "walkforward_validation_summary.csv",
        reports_root / profile / "coverage.csv",
        outputs_root / profile / "live_pool.json",
    ]
    return all(path.exists() for path in required_paths)


def load_profile_row(
    profile: str,
    evaluation_start: str,
    coverage_path: Path,
    reports_dir: Path,
    outputs_dir: Path,
) -> dict[str, Any]:
    coverage = pd.read_csv(coverage_path)
    perf = pd.read_csv(reports_dir / "performance_summary.csv")
    leaders = pd.read_csv(reports_dir / "leader_metrics.csv")
    validation = pd.read_csv(reports_dir / "walkforward_validation_summary.csv")
    live_pool = read_json(outputs_dir / "live_pool.json", default={})

    final_perf = perf.loc[perf["strategy"].eq("final_score")].iloc[0].to_dict()
    horizon_map = {str(int(row["horizon"])): row for _, row in leaders.iterrows()}

    return {
        "profile": profile,
        "evaluation_start": evaluation_start,
        "approved_core": int(coverage["final_decision"].eq("approved_core").sum()),
        "approved_cautious": int(coverage["final_decision"].eq("approved_cautious").sum()),
        "rejected": int(coverage["final_decision"].eq("rejected").sum()),
        "research_cagr": float(final_perf["CAGR"]),
        "research_sharpe": float(final_perf["Sharpe"]),
        "research_max_drawdown": float(final_perf["Max Drawdown"]),
        "research_turnover": float(final_perf["Turnover"]),
        "h30_precision": float(horizon_map["30"]["Precision@N"]),
        "h30_capture": float(horizon_map["30"]["Leader Capture Rate"]),
        "h60_precision": float(horizon_map["60"]["Precision@N"]),
        "h60_capture": float(horizon_map["60"]["Leader Capture Rate"]),
        "h90_precision": float(horizon_map["90"]["Precision@N"]),
        "h90_capture": float(horizon_map["90"]["Leader Capture Rate"]),
        "wf_h30_precision": float(validation["h30_precision"].mean()),
        "wf_h30_capture": float(validation["h30_capture"].mean()),
        "wf_h60_precision": float(validation["h60_precision"].mean()),
        "wf_h60_capture": float(validation["h60_capture"].mean()),
        "wf_h90_precision": float(validation["h90_precision"].mean()),
        "wf_h90_capture": float(validation["h90_capture"].mean()),
        "wf_mean_sharpe": float(validation["window_sharpe"].mean()),
        "wf_mean_turnover": float(validation["window_turnover"].mean()),
        "live_pool": "|".join(live_pool.get("symbols", [])),
    }


def main() -> None:
    args = parse_args()
    logger = get_logger("sweep_external_data_profiles")

    selected_profiles = [name for name in args.profiles if name in DEFAULT_PROFILES]
    missing_profiles = sorted(set(args.profiles) - set(selected_profiles))
    if missing_profiles:
        raise ValueError(f"Unknown profile names: {', '.join(missing_profiles)}")

    reports_root = PROJECT_ROOT / args.reports_root
    outputs_root = PROJECT_ROOT / args.outputs_root
    reports_root.mkdir(parents=True, exist_ok=True)
    outputs_root.mkdir(parents=True, exist_ok=True)

    baseline_cfg = build_cfg(args.config, args.reports_root, args.outputs_root, "binance_only", external_enabled=False)
    logger.info("Running shared Binance-only baseline for the profile sweep.")
    scored_binance = run_variant_scoring(baseline_cfg)
    baseline_result = evaluate_variant(
        "binance_only",
        baseline_cfg,
        scored_binance,
        scored_binance["first_scored_date"],
    )
    baseline_perf = baseline_result["performance_table"].loc[
        baseline_result["performance_table"]["strategy"].eq("final_score")
    ].iloc[0]
    baseline_leaders = {
        str(int(row["horizon"])): row for _, row in baseline_result["leader_metrics"].iterrows()
    }
    baseline_validation = baseline_result["validation_table"]
    summary_rows = [
        {
            "profile": "binance_only",
            "evaluation_start": str(pd.Timestamp(scored_binance["first_scored_date"]).date()),
            "approved_core": 0,
            "approved_cautious": 0,
            "rejected": 0,
            "research_cagr": float(baseline_perf["CAGR"]),
            "research_sharpe": float(baseline_perf["Sharpe"]),
            "research_max_drawdown": float(baseline_perf["Max Drawdown"]),
            "research_turnover": float(baseline_perf["Turnover"]),
            "h30_precision": float(baseline_leaders["30"]["Precision@N"]),
            "h30_capture": float(baseline_leaders["30"]["Leader Capture Rate"]),
            "h60_precision": float(baseline_leaders["60"]["Precision@N"]),
            "h60_capture": float(baseline_leaders["60"]["Leader Capture Rate"]),
            "h90_precision": float(baseline_leaders["90"]["Precision@N"]),
            "h90_capture": float(baseline_leaders["90"]["Leader Capture Rate"]),
            "wf_h30_precision": float(baseline_validation["h30_precision"].mean()),
            "wf_h30_capture": float(baseline_validation["h30_capture"].mean()),
            "wf_h60_precision": float(baseline_validation["h60_precision"].mean()),
            "wf_h60_capture": float(baseline_validation["h60_capture"].mean()),
            "wf_h90_precision": float(baseline_validation["h90_precision"].mean()),
            "wf_h90_capture": float(baseline_validation["h90_capture"].mean()),
            "wf_mean_sharpe": float(baseline_validation["window_sharpe"].mean()),
            "wf_mean_turnover": float(baseline_validation["window_turnover"].mean()),
            "live_pool": "|".join(baseline_result["live_pool"].get("symbols", [])),
        }
    ]
    coverage_rows: list[dict[str, Any]] = []

    for idx, profile in enumerate(selected_profiles, start=1):
        logger.info("[%s/%s] Evaluating %s", idx, len(selected_profiles), profile)
        profile_cfg = build_cfg(
            args.config,
            args.reports_root,
            args.outputs_root,
            profile,
            external_enabled=True,
            core=DEFAULT_PROFILES[profile]["core"],
            cautious=DEFAULT_PROFILES[profile]["cautious"],
        )
        profile_reports_dir = reports_root / profile
        profile_outputs_dir = outputs_root / profile
        coverage_path = profile_reports_dir / "coverage.csv"

        if not args.force and profile_complete(reports_root, outputs_root, profile):
            logger.info("Reusing existing artifacts for %s", profile)
            evaluation_start = (
                pd.read_csv(profile_reports_dir / "performance_summary.csv").attrs.get("evaluation_start")
                or str(pd.Timestamp(scored_binance["first_scored_date"]).date())
            )
            summary_rows.append(
                load_profile_row(profile, evaluation_start, coverage_path, profile_reports_dir, profile_outputs_dir)
            )
            if coverage_path.exists():
                coverage = pd.read_csv(coverage_path)
                coverage_rows.extend({"profile": profile, **row} for row in coverage.to_dict("records"))
            continue

        representative_symbols = DEFAULT_PROFILES[profile]["core"] + DEFAULT_PROFILES[profile]["cautious"]
        histories = load_local_histories(
            profile_cfg["paths"].raw_dir,
            symbols=representative_symbols,
            start_date=profile_cfg["data"]["start_date"],
            end_date=profile_cfg["data"]["end_date"],
        )
        _, coverage = merge_histories_with_external(histories, profile_cfg)
        profile_reports_dir.mkdir(parents=True, exist_ok=True)
        coverage.to_csv(coverage_path, index=False)

        scored_external = run_variant_scoring(profile_cfg)
        evaluation_start = max(scored_binance["first_scored_date"], scored_external["first_scored_date"])
        evaluate_variant(profile, profile_cfg, scored_external, evaluation_start)
        summary_rows.append(
            load_profile_row(
                profile,
                str(pd.Timestamp(evaluation_start).date()),
                coverage_path,
                profile_reports_dir,
                profile_outputs_dir,
            )
        )
        coverage_rows.extend({"profile": profile, **row} for row in coverage.to_dict("records"))

    summary = pd.DataFrame(summary_rows)
    coverage_table = pd.DataFrame(coverage_rows)
    summary.to_csv(reports_root / "profile_summary.csv", index=False)
    if not coverage_table.empty:
        coverage_table.to_csv(reports_root / "profile_coverage.csv", index=False)
    logger.info("Profile sweep summary:\n%s", summary.to_string(index=False))


if __name__ == "__main__":
    main()
