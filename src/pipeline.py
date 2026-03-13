from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import pandas as pd

from .backtest import build_walkforward_windows, run_backtest_suite, run_walkforward_scoring
from .evaluation import evaluate_leader_selection, leader_metrics_to_frame
from .external_data import load_optional_market_cap_metadata, merge_histories_with_external
from .export import export_latest_ranking, export_latest_universe, export_live_pool
from .features import MODEL_FEATURE_COLUMNS, add_market_context_features, build_feature_panel
from .labels import build_labels
from .models import fit_predict_models
from .plots import save_equity_curve_plot, save_leader_metrics_plot
from .ranking import build_final_scores, latest_ranking_snapshot
from .regime import classify_regime
from .rules import compute_rule_scores
from .universe import build_dynamic_universe, resolve_universe_mode
from .utils import get_logger, load_local_histories


def prepare_research_panel(
    config: dict[str, Any],
    as_of_date: Optional[pd.Timestamp] = None,
    symbols: Optional[list[str]] = None,
    universe_mode: Optional[str] = None,
    purpose: str = "research",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load local raw data and build the full research panel."""
    logger = get_logger("prepare_research_panel")
    resolved_mode, _ = resolve_universe_mode(config, universe_mode=universe_mode, purpose=purpose)
    paths = config["paths"]
    metadata_path = Path(paths.cache_dir) / "symbol_metadata.csv"
    if not metadata_path.exists():
        raise FileNotFoundError(
            "Missing cached symbol metadata. Run scripts/download_history.py first."
        )
    metadata = pd.read_csv(metadata_path)
    start_date = config["data"]["start_date"]
    end_date = as_of_date or config["data"]["end_date"]
    histories = load_local_histories(paths.raw_dir, symbols=symbols, start_date=start_date, end_date=end_date)
    if not histories:
        raise FileNotFoundError("No local raw symbol histories were found. Run scripts/download_history.py first.")
    histories, external_merge_summary = merge_histories_with_external(histories, config, as_of_date=as_of_date)
    market_cap_metadata = load_optional_market_cap_metadata(config)
    if not external_merge_summary.empty:
        external_merge_summary.to_csv(paths.reports_dir / "external_data_quality_report.csv", index=False)
        extended = int((external_merge_summary["merged_rows"] > external_merge_summary["binance_rows"]).sum())
        applied = int(external_merge_summary["merge_applied"].fillna(False).astype(bool).sum()) if "merge_applied" in external_merge_summary.columns else extended
        logger.info(
            "External history merge summary: %s symbols extended; %s symbols passed the external quality gate.",
            extended,
            applied,
        )
    if not market_cap_metadata.empty:
        logger.info("Loaded optional external market-cap metadata with %s rows.", len(market_cap_metadata))

    benchmark_symbol = config["data"]["benchmark_symbol"]
    if benchmark_symbol not in histories:
        raise FileNotFoundError(
            f"The benchmark symbol {benchmark_symbol} is missing from data/raw. Download it before research."
        )

    logger.info("Building base feature panel from %s local symbols.", len(histories))
    panel = build_feature_panel(histories, benchmark_symbol, config, as_of_date=as_of_date)
    panel = build_dynamic_universe(
        panel,
        metadata,
        config,
        universe_mode=resolved_mode,
        purpose=purpose,
        market_cap_metadata=market_cap_metadata,
    )
    panel = add_market_context_features(panel, config["feature_engineering"]["breadth_min_names"])
    panel = build_labels(panel, config)
    panel = compute_rule_scores(panel, config)
    panel = classify_regime(panel, config)
    return panel.sort_index(), metadata


def run_research_pipeline(config: dict[str, Any], universe_mode: Optional[str] = None) -> dict[str, Any]:
    """End-to-end research workflow used by the research and validation scripts."""
    logger = get_logger("run_research_pipeline")
    resolved_mode, _ = resolve_universe_mode(config, universe_mode=universe_mode, purpose="research")
    logger.info("Running research pipeline with universe mode '%s'.", resolved_mode)
    panel, metadata = prepare_research_panel(config, universe_mode=resolved_mode, purpose="research")
    feature_columns = [column for column in MODEL_FEATURE_COLUMNS if column in panel.columns]
    panel, window_summary = run_walkforward_scoring(panel, feature_columns, config)
    panel = build_final_scores(panel, config)

    backtests = run_backtest_suite(panel, config)
    leader_metrics = leader_metrics_to_frame(evaluate_leader_selection(panel, "final_score", config))

    reports_dir = config["paths"].reports_dir
    plots_cfg = config["plots"]
    save_equity_curve_plot(backtests, reports_dir / "equity_curves.png", plots_cfg["style"])
    save_leader_metrics_plot(leader_metrics, reports_dir / "leader_metrics.png", plots_cfg["style"])

    summary_rows = []
    for name, result in backtests.items():
        row = {"strategy": name}
        row.update(result.metrics)
        summary_rows.append(row)
    performance_table = pd.DataFrame(summary_rows)
    performance_table.to_csv(reports_dir / "performance_summary.csv", index=False)
    window_summary.to_csv(reports_dir / "walkforward_windows.csv", index=False)
    leader_metrics.to_csv(reports_dir / "leader_metrics.csv", index=False)
    logger.info("Saved reports into %s.", reports_dir)

    return {
        "panel": panel,
        "metadata": metadata,
        "window_summary": window_summary,
        "backtests": backtests,
        "leader_metrics": leader_metrics,
        "performance_table": performance_table,
        "universe_mode": resolved_mode,
    }


def build_live_pool_outputs(
    config: dict[str, Any],
    as_of_date: Optional[pd.Timestamp] = None,
    universe_mode: Optional[str] = None,
) -> dict[str, Any]:
    """Train on the latest eligible history and export live universe/ranking files."""
    logger = get_logger("build_live_pool_outputs")
    resolved_mode, _ = resolve_universe_mode(config, universe_mode=universe_mode, purpose="live")
    logger.info("Building live pool with universe mode '%s'.", resolved_mode)
    panel, metadata = prepare_research_panel(
        config,
        as_of_date=as_of_date,
        universe_mode=resolved_mode,
        purpose="live",
    )
    available_dates = list(panel.index.get_level_values("date").unique().sort_values())
    if as_of_date is None:
        latest_date = available_dates[-1]
    else:
        requested_date = pd.Timestamp(as_of_date)
        eligible_dates = [date for date in available_dates if date <= requested_date]
        if not eligible_dates:
            raise ValueError(f"No local data is available on or before {requested_date.date()}.")
        latest_date = max(eligible_dates)

    horizons = [int(h) for h in config["labels"]["horizons"]]
    max_horizon = max(horizons)
    latest_position = available_dates.index(latest_date)
    train_end_position = max(0, latest_position - max_horizon)
    train_end_date = available_dates[train_end_position]
    train_start_position = max(0, train_end_position - int(config["walkforward"]["train_window_days"]) + 1)
    train_start_date = available_dates[train_start_position]

    feature_columns = [column for column in MODEL_FEATURE_COLUMNS if column in panel.columns]
    date_index = panel.index.get_level_values("date")
    train_mask = (
        (date_index >= train_start_date)
        & (date_index <= train_end_date)
        & panel["in_universe"]
        & panel["blended_target"].notna()
    )
    score_mask = (date_index == latest_date) & panel["in_universe"]
    result = fit_predict_models(panel.loc[train_mask], panel.loc[score_mask], feature_columns, config)

    if result.predictions.empty:
        panel.loc[score_mask, "linear_score_raw"] = pd.NA
        panel.loc[score_mask, "ml_score_raw"] = pd.NA
    else:
        panel = panel.join(result.predictions, how="left")
    panel = build_final_scores(panel, config)

    output_dir = config["paths"].output_dir
    export_latest_universe(panel, output_dir, latest_date)
    ranking_snapshot = export_latest_ranking(panel, output_dir, latest_date)
    latest_snapshot = latest_ranking_snapshot(panel, latest_date)
    live_payload = export_live_pool(
        ranking_snapshot=latest_snapshot.loc[latest_snapshot["selected_flag"] | latest_snapshot["in_universe"]],
        metadata=metadata,
        output_dir=output_dir,
        as_of_date=latest_date,
        pool_size=int(config["export"]["live_pool_size"]),
        save_legacy=bool(config["export"]["save_legacy_live_pool"]),
    )
    logger.info("Live pool exports saved into %s for %s.", output_dir, latest_date.date())

    return {
        "panel": panel,
        "metadata": metadata,
        "live_payload": live_payload,
        "as_of_date": latest_date,
        "train_start_date": train_start_date,
        "train_end_date": train_end_date,
        "linear_backend": result.linear_backend,
        "ml_backend": result.ml_backend,
        "universe_mode": resolved_mode,
    }
