from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from .indicators import (
    annualized_volatility,
    atr,
    downside_volatility,
    rate_of_change,
    rolling_beta,
    rolling_correlation,
    rolling_drawdown,
    rolling_zscore,
    sma,
    ulcer_index,
)
from .utils import clean_numeric_frame, safe_divide


MODEL_FEATURE_COLUMNS = [
    "roc20",
    "roc60",
    "roc120",
    "rs20",
    "rs60",
    "rs120",
    "rs_combo",
    "rs_risk_adj",
    "price_vs_sma20",
    "price_vs_sma60",
    "price_vs_sma120",
    "price_vs_sma200",
    "trend_persist_90",
    "ma200_slope",
    "dist_to_90d_high",
    "dist_to_180d_high",
    "breakout_proximity",
    "vol20",
    "vol60",
    "momentum_combo",
    "risk_adjusted_momentum",
    "downside_volatility",
    "atr_ratio",
    "rolling_drawdown",
    "ulcer_index",
    "drawdown_severity",
    "quote_volume",
    "avg_quote_vol_30",
    "avg_quote_vol_90",
    "avg_quote_vol_180",
    "liquidity_stability",
    "age_days",
    "tradable_ratio_180",
    "rolling_beta_to_btc",
    "rolling_corr_to_btc",
    "recent_liquidity_acceleration",
    "btc_above_ma200",
    "btc_ma200_slope",
    "btc_zscore_120",
    "breadth_above_sma60",
    "breadth_above_sma200",
    "universe_momentum_dispersion",
    "universe_rs_dispersion",
    "single_leader_burst",
]


def build_symbol_feature_frame(
    symbol: str,
    history: pd.DataFrame,
    feature_config: dict[str, Any],
) -> pd.DataFrame:
    """Compute per-symbol features using only local symbol history."""
    frame = history.copy().sort_values("date").reset_index(drop=True)
    frame["symbol"] = symbol
    frame["daily_return"] = frame["close"].pct_change()
    frame["roc20"] = rate_of_change(frame["close"], 20)
    frame["roc60"] = rate_of_change(frame["close"], 60)
    frame["roc120"] = rate_of_change(frame["close"], 120)

    for window in (20, 60, 120, 200):
        frame[f"sma{window}"] = sma(frame["close"], window)
        frame[f"price_vs_sma{window}"] = safe_divide(frame["close"], frame[f"sma{window}"]) - 1.0

    frame["trend_persist_90"] = (
        (frame["close"] > frame["sma200"]).astype(float).rolling(90, min_periods=90).mean()
    )
    frame["ma200_slope"] = frame["sma200"].pct_change(20, fill_method=None)

    high_90 = frame["close"].rolling(90, min_periods=90).max()
    high_180 = frame["close"].rolling(180, min_periods=180).max()
    frame["dist_to_90d_high"] = safe_divide(frame["close"], high_90) - 1.0
    frame["dist_to_180d_high"] = safe_divide(frame["close"], high_180) - 1.0
    frame["breakout_proximity"] = safe_divide(frame["close"], high_90)

    frame["vol20"] = annualized_volatility(frame["daily_return"], 20)
    frame["vol60"] = annualized_volatility(frame["daily_return"], 60)
    frame["momentum_combo"] = 0.5 * frame["roc20"] + 0.3 * frame["roc60"] + 0.2 * frame["roc120"]
    frame["risk_adjusted_momentum"] = safe_divide(frame["momentum_combo"], frame["vol20"])

    downside_window = feature_config["downside_window"]
    atr_window = feature_config["atr_window"]
    ulcer_window = feature_config["ulcer_window"]
    drawdown_window = feature_config["drawdown_window"]

    frame["downside_volatility"] = downside_volatility(frame["daily_return"], downside_window)
    frame["atr14"] = atr(frame["high"], frame["low"], frame["close"], atr_window)
    frame["atr_ratio"] = safe_divide(frame["atr14"], frame["close"])
    frame["rolling_drawdown"] = rolling_drawdown(frame["close"], drawdown_window)
    frame["ulcer_index"] = ulcer_index(frame["close"], ulcer_window)
    frame["drawdown_severity"] = frame["rolling_drawdown"].abs() * (1.0 + frame["ulcer_index"].fillna(0.0) / 100.0)

    frame["quote_volume"] = frame["quote_volume"]
    frame["avg_quote_vol_30"] = frame["quote_volume"].rolling(30, min_periods=30).mean()
    frame["avg_quote_vol_90"] = frame["quote_volume"].rolling(90, min_periods=90).mean()
    frame["avg_quote_vol_180"] = frame["quote_volume"].rolling(180, min_periods=180).mean()
    frame["liquidity_stability"] = (
        frame[["avg_quote_vol_30", "avg_quote_vol_90", "avg_quote_vol_180"]].min(axis=1)
        / frame[["avg_quote_vol_30", "avg_quote_vol_90", "avg_quote_vol_180"]].max(axis=1)
    )
    frame["age_days"] = np.arange(1, len(frame) + 1)
    frame["tradable_flag"] = ((frame["quote_volume"] > 0.0) & frame["close"].notna()).astype(float)
    frame["tradable_ratio_180"] = frame["tradable_flag"].rolling(180, min_periods=30).mean()
    frame["recent_liquidity_acceleration"] = safe_divide(frame["avg_quote_vol_30"], frame["avg_quote_vol_180"]) - 1.0

    frame["downside_vol_penalty"] = -frame["downside_volatility"]
    frame["drawdown_penalty"] = -frame["drawdown_severity"]
    return clean_numeric_frame(frame)


def build_feature_panel(
    histories: dict[str, pd.DataFrame],
    benchmark_symbol: str,
    config: dict[str, Any],
    as_of_date: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Build a multi-index panel with per-symbol features and BTC-relative features."""
    feature_cfg = config["feature_engineering"]
    frames = []
    for symbol, history in histories.items():
        current = history.copy()
        if as_of_date is not None:
            current = current.loc[current["date"] <= as_of_date]
        if current.empty:
            continue
        frames.append(build_symbol_feature_frame(symbol, current, feature_cfg))

    if not frames:
        raise ValueError("No local histories were available to build a feature panel.")

    panel = pd.concat(frames, ignore_index=True).sort_values(["date", "symbol"])
    panel = panel.set_index(["date", "symbol"]).sort_index()
    panel = add_benchmark_relative_features(panel, benchmark_symbol, feature_cfg)
    return clean_numeric_frame(panel)


def add_benchmark_relative_features(
    panel: pd.DataFrame,
    benchmark_symbol: str,
    feature_config: dict[str, Any],
) -> pd.DataFrame:
    """Join benchmark context to all symbols and compute BTC-relative features."""
    if benchmark_symbol not in panel.index.get_level_values("symbol"):
        raise ValueError(f"Benchmark symbol {benchmark_symbol} is missing from the local data.")

    benchmark = panel.xs(benchmark_symbol, level="symbol").copy()
    benchmark_features = benchmark[
        ["daily_return", "roc20", "roc60", "roc120", "sma200", "ma200_slope", "close"]
    ].rename(
        columns={
            "daily_return": "btc_daily_return",
            "roc20": "btc_roc20",
            "roc60": "btc_roc60",
            "roc120": "btc_roc120",
            "sma200": "btc_sma200",
            "ma200_slope": "btc_ma200_slope",
            "close": "btc_close",
        }
    )
    benchmark_features["btc_above_ma200"] = (benchmark["close"] > benchmark["sma200"]).astype(float)
    benchmark_features["btc_zscore_120"] = rolling_zscore(benchmark["close"], 120)

    merged = panel.join(benchmark_features, on="date")
    merged["rs20"] = merged["roc20"] - merged["btc_roc20"]
    merged["rs60"] = merged["roc60"] - merged["btc_roc60"]
    merged["rs120"] = merged["roc120"] - merged["btc_roc120"]
    merged["rs_combo"] = 0.5 * merged["rs20"] + 0.3 * merged["rs60"] + 0.2 * merged["rs120"]
    merged["rs_risk_adj"] = safe_divide(merged["rs_combo"], merged["vol20"])

    beta_lookback = feature_config["beta_lookback"]
    corr_lookback = feature_config["correlation_lookback"]
    aligned_frames = []
    for symbol, symbol_frame in merged.groupby(level="symbol", group_keys=False):
        current = symbol_frame.copy()
        current["rolling_beta_to_btc"] = rolling_beta(
            current["daily_return"], current["btc_daily_return"], beta_lookback
        )
        current["rolling_corr_to_btc"] = rolling_correlation(
            current["daily_return"], current["btc_daily_return"], corr_lookback
        )
        if symbol == benchmark_symbol:
            current["rolling_beta_to_btc"] = 1.0
            current["rolling_corr_to_btc"] = 1.0
        aligned_frames.append(current)

    merged = pd.concat(aligned_frames).sort_index()
    return clean_numeric_frame(merged)


def add_market_context_features(
    panel: pd.DataFrame,
    min_names: int = 5,
) -> pd.DataFrame:
    """Compute universe-level breadth and dispersion features per date."""
    if "in_universe" not in panel.columns:
        raise ValueError("Panel must have an in_universe column before adding market context features.")

    universe_slice = panel.loc[panel["in_universe"]].copy()
    if universe_slice.empty:
        panel["breadth_above_sma60"] = np.nan
        panel["breadth_above_sma200"] = np.nan
        panel["universe_momentum_dispersion"] = np.nan
        panel["universe_rs_dispersion"] = np.nan
        panel["single_leader_burst"] = np.nan
        return panel

    def _date_summary(group: pd.DataFrame) -> pd.Series:
        valid_count = len(group)
        if valid_count < min_names:
            return pd.Series(
                {
                    "breadth_above_sma60": np.nan,
                    "breadth_above_sma200": np.nan,
                    "universe_momentum_dispersion": np.nan,
                    "universe_rs_dispersion": np.nan,
                    "single_leader_burst": np.nan,
                }
            )

        momentum = group["momentum_combo"].dropna()
        rs_combo = group["rs_combo"].dropna()
        leader_burst = np.nan
        if not momentum.empty:
            leader_burst = momentum.max() - momentum.median()
        return pd.Series(
            {
                "breadth_above_sma60": (group["price_vs_sma60"] > 0.0).mean(),
                "breadth_above_sma200": (group["price_vs_sma200"] > 0.0).mean(),
                "universe_momentum_dispersion": momentum.std() if len(momentum) > 1 else np.nan,
                "universe_rs_dispersion": rs_combo.std() if len(rs_combo) > 1 else np.nan,
                "single_leader_burst": leader_burst,
            }
        )

    market_context = universe_slice.groupby(level="date").apply(_date_summary)
    return panel.join(market_context, on="date")
