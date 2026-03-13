from __future__ import annotations

import numpy as np
import pandas as pd


def sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window, min_periods=window).mean()


def rate_of_change(series: pd.Series, periods: int) -> pd.Series:
    return series.pct_change(periods)


def annualized_volatility(series: pd.Series, window: int, periods_per_year: int = 365) -> pd.Series:
    return series.rolling(window, min_periods=window).std() * np.sqrt(periods_per_year)


def downside_volatility(series: pd.Series, window: int, periods_per_year: int = 365) -> pd.Series:
    downside = series.where(series < 0.0, 0.0)
    return downside.rolling(window, min_periods=window).std() * np.sqrt(periods_per_year)


def true_range(high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
    previous_close = close.shift(1)
    components = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs(),
        ],
        axis=1,
    )
    return components.max(axis=1)


def atr(high: pd.Series, low: pd.Series, close: pd.Series, window: int = 14) -> pd.Series:
    return true_range(high, low, close).rolling(window, min_periods=window).mean()


def rolling_drawdown(series: pd.Series, window: int = 180) -> pd.Series:
    rolling_peak = series.rolling(window, min_periods=1).max()
    return series / rolling_peak - 1.0


def ulcer_index(series: pd.Series, window: int = 50) -> pd.Series:
    rolling_peak = series.rolling(window, min_periods=window).max()
    drawdown_pct = 100.0 * (series / rolling_peak - 1.0)
    return np.sqrt((drawdown_pct.pow(2)).rolling(window, min_periods=window).mean())


def rolling_zscore(series: pd.Series, window: int = 120) -> pd.Series:
    mean = series.rolling(window, min_periods=window).mean()
    std = series.rolling(window, min_periods=window).std()
    return (series - mean) / std.replace(0.0, np.nan)


def rolling_beta(asset_returns: pd.Series, benchmark_returns: pd.Series, window: int = 60) -> pd.Series:
    covariance = asset_returns.rolling(window, min_periods=window).cov(benchmark_returns)
    variance = benchmark_returns.rolling(window, min_periods=window).var()
    return covariance / variance.replace(0.0, np.nan)


def rolling_correlation(asset_returns: pd.Series, benchmark_returns: pd.Series, window: int = 60) -> pd.Series:
    return asset_returns.rolling(window, min_periods=window).corr(benchmark_returns)

