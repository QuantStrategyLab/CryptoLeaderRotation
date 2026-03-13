#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.binance_client import BinanceSpotClient
from src.config import load_config
from src.universe import filter_metadata_candidates, resolve_universe_mode
from src.utils import get_logger


AUTO_DOWNLOAD_EXCLUDE_BASE_ASSETS = {
    "AEUR",
    "EUR",
    "FDUSD",
    "PAXG",
    "RLUSD",
    "TUSD",
    "USDC",
    "USDP",
    "USDT",
    "USD1",
    "XAUT",
    "XUSD",
}


def build_local_history_stats(raw_dir: Path, candidate_symbols: set[str]) -> pd.DataFrame:
    """Summarize existing local histories for download prioritization."""
    rows: list[dict[str, float | int | str | bool]] = []
    for file_path in sorted(raw_dir.glob("*.csv")):
        symbol = file_path.stem.upper()
        if symbol not in candidate_symbols:
            continue
        try:
            history = pd.read_csv(file_path, usecols=["date", "quote_volume"])
        except Exception:
            continue
        if history.empty:
            continue

        quote_volume = pd.to_numeric(history["quote_volume"], errors="coerce").dropna()
        if quote_volume.empty:
            continue

        tail_30 = quote_volume.tail(min(30, len(quote_volume)))
        tail_90 = quote_volume.tail(min(90, len(quote_volume)))
        tail_180 = quote_volume.tail(min(180, len(quote_volume)))
        max_liquidity = max(tail_30.mean(), tail_90.mean(), tail_180.mean())
        min_liquidity = min(tail_30.mean(), tail_90.mean(), tail_180.mean())
        liquidity_stability = min_liquidity / max_liquidity if max_liquidity > 0 else 0.0

        rows.append(
            {
                "symbol": symbol,
                "local_history_days": int(len(history)),
                "local_last_date": str(pd.to_datetime(history["date"]).max().date()),
                "avg_quote_vol_30_local": float(tail_30.mean()),
                "avg_quote_vol_90_local": float(tail_90.mean()),
                "avg_quote_vol_180_local": float(tail_180.mean()),
                "liquidity_stability_local": float(liquidity_stability),
            }
        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "symbol",
                "local_history_days",
                "local_last_date",
                "avg_quote_vol_30_local",
                "avg_quote_vol_90_local",
                "avg_quote_vol_180_local",
                "liquidity_stability_local",
            ]
        )
    return pd.DataFrame(rows)


def rank_download_candidates(
    client: BinanceSpotClient,
    metadata: pd.DataFrame,
    config: dict,
    logger,
) -> pd.DataFrame:
    """Rank symbols by research value rather than by alphabetical symbol order."""
    mode_name, mode_cfg = resolve_universe_mode(config, purpose="download")
    filtered = filter_metadata_candidates(metadata, config, universe_mode=mode_name, purpose="download").copy()
    eligible = filtered.loc[filtered["metadata_eligible"]].copy()
    if eligible.empty:
        return eligible

    eligible["base_asset"] = eligible["base_asset"].astype(str).str.upper()
    auto_exclude = set(AUTO_DOWNLOAD_EXCLUDE_BASE_ASSETS)
    auto_exclude.update({asset.upper() for asset in config["universe"].get("exclude_base_assets", [])})
    eligible = eligible.loc[
        ~eligible["base_asset"].isin(auto_exclude)
        & ~eligible["base_asset"].str.contains("USD", regex=False)
    ].copy()
    if eligible.empty:
        logger.warning("All metadata-eligible symbols were filtered out by auto-download exclusions.")
        return eligible

    candidate_symbols = set(eligible["symbol"].astype(str).str.upper())
    local_stats = build_local_history_stats(config["paths"].raw_dir, candidate_symbols)
    if not local_stats.empty:
        eligible = eligible.merge(local_stats, on="symbol", how="left")
    else:
        eligible["local_history_days"] = np.nan
        eligible["avg_quote_vol_30_local"] = np.nan
        eligible["avg_quote_vol_90_local"] = np.nan
        eligible["avg_quote_vol_180_local"] = np.nan
        eligible["liquidity_stability_local"] = np.nan

    try:
        ticker_stats = client.get_24h_ticker_stats()
        if not ticker_stats.empty:
            eligible = eligible.merge(ticker_stats, on="symbol", how="left")
    except Exception as exc:
        logger.warning("Unable to fetch 24h ticker stats; falling back to local history ranking only: %s", exc)
        eligible["quote_volume_24h"] = np.nan

    benchmark_symbol = config["data"]["benchmark_symbol"]
    min_history_days = int(mode_cfg["min_history_days"])
    eligible["local_history_days"] = pd.to_numeric(eligible["local_history_days"], errors="coerce").fillna(0).astype(int)
    eligible["avg_quote_vol_30_local"] = pd.to_numeric(eligible["avg_quote_vol_30_local"], errors="coerce").fillna(0.0)
    eligible["avg_quote_vol_90_local"] = pd.to_numeric(eligible["avg_quote_vol_90_local"], errors="coerce").fillna(0.0)
    eligible["avg_quote_vol_180_local"] = pd.to_numeric(eligible["avg_quote_vol_180_local"], errors="coerce").fillna(0.0)
    eligible["liquidity_stability_local"] = pd.to_numeric(
        eligible["liquidity_stability_local"], errors="coerce"
    ).fillna(0.0)
    eligible["quote_volume_24h"] = pd.to_numeric(eligible.get("quote_volume_24h"), errors="coerce").fillna(0.0)
    eligible["trade_count_24h"] = pd.to_numeric(eligible.get("trade_count_24h"), errors="coerce").fillna(0.0)

    eligible["history_priority"] = (eligible["local_history_days"] >= min_history_days).astype(int)
    eligible["has_local_history"] = (eligible["local_history_days"] > 0).astype(int)
    eligible["is_benchmark"] = eligible["symbol"].eq(benchmark_symbol).astype(int)

    ranked = eligible.sort_values(
        by=[
            "is_benchmark",
            "history_priority",
            "avg_quote_vol_180_local",
            "avg_quote_vol_90_local",
            "avg_quote_vol_30_local",
            "liquidity_stability_local",
            "quote_volume_24h",
            "trade_count_24h",
            "local_history_days",
            "symbol",
        ],
        ascending=[False, False, False, False, False, False, False, False, False, True],
    ).reset_index(drop=True)
    ranked["download_rank"] = np.arange(1, len(ranked) + 1)
    return ranked


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download or incrementally update Binance Spot daily history.")
    parser.add_argument("--config", default="config/default.yaml", help="Path to the YAML config file.")
    parser.add_argument("--start-date", default=None, help="Override the configured history start date.")
    parser.add_argument("--end-date", default=None, help="Optional inclusive end date.")
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=None,
        help="Optional explicit symbol list, e.g. BTCUSDT ETHUSDT SOLUSDT.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on the number of symbols downloaded after liquidity-aware ranking.",
    )
    parser.add_argument(
        "--top-liquid",
        type=int,
        default=None,
        help="Download the top N liquidity-ranked candidate symbols instead of the full eligible list.",
    )
    parser.add_argument(
        "--force-exchange-info",
        action="store_true",
        help="Ignore exchangeInfo cache and refresh metadata from Binance first.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    logger = get_logger("download_history")
    client = BinanceSpotClient(config, config["paths"])

    metadata = client.get_symbol_metadata(force_refresh=args.force_exchange_info)
    start_date = args.start_date or config["data"]["start_date"]
    end_date = args.end_date or config["data"]["end_date"]

    if args.symbols:
        symbols = sorted({symbol.upper() for symbol in args.symbols})
    else:
        ranked = rank_download_candidates(client, metadata, config, logger)
        logger.info(
            "Top ranked download candidates:\n%s",
            ranked[
                [
                    "download_rank",
                    "symbol",
                    "local_history_days",
                    "avg_quote_vol_180_local",
                    "avg_quote_vol_90_local",
                    "avg_quote_vol_30_local",
                    "liquidity_stability_local",
                    "quote_volume_24h",
                ]
            ]
            .head(15)
            .to_string(index=False),
        )
        symbols = ranked["symbol"].tolist()

    if args.top_liquid is not None:
        symbols = symbols[: args.top_liquid]
    elif args.limit is not None:
        symbols = symbols[: args.limit]

    logger.info("Preparing to download %s symbols from Binance Spot.", len(symbols))
    client.sync_history(symbols=symbols, start_date=start_date, end_date=end_date)


if __name__ == "__main__":
    main()
