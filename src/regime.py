from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def classify_regime(panel: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """Classify each date into a simple, explainable market regime."""
    panel = panel.copy()
    date_context = (
        panel.reset_index()
        .groupby("date")[
            [
                "btc_above_ma200",
                "btc_ma200_slope",
                "btc_zscore_120",
                "breadth_above_sma60",
                "breadth_above_sma200",
                "universe_momentum_dispersion",
                "universe_rs_dispersion",
                "single_leader_burst",
            ]
        ]
        .first()
        .sort_index()
    )

    regimes = []
    confidences = []
    for _, row in date_context.iterrows():
        btc_above = row.get("btc_above_ma200", 0.0) > 0.5
        breadth60 = row.get("breadth_above_sma60", np.nan)
        breadth200 = row.get("breadth_above_sma200", np.nan)
        btc_z = row.get("btc_zscore_120", 0.0)
        momentum_dispersion = row.get("universe_momentum_dispersion", 0.0)
        rs_dispersion = row.get("universe_rs_dispersion", 0.0)
        leader_burst = row.get("single_leader_burst", 0.0)

        if (not btc_above and pd.notna(breadth200) and breadth200 < 0.35) or (
            pd.notna(breadth60) and breadth60 < 0.25
        ):
            regime = "risk_off"
            confidence = min(1.0, max(0.0, 0.35 - (breadth200 if pd.notna(breadth200) else 0.0)) * 2.5)
        elif btc_above and pd.notna(breadth60) and breadth60 < 0.35 and btc_z > 0.0:
            regime = "btc_dominant"
            confidence = min(1.0, max(0.0, 0.35 - breadth60) * 2.5 + min(0.5, btc_z / 3.0))
        elif (
            btc_above
            and pd.notna(breadth60)
            and pd.notna(breadth200)
            and breadth60 > 0.55
            and breadth200 > 0.40
            and (pd.isna(leader_burst) or leader_burst < max(momentum_dispersion, 0.0))
        ):
            regime = "broad_alt_strength"
            confidence = min(1.0, max(0.0, breadth60 - 0.55) * 2.0 + max(0.0, breadth200 - 0.40) * 2.0)
        else:
            regime = "late_momentum"
            confidence = min(
                1.0,
                max(0.0, leader_burst if pd.notna(leader_burst) else 0.0)
                + max(0.0, rs_dispersion if pd.notna(rs_dispersion) else 0.0),
            )

        regimes.append(regime)
        confidences.append(confidence)

    regime_frame = pd.DataFrame(
        {
            "date": date_context.index,
            "regime": regimes,
            "regime_confidence": confidences,
        }
    ).set_index("date")
    return panel.join(regime_frame, on="date")


def get_regime_weights(regime_name: str, config: dict[str, Any]) -> dict[str, float]:
    """Fetch ensemble weights for a regime, with a default fallback."""
    return config["regime_weights"].get(regime_name, config["ensemble"]["default_weights"])
