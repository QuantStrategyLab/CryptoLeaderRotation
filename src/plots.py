from __future__ import annotations

from pathlib import Path
from typing import Mapping

import matplotlib.pyplot as plt
import pandas as pd

from .backtest import BacktestResult
from .utils import ensure_directory


def save_equity_curve_plot(
    results: Mapping[str, BacktestResult],
    output_path: Path,
    style: str = "seaborn-v0_8-darkgrid",
) -> None:
    """Save a comparison chart for strategy equity curves."""
    ensure_directory(output_path.parent)
    plt.style.use(style)
    fig, ax = plt.subplots(figsize=(12, 6))
    for name, result in results.items():
        if result.equity_curve.empty:
            continue
        ax.plot(result.equity_curve.index, result.equity_curve.values, label=name)
    ax.set_title("Leader Rotation Equity Curves")
    ax.set_ylabel("Equity")
    ax.set_xlabel("Date")
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def save_leader_metrics_plot(
    leader_metrics: pd.DataFrame,
    output_path: Path,
    style: str = "seaborn-v0_8-darkgrid",
) -> None:
    """Save a bar chart for leader-selection quality by horizon."""
    if leader_metrics.empty:
        return
    ensure_directory(output_path.parent)
    plt.style.use(style)
    fig, ax = plt.subplots(figsize=(10, 5))
    x = range(len(leader_metrics))
    ax.bar(x, leader_metrics["Leader Capture Rate"], width=0.4, label="Leader Capture Rate")
    ax.bar(
        [value + 0.4 for value in x],
        leader_metrics["Precision@N"],
        width=0.4,
        label="Precision@N",
    )
    ax.set_xticks([value + 0.2 for value in x])
    ax.set_xticklabels([str(h) for h in leader_metrics["horizon"]])
    ax.set_ylabel("Score")
    ax.set_xlabel("Horizon")
    ax.set_title("Out-of-Sample Leader Selection Quality")
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)

