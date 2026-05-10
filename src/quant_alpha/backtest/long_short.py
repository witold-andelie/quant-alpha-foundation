from __future__ import annotations

import numpy as np
import pandas as pd

from quant_alpha.config import BacktestConfig


def _daily_weights(panel: pd.DataFrame, alpha_col: str, cfg: BacktestConfig) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for dt, day in panel.dropna(subset=[alpha_col, "forward_return"]).groupby("date"):
        day = day.copy()
        top_cut = day[alpha_col].quantile(1 - cfg.top_quantile)
        bottom_cut = day[alpha_col].quantile(cfg.bottom_quantile)
        longs = day[day[alpha_col] >= top_cut].copy()
        shorts = day[day[alpha_col] <= bottom_cut].copy()
        if longs.empty or shorts.empty:
            continue
        longs["weight"] = 0.5 / len(longs)
        shorts["weight"] = -0.5 / len(shorts)
        rows.append(pd.concat([longs, shorts], ignore_index=True).assign(date=dt))

    if not rows:
        return pd.DataFrame(columns=["date", "symbol", "weight", "forward_return"])
    return pd.concat(rows, ignore_index=True)


def run_long_short_backtest(
    factor_panel: pd.DataFrame,
    cfg: BacktestConfig,
    alpha_col: str = "alpha_composite",
) -> tuple[pd.DataFrame, dict[str, float]]:
    required = {alpha_col, "forward_return", "date", "symbol"}
    missing = required - set(factor_panel.columns)
    if missing:
        raise ValueError(f"Missing required columns in panel: {sorted(missing)}")
    weights = _daily_weights(factor_panel, alpha_col, cfg)
    if weights.empty:
        empty = pd.DataFrame(
            columns=[
                "date",
                "gross_return",
                "transaction_cost",
                "portfolio_return",
                "equity_curve",
                "long_count",
                "short_count",
            ]
        )
        return empty, {}

    weights = weights.sort_values(["date", "symbol"])
    gross = (
        weights.assign(weighted_return=weights["weight"] * weights["forward_return"])
        .groupby("date", as_index=False)["weighted_return"]
        .sum()
        .rename(columns={"weighted_return": "gross_return"})
    )
    gross["gross_return"] = gross["gross_return"] / cfg.forward_return_days

    wide_weights = weights.pivot_table(index="date", columns="symbol", values="weight", fill_value=0)
    turnover = wide_weights.diff().abs().sum(axis=1).fillna(wide_weights.abs().sum(axis=1))
    cost = turnover * (cfg.transaction_cost_bps / 10_000)
    counts = weights.assign(side=np.where(weights["weight"] > 0, "long", "short")).pivot_table(
        index="date", columns="side", values="symbol", aggfunc="count", fill_value=0
    )

    daily = gross.set_index("date")
    daily["transaction_cost"] = cost.reindex(daily.index, fill_value=0)
    daily["portfolio_return"] = daily["gross_return"] - daily["transaction_cost"]
    daily["equity_curve"] = (1 + daily["portfolio_return"]).cumprod()
    daily["long_count"] = counts.get("long", pd.Series(dtype=float)).reindex(daily.index, fill_value=0)
    daily["short_count"] = counts.get("short", pd.Series(dtype=float)).reindex(daily.index, fill_value=0)
    daily = daily.reset_index()

    ret = daily["portfolio_return"]
    annualization = cfg.periods_per_year
    ann_vol = float(ret.std(ddof=0) * np.sqrt(annualization))
    ann_ret = float(ret.mean() * annualization)
    drawdown = daily["equity_curve"] / daily["equity_curve"].cummax() - 1
    max_dd = float(drawdown.min())

    downside = ret[ret < 0]
    downside_std = float(downside.std(ddof=0)) if len(downside) > 1 else 0.0
    sortino = float(ann_ret / (downside_std * np.sqrt(annualization))) if downside_std > 0 else 0.0
    calmar = float(ann_ret / abs(max_dd)) if abs(max_dd) > 1e-9 else 0.0

    metrics = {
        "total_return": float(daily["equity_curve"].iloc[-1] - 1),
        "annualized_return": ann_ret,
        "annualized_volatility": ann_vol,
        "sharpe": float(ann_ret / ann_vol) if ann_vol > 0 else 0.0,
        "sortino": sortino,
        "calmar": calmar,
        "max_drawdown": max_dd,
        "win_rate": float((ret > 0).mean()),
        "observations": float(len(daily)),
    }
    return daily, metrics
