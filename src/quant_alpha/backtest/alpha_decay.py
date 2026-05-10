from __future__ import annotations

import numpy as np
import pandas as pd

from quant_alpha.config import BacktestConfig


_DECAY_HORIZONS = [1, 3, 5, 10, 22, 44]


def _ic_at_horizon(
    panel: pd.DataFrame,
    alpha_col: str,
    horizon: int,
    date_col: str = "date",
    symbol_col: str = "symbol",
    price_col: str = "adj_close",
) -> float:
    if price_col not in panel.columns:
        return np.nan
    df = panel[[date_col, symbol_col, alpha_col, price_col]].dropna(subset=[alpha_col]).copy()
    df = df.sort_values([symbol_col, date_col])
    df["fwd_ret"] = df.groupby(symbol_col)[price_col].transform(
        lambda s: (s.shift(-horizon) / s.replace(0, np.nan)) - 1
    )
    clean = df.dropna(subset=[alpha_col, "fwd_ret"])
    if len(clean) < 10:
        return np.nan
    return float(clean[alpha_col].rank().corr(clean["fwd_ret"].rank()))


def compute_alpha_decay(
    panel: pd.DataFrame,
    alpha_cols: list[str],
    horizons: list[int] | None = None,
    date_col: str = "date",
    symbol_col: str = "symbol",
    price_col: str = "adj_close",
) -> pd.DataFrame:
    """Return IC by forward horizon for each alpha — the decay curve."""
    horizons = horizons or _DECAY_HORIZONS
    rows: list[dict[str, object]] = []
    for alpha_col in alpha_cols:
        for h in horizons:
            ic = _ic_at_horizon(panel, alpha_col, h, date_col, symbol_col, price_col)
            rows.append({"alpha_name": alpha_col, "horizon_days": h, "ic": ic})
    return pd.DataFrame(rows)


def compute_energy_alpha_decay(
    panel: pd.DataFrame,
    alpha_cols: list[str],
    horizons: list[int] | None = None,
) -> pd.DataFrame:
    """Decay curve for energy factors using power_market_features panel layout."""
    horizons = horizons or [1, 3, 6, 12, 24, 48]
    rows: list[dict[str, object]] = []
    for alpha_col in alpha_cols:
        for h in horizons:
            df = panel[["timestamp", "market", alpha_col, "spot_price"]].dropna(subset=[alpha_col]).copy()
            df = df.sort_values(["market", "timestamp"])
            df["fwd_ret"] = df.groupby("market")["spot_price"].transform(
                lambda s: (s.shift(-h) - s) / s.abs().clip(lower=20.0)
            )
            clean = df.dropna(subset=[alpha_col, "fwd_ret"])
            ic = float(clean[alpha_col].rank().corr(clean["fwd_ret"].rank())) if len(clean) >= 10 else np.nan
            rows.append({"alpha_name": alpha_col, "horizon_hours": h, "ic": ic})
    return pd.DataFrame(rows)


def walk_forward_ic(
    panel: pd.DataFrame,
    alpha_col: str,
    cfg: BacktestConfig,
    is_days: int = 252,
    oos_days: int = 63,
    step_days: int = 63,
    date_col: str = "date",
) -> pd.DataFrame:
    """Roll IS/OOS windows forward, returning one IC row per OOS window."""
    dates = sorted(pd.to_datetime(panel[date_col].dropna().unique()))
    if len(dates) < is_days + oos_days:
        return pd.DataFrame(columns=["window", "oos_start", "oos_end", "ic_mean", "ic_ir"])

    rows: list[dict[str, object]] = []
    window = 0
    idx = 0
    while idx + is_days + oos_days <= len(dates):
        oos_start = dates[idx + is_days]
        oos_end = dates[min(idx + is_days + oos_days - 1, len(dates) - 1)]
        oos = panel[
            (pd.to_datetime(panel[date_col]) >= oos_start)
            & (pd.to_datetime(panel[date_col]) <= oos_end)
        ]
        ic_series = _daily_rank_ic(oos, alpha_col, date_col)
        std = ic_series.std(ddof=0)
        rows.append(
            {
                "window": window,
                "oos_start": oos_start,
                "oos_end": oos_end,
                "ic_mean": float(ic_series.mean()) if not ic_series.empty else np.nan,
                "ic_ir": float(ic_series.mean() / std) if std > 0 else np.nan,
                "n_days": len(ic_series),
            }
        )
        idx += step_days
        window += 1

    return pd.DataFrame(rows)


def _daily_rank_ic(panel: pd.DataFrame, alpha_col: str, date_col: str = "date") -> pd.Series:
    def corr(day: pd.DataFrame) -> float:
        clean = day[[alpha_col, "forward_return"]].dropna()
        if len(clean) < 3:
            return np.nan
        return float(clean[alpha_col].rank().corr(clean["forward_return"].rank()))

    return panel.groupby(date_col).apply(corr, include_groups=False).dropna()
