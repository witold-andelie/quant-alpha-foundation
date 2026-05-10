from __future__ import annotations

import hashlib
from datetime import datetime

import numpy as np
import pandas as pd
import yfinance as yf

from quant_alpha.config import ProjectConfig, Universe


PRICE_COLUMNS = ["date", "symbol", "open", "high", "low", "close", "adj_close", "volume"]


def _stable_seed(symbol: str) -> int:
    digest = hashlib.sha256(symbol.encode("utf-8")).hexdigest()
    return int(digest[:8], 16)


def generate_synthetic_prices(cfg: ProjectConfig, universe: Universe) -> pd.DataFrame:
    dates = pd.bdate_range(cfg.start_date, cfg.end_date)
    frames: list[pd.DataFrame] = []

    for symbol in universe.symbols:
        rng = np.random.default_rng(_stable_seed(symbol))
        drift = rng.normal(0.0003, 0.0001)
        vol = rng.uniform(0.012, 0.026)
        shocks = rng.normal(drift, vol, len(dates))
        close = 100 * np.exp(np.cumsum(shocks))
        open_ = close * (1 + rng.normal(0, 0.003, len(dates)))
        high = np.maximum(open_, close) * (1 + rng.uniform(0.001, 0.018, len(dates)))
        low = np.minimum(open_, close) * (1 - rng.uniform(0.001, 0.018, len(dates)))
        volume = rng.integers(2_000_000, 80_000_000, len(dates))

        frames.append(
            pd.DataFrame(
                {
                    "date": dates,
                    "symbol": symbol,
                    "open": open_,
                    "high": high,
                    "low": low,
                    "close": close,
                    "adj_close": close,
                    "volume": volume,
                }
            )
        )

    return pd.concat(frames, ignore_index=True)[PRICE_COLUMNS]


def _normalize_yfinance_frame(data: pd.DataFrame, symbols: list[str]) -> pd.DataFrame:
    if data.empty:
        raise RuntimeError("Yahoo Finance returned no rows for the configured universe.")

    frames: list[pd.DataFrame] = []
    if isinstance(data.columns, pd.MultiIndex):
        for symbol in symbols:
            if symbol not in data.columns.get_level_values(0):
                continue
            part = data[symbol].reset_index()
            part["symbol"] = symbol
            frames.append(part)
    else:
        part = data.reset_index()
        part["symbol"] = symbols[0]
        frames.append(part)

    prices = pd.concat(frames, ignore_index=True)
    prices.columns = [str(col).lower().replace(" ", "_") for col in prices.columns]
    rename_map = {"datetime": "date", "adj_close": "adj_close"}
    prices = prices.rename(columns=rename_map)
    if "date" not in prices.columns and "index" in prices.columns:
        prices = prices.rename(columns={"index": "date"})
    if "adj_close" not in prices.columns:
        prices["adj_close"] = prices["close"]

    prices["date"] = pd.to_datetime(prices["date"]).dt.tz_localize(None)
    return prices[PRICE_COLUMNS].dropna(subset=["date", "symbol", "close"])


def fetch_prices(cfg: ProjectConfig, universe: Universe, offline: bool = False) -> pd.DataFrame:
    if offline:
        return generate_synthetic_prices(cfg, universe)

    data = yf.download(
        tickers=universe.symbols,
        start=cfg.start_date,
        end=cfg.end_date,
        interval=cfg.bar_interval,
        auto_adjust=False,
        group_by="ticker",
        progress=False,
        threads=True,
    )
    prices = _normalize_yfinance_frame(data, universe.symbols)
    if prices.empty:
        as_of = datetime.utcnow().isoformat(timespec="seconds")
        raise RuntimeError(f"No prices fetched as of {as_of} UTC.")
    return prices.sort_values(["symbol", "date"]).reset_index(drop=True)
