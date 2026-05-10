from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DatasetContract:
    name: str
    grain: str
    owner: str
    primary_keys: tuple[str, ...]
    freshness_expectation: str


EQUITY_DATASETS = [
    DatasetContract(
        name="raw_prices",
        grain="daily x symbol",
        owner="ingestion-team",
        primary_keys=("date", "symbol"),
        freshness_expectation="daily",
    ),
    DatasetContract(
        name="alpha_panel",
        grain="daily x symbol",
        owner="alpha-research",
        primary_keys=("date", "symbol"),
        freshness_expectation="daily",
    ),
    DatasetContract(
        name="alpha_diagnostics",
        grain="per alpha",
        owner="alpha-research",
        primary_keys=("alpha_name",),
        freshness_expectation="daily",
    ),
    DatasetContract(
        name="backtest_daily",
        grain="daily x alpha",
        owner="alpha-research",
        primary_keys=("date", "alpha_name"),
        freshness_expectation="daily",
    ),
]

ENERGY_DATASETS = [
    DatasetContract(
        name="power_market_raw",
        grain="hourly x market",
        owner="research-platform",
        primary_keys=("timestamp", "market"),
        freshness_expectation="hourly",
    ),
    DatasetContract(
        name="energy_alpha_features",
        grain="hourly x market",
        owner="research-platform",
        primary_keys=("timestamp", "market"),
        freshness_expectation="hourly",
    ),
    DatasetContract(
        name="energy_alpha_diagnostics",
        grain="per alpha",
        owner="energy-research",
        primary_keys=("alpha_name",),
        freshness_expectation="daily",
    ),
]

ALL_DATASETS = EQUITY_DATASETS + ENERGY_DATASETS
