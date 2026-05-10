from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import numpy as np
import pandas as pd


AlphaFn = Callable[[pd.DataFrame], pd.Series]


@dataclass(frozen=True)
class AlphaDefinition:
    name: str
    expression: str
    family: str
    hypothesis: str
    expected_direction: int
    compute: AlphaFn

    def __post_init__(self) -> None:
        if self.expected_direction not in (-1, 1):
            raise ValueError(
                f"expected_direction must be -1 or 1, got {self.expected_direction!r}"
            )


def cs_rank(series: pd.Series) -> pd.Series:
    return series.groupby(level=0).rank(pct=True) - 0.5


def ts_rank(series: pd.Series, window: int) -> pd.Series:
    return series.groupby(level=1).transform(lambda s: s.rolling(window).rank(pct=True))


def delta(series: pd.Series, periods: int) -> pd.Series:
    return series.groupby(level=1).diff(periods)


def delay(series: pd.Series, periods: int) -> pd.Series:
    return series.groupby(level=1).shift(periods)


def ts_corr(left: pd.Series, right: pd.Series, window: int) -> pd.Series:
    frame = pd.concat({"left": left, "right": right}, axis=1)
    return frame.groupby(level=1, group_keys=False).apply(
        lambda x: x["left"].rolling(window).corr(x["right"])
    )


def ts_std(series: pd.Series, window: int) -> pd.Series:
    return series.groupby(level=1).transform(lambda s: s.rolling(window).std())


def ts_mean(series: pd.Series, window: int) -> pd.Series:
    return series.groupby(level=1).transform(lambda s: s.rolling(window).mean())


def safe_divide(left: pd.Series, right: pd.Series, eps: float = 1e-9) -> pd.Series:
    return left / right.replace(0, np.nan).fillna(eps)


def make_equity_alpha_registry() -> list[AlphaDefinition]:
    return [
        AlphaDefinition(
            name="alpha_wq_001_reversal_rank",
            expression="-rank(delta(close, 5))",
            family="short_reversal",
            hypothesis="A sharp five-day move tends to partially mean-revert across a liquid universe.",
            expected_direction=1,
            compute=lambda x: -cs_rank(delta(x["adj_close"], 5)),
        ),
        AlphaDefinition(
            name="alpha_wq_002_volume_price_divergence",
            expression="-correlation(rank(delta(log(volume), 2)), rank((close-open)/open), 6)",
            family="volume_price",
            hypothesis="Volume acceleration that agrees too strongly with intraday return can mark crowded short-term pressure.",
            expected_direction=1,
            compute=lambda x: -ts_corr(
                cs_rank(delta(np.log1p(x["volume"]), 2)),
                cs_rank(safe_divide(x["close"] - x["open"], x["open"])),
                6,
            ),
        ),
        AlphaDefinition(
            name="alpha_wq_003_intraday_range_position",
            expression="rank((close-open)/(high-low+0.001))",
            family="intraday_pressure",
            hypothesis="Close location inside the intraday range captures directional pressure without mixing horizons.",
            expected_direction=1,
            compute=lambda x: cs_rank(safe_divide(x["close"] - x["open"], x["high"] - x["low"] + 0.001)),
        ),
        AlphaDefinition(
            name="alpha_trend_021_medium_momentum",
            expression="rank(delay(close / close_21d_ago - 1, 1))",
            family="medium_momentum",
            hypothesis="Medium-horizon winners can persist after a one-day lag removes lookahead leakage.",
            expected_direction=1,
            compute=lambda x: cs_rank(delay(x["adj_close"].groupby(level=1).pct_change(21), 1)),
        ),
        AlphaDefinition(
            name="alpha_risk_020_low_volatility",
            expression="-rank(delay(stddev(returns, 20), 1))",
            family="risk_premium",
            hypothesis="Lower realized volatility names can carry better risk-adjusted forward returns.",
            expected_direction=1,
            compute=lambda x: -cs_rank(delay(ts_std(x["ret_1d"], 20), 1)),
        ),
        AlphaDefinition(
            name="alpha_liquidity_020_volume_shock",
            expression="rank(delay(zscore(volume, 20), 1))",
            family="liquidity",
            hypothesis="Abnormal participation is a clean liquidity-pressure feature worth testing alone.",
            expected_direction=1,
            compute=lambda x: cs_rank(delay(safe_divide(x["volume"] - ts_mean(x["volume"], 20), ts_std(x["volume"], 20)), 1)),
        ),
        AlphaDefinition(
            name="alpha_wq_007_price_to_ma_reversion",
            expression="-rank(close / ts_mean(close, 60) - 1)",
            family="mean_reversion",
            hypothesis="Price stretched above its 60-day average captures intermediate-horizon mean reversion distinct from the 5-day reversal already in the panel.",
            expected_direction=1,
            compute=lambda x: -cs_rank(x["adj_close"] / ts_mean(x["adj_close"], 60) - 1),
        ),
        AlphaDefinition(
            name="alpha_wq_008_overnight_gap",
            expression="rank(open / delay(close, 1) - 1)",
            family="microstructure",
            hypothesis="The overnight gap isolates informed after-hours order flow from intraday noise; it is orthogonal to intraday range signals.",
            expected_direction=1,
            compute=lambda x: cs_rank(safe_divide(x["open"], delay(x["adj_close"], 1)) - 1),
        ),
        AlphaDefinition(
            name="alpha_wq_009_volume_weighted_return",
            expression="rank(sum(ret * volume, 10) / sum(volume, 10))",
            family="volume_momentum",
            hypothesis="Volume-weighted 10-day returns highlight genuine momentum backed by real participation, filtering volume-lite noise moves.",
            expected_direction=1,
            compute=lambda x: cs_rank(
                safe_divide(
                    (x["ret_1d"] * x["volume"]).groupby(level=1).transform(
                        lambda s: s.rolling(10, min_periods=5).sum()
                    ),
                    x["volume"].groupby(level=1).transform(
                        lambda s: s.rolling(10, min_periods=5).sum()
                    ),
                )
            ),
        ),
        AlphaDefinition(
            name="alpha_wq_010_gap_quality",
            expression="-rank(ts_std(open / delay(close, 1) - 1, 20))",
            family="quality",
            hypothesis="Low overnight-gap volatility over 20 days proxies earnings stability and information quality, orthogonal to intraday range and volume signals.",
            expected_direction=1,
            compute=lambda x: -cs_rank(ts_std(safe_divide(x["open"], delay(x["adj_close"], 1)) - 1, 20)),
        ),
    ]
