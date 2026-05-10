from __future__ import annotations

import os
import ssl
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Iterable

import pandas as pd


class EntsoeError(RuntimeError):
    pass


@dataclass(frozen=True)
class EntsoeClient:
    token: str
    base_url: str = "https://web-api.tp.entsoe.eu/api"
    timeout_seconds: int = 60
    polite_sleep_seconds: float = 0.2

    @classmethod
    def from_env(
        cls,
        token_env: str = "ENTSOE_API_KEY",
        base_url: str = "https://web-api.tp.entsoe.eu/api",
        timeout_seconds: int = 60,
    ) -> "EntsoeClient":
        token = os.getenv(token_env)
        if not token:
            raise EntsoeError(
                f"Missing ENTSO-E API token. Set {token_env} before using data_source=entsoe."
            )
        return cls(token=token, base_url=base_url, timeout_seconds=timeout_seconds)

    def request(self, params: dict[str, str]) -> bytes:
        query = {"securityToken": self.token, **params}
        url = f"{self.base_url}?{urllib.parse.urlencode(query)}"
        context = ssl.create_default_context()
        try:
            import certifi

            context.load_verify_locations(cafile=certifi.where())
        except Exception:
            # Fall back to platform trust store if certifi is unavailable.
            pass
        with urllib.request.urlopen(url, timeout=self.timeout_seconds, context=context) as response:
            payload = response.read()
        time.sleep(self.polite_sleep_seconds)
        return payload


def _strip_namespace(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _children(element: ET.Element, name: str) -> Iterable[ET.Element]:
    return (child for child in element if _strip_namespace(child.tag) == name)


def _first_text(element: ET.Element, name: str) -> str | None:
    for child in _children(element, name):
        return child.text
    return None


def _period_start(period: ET.Element) -> datetime:
    for interval in _children(period, "timeInterval"):
        start = _first_text(interval, "start")
        if start:
            return pd.Timestamp(start).to_pydatetime()
    raise EntsoeError("ENTSO-E response period is missing timeInterval/start.")


def _resolution_delta(value: str | None) -> timedelta:
    if value in {"PT15M", "PT15m"}:
        return timedelta(minutes=15)
    if value in {"PT30M", "PT30m"}:
        return timedelta(minutes=30)
    if value in {"PT60M", "PT1H", "PT60m", "PT1h"}:
        return timedelta(hours=1)
    if value in {"P1D", "P1d"}:
        return timedelta(days=1)
    raise EntsoeError(f"Unsupported ENTSO-E resolution: {value}")


def parse_entsoe_timeseries(xml_payload: bytes, value_names: tuple[str, ...]) -> pd.Series:
    root = ET.parse(BytesIO(xml_payload)).getroot()
    records: list[dict[str, object]] = []

    for timeseries in root.iter():
        if _strip_namespace(timeseries.tag) != "TimeSeries":
            continue
        for period in _children(timeseries, "Period"):
            start = _period_start(period)
            delta = _resolution_delta(_first_text(period, "resolution"))
            for point in _children(period, "Point"):
                position_text = _first_text(point, "position")
                if position_text is None:
                    continue
                value_text = next(
                    (_first_text(point, value_name) for value_name in value_names if _first_text(point, value_name)),
                    None,
                )
                if value_text is None:
                    continue
                records.append(
                    {
                        "timestamp": start + delta * (int(position_text) - 1),
                        "value": float(value_text),
                    }
                )

    if not records:
        return pd.Series(dtype="float64", name="value")

    frame = pd.DataFrame(records)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True).dt.tz_convert(None)
    return frame.groupby("timestamp")["value"].mean().sort_index()


def _period_params(start: str, end: str) -> dict[str, str]:
    start_ts = pd.Timestamp(start, tz=timezone.utc)
    end_ts = pd.Timestamp(end, tz=timezone.utc)
    return {
        "periodStart": start_ts.strftime("%Y%m%d%H%M"),
        "periodEnd": end_ts.strftime("%Y%m%d%H%M"),
    }


def _query_series(client: EntsoeClient, params: dict[str, str], value_names: tuple[str, ...]) -> pd.Series:
    try:
        payload = client.request(params)
    except Exception as exc:  # pragma: no cover - network failures vary by platform
        raise EntsoeError(f"ENTSO-E request failed for {params}: {exc}") from exc
    return parse_entsoe_timeseries(payload, value_names)


def _resample(series: pd.Series, interval: str) -> pd.Series:
    if series.empty:
        return series
    return series.resample(interval).mean().interpolate(limit_direction="both")


def fetch_entsoe_power_market(
    markets: list[str],
    domains: dict[str, str],
    start: str,
    end: str,
    bar_interval: str,
    client: EntsoeClient,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    period = _period_params(start, end)

    for market in markets:
        domain = domains.get(market)
        if not domain:
            raise EntsoeError(f"Missing ENTSO-E bidding-zone domain code for market {market}.")

        spot = _query_series(
            client,
            {
                "documentType": "A44",
                "in_Domain": domain,
                "out_Domain": domain,
                **period,
            },
            ("price.amount",),
        )
        load = _query_series(
            client,
            {
                "documentType": "A65",
                "processType": "A01",
                "outBiddingZone_Domain": domain,
                **period,
            },
            ("quantity",),
        )
        solar = _query_series(
            client,
            {
                "documentType": "A69",
                "processType": "A01",
                "in_Domain": domain,
                "psrType": "B16",
                **period,
            },
            ("quantity",),
        )
        wind_onshore = _query_series(
            client,
            {
                "documentType": "A69",
                "processType": "A01",
                "in_Domain": domain,
                "psrType": "B19",
                **period,
            },
            ("quantity",),
        )
        wind_offshore = _query_series(
            client,
            {
                "documentType": "A69",
                "processType": "A01",
                "in_Domain": domain,
                "psrType": "B18",
                **period,
            },
            ("quantity",),
        )

        market_frame = pd.concat(
            {
                "spot_price": _resample(spot, bar_interval),
                "load_forecast": _resample(load, bar_interval),
                "solar_forecast": _resample(solar, bar_interval),
                "wind_forecast": _resample(wind_onshore.add(wind_offshore, fill_value=0), bar_interval),
            },
            axis=1,
        ).dropna(subset=["spot_price", "load_forecast"])

        market_frame["solar_forecast"] = market_frame["solar_forecast"].fillna(0)
        market_frame["wind_forecast"] = market_frame["wind_forecast"].fillna(0)
        market_frame["residual_load"] = (
            market_frame["load_forecast"]
            - market_frame["wind_forecast"]
            - market_frame["solar_forecast"]
        )
        scarcity = market_frame["residual_load"].rank(pct=True).fillna(0.5) - 0.5
        market_frame["imbalance_price"] = market_frame["spot_price"] + scarcity * 10.0
        market_frame["market"] = market
        market_frame = market_frame.reset_index().rename(columns={"index": "timestamp"})
        frames.append(market_frame)

    if not frames:
        raise EntsoeError("ENTSO-E returned no data for the requested markets.")

    return pd.concat(frames, ignore_index=True)[
        [
            "timestamp",
            "market",
            "spot_price",
            "load_forecast",
            "wind_forecast",
            "solar_forecast",
            "residual_load",
            "imbalance_price",
        ]
    ]
