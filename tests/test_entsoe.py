from __future__ import annotations

import pandas as pd
import pytest

from quant_alpha.ingestion.entsoe import EntsoeError, _period_params, parse_entsoe_timeseries


SAMPLE_PRICE_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3">
  <TimeSeries>
    <Period>
      <timeInterval>
        <start>2024-01-01T00:00Z</start>
        <end>2024-01-01T03:00Z</end>
      </timeInterval>
      <resolution>PT60M</resolution>
      <Point>
        <position>1</position>
        <price.amount>40.5</price.amount>
      </Point>
      <Point>
        <position>2</position>
        <price.amount>42.0</price.amount>
      </Point>
      <Point>
        <position>3</position>
        <price.amount>39.0</price.amount>
      </Point>
    </Period>
  </TimeSeries>
</Publication_MarketDocument>
"""


def test_parse_entsoe_timeseries_handles_namespaced_price_points() -> None:
    series = parse_entsoe_timeseries(SAMPLE_PRICE_XML, ("price.amount",))

    assert list(series.index) == list(pd.date_range("2024-01-01", periods=3, freq="h"))
    assert series.tolist() == [40.5, 42.0, 39.0]


def test_period_params_are_utc_entsoe_format() -> None:
    params = _period_params("2024-01-01", "2024-01-02")

    assert params == {
        "periodStart": "202401010000",
        "periodEnd": "202401020000",
    }


def test_parser_rejects_unsupported_resolution() -> None:
    xml = SAMPLE_PRICE_XML.replace(b"<resolution>PT60M</resolution>", b"<resolution>P1Y</resolution>")

    with pytest.raises(EntsoeError):
        parse_entsoe_timeseries(xml, ("price.amount",))
