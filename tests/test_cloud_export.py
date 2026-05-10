from __future__ import annotations

import pandas as pd
import pytest

from quant_alpha.config import CloudExportConfig
from quant_alpha.storage.gcp import CloudExportError, export_frames_to_gcs_bigquery


def test_cloud_export_disabled_is_noop() -> None:
    result = export_frames_to_gcs_bigquery(
        {"table": pd.DataFrame({"x": [1]})},
        CloudExportConfig(enabled=False),
    )

    assert result == {}


def test_cloud_export_requires_destination_config() -> None:
    with pytest.raises(CloudExportError):
        export_frames_to_gcs_bigquery(
            {"table": pd.DataFrame({"x": [1]})},
            CloudExportConfig(enabled=True),
        )
