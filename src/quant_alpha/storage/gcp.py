from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from quant_alpha.config import CloudExportConfig


class CloudExportError(RuntimeError):
    pass


def export_frames_to_gcs_bigquery(
    frames: dict[str, pd.DataFrame],
    config: CloudExportConfig,
) -> dict[str, str]:
    if not config.enabled:
        return {}
    if not config.gcp_project_id or not config.gcs_bucket or not config.bigquery_dataset:
        raise CloudExportError(
            "Cloud export requires gcp_project_id, gcs_bucket, and bigquery_dataset."
        )

    try:
        from google.cloud import bigquery, storage
    except ImportError as exc:  # pragma: no cover - exercised only without cloud extra
        raise CloudExportError(
            "Install the cloud extra before exporting: pip install -e '.[cloud]'."
        ) from exc

    storage_client = storage.Client(project=config.gcp_project_id)
    bq_client = bigquery.Client(project=config.gcp_project_id, location=config.bigquery_location)
    bucket = storage_client.bucket(config.gcs_bucket)
    exported: dict[str, str] = {}

    import re

    def _validate_table_name(name: str) -> None:
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
            raise CloudExportError(f"Invalid table name for cloud export: {name!r}")

    with TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        for table_name, frame in frames.items():
            _validate_table_name(table_name)
            local_path = tmp_path / f"{table_name}.parquet"
            frame.to_parquet(local_path, index=False)

            blob_name = f"{config.gcs_prefix.rstrip('/')}/{table_name}/{table_name}.parquet"
            blob = bucket.blob(blob_name)
            try:
                blob.upload_from_filename(str(local_path))
            except Exception as exc:
                raise CloudExportError(f"GCS upload failed for {table_name}: {exc}") from exc
            gcs_uri = f"gs://{config.gcs_bucket}/{blob_name}"

            table_id = f"{config.gcp_project_id}.{config.bigquery_dataset}.{table_name}"
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=config.write_disposition,
                autodetect=True,
            )
            try:
                load_job = bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
                load_job.result()
            except Exception as exc:
                raise CloudExportError(f"BigQuery load failed for {table_name}: {exc}") from exc
            exported[table_name] = table_id

    return exported
