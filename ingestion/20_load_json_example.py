"""
Load a single JSON price-transparency file into pt_analytics.pt_json_raw.

One row per file: source_file_name (basename), ingested_at (now), raw (full JSON).
Uses BigQuery Load Job (NDJSON) to avoid streaming insert limits.

Run:
  Set GOOGLE_APPLICATION_CREDENTIALS to your service account JSON key path.
  pip install google-cloud-bigquery
  python ingestion/20_load_json_example.py [path/to/file.json]

  Example:
    python ingestion/20_load_json_example.py ./samples/sample.json
"""
import json
import os
import sys
import tempfile
from datetime import datetime, timezone

from google.cloud import bigquery

DATASET = "pt_analytics"
TABLE = "pt_json_raw"
PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT")


def load_json_into_raw(file_path: str, project: str | None = None) -> None:
    with open(file_path, "r", encoding="utf-8-sig") as f:
        payload = json.load(f)

    row = {
        "source_file_name": os.path.basename(file_path),
        "ingested_at": datetime.now(tz=timezone.utc).isoformat(),
        "raw": payload,
    }

    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", suffix=".ndjson", delete=False
    ) as tmp:
        tmp.write(json.dumps(row, ensure_ascii=False) + "\n")
        tmp_path = tmp.name

    try:
        client = bigquery.Client(project=project)
        table_id = f"{client.project}.{DATASET}.{TABLE}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        with open(tmp_path, "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()
        print(f"Loaded 1 row into {table_id} from {file_path}")
    finally:
        os.unlink(tmp_path)


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    path = sys.argv[1]
    if not os.path.isfile(path):
        print(f"File not found: {path}")
        sys.exit(1)
    load_json_into_raw(path, project=PROJECT)


if __name__ == "__main__":
    main()
