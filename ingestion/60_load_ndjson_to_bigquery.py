"""
Load NDJSON into BigQuery using a load job (NOT streaming insert).
Uses NEWLINE_DELIMITED_JSON and WRITE_APPEND.

Usage:
  Set GOOGLE_APPLICATION_CREDENTIALS to your service account JSON key path.
  python ingestion/60_load_ndjson_to_bigquery.py "C:\\path\\json_rates.ndjson" pt_analytics pt_json_extracted_rates
"""
import argparse
import os
import sys

from google.cloud import bigquery


def load_ndjson_to_table(
    ndjson_path: str,
    dataset: str,
    table: str,
    project: str | None = None,
) -> int:
    """
    Load NDJSON file into a BigQuery table via load job (WRITE_APPEND).
    Returns number of rows loaded (job.output_rows).
    Raises if file not found or job fails.
    """
    path = os.path.abspath(ndjson_path)
    if not os.path.isfile(path):
        raise FileNotFoundError(f"File not found: {path}")

    client = bigquery.Client(project=project or os.environ.get("GOOGLE_CLOUD_PROJECT"))
    table_id = f"{client.project}.{dataset}.{table}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    with open(path, "rb") as f:
        job = client.load_table_from_file(f, table_id, job_config=job_config)
    job.result()

    return job.output_rows or 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Load NDJSON file into a BigQuery table via load job.")
    parser.add_argument("ndjson_path", help="Path to the NDJSON file")
    parser.add_argument("dataset", help="BigQuery dataset id (e.g. pt_analytics)")
    parser.add_argument("table", help="BigQuery table id (e.g. pt_json_extracted_rates)")
    parser.add_argument("--project", default=os.environ.get("GOOGLE_CLOUD_PROJECT"), help="GCP project (default from env or ADC)")
    args = parser.parse_args()

    try:
        rows = load_ndjson_to_table(args.ndjson_path, args.dataset, args.table, project=args.project)
        print(f"Loaded rows: {rows}")
        print(f"Destination: {args.dataset}.{args.table}")
    except FileNotFoundError as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
