"""
GCS-first bulk ingestion runner: list blobs under pt_incoming only, move to processing,
download, process (JSON -> pt_json_extracted_rates + registry; CSV -> pt_csv_raw_tall or pt_csv_raw_wide),
verify rows in BigQuery, then archive to success or failed. Optionally delete success blobs.
Uses BigQuery load jobs only (no streaming inserts).
"""
import argparse
import json
import os
import sys
import tempfile
from datetime import datetime, timezone

from google.cloud import bigquery

_ingestion_dir = os.path.dirname(os.path.abspath(__file__))


def _load_module(name: str, filename: str):
    from importlib.util import spec_from_file_location, module_from_spec
    path = os.path.join(_ingestion_dir, filename)
    spec = spec_from_file_location(name, path)
    mod = module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_mod_50 = _load_module("extract_json", "50_extract_json_to_ndjson.py")
_mod_60 = _load_module("load_ndjson", "60_load_ndjson_to_bigquery.py")
_mod_30 = _load_module("load_csv", "30_load_csv_examples.py")
_gcs = _load_module("gcs_utils", "42_gcs_utils.py")

run_extract = _mod_50.run_extract
load_ndjson_to_table = _mod_60.load_ndjson_to_table
detect_csv_format = _mod_30.detect_csv_format
load_csv_file_to_bigquery = _mod_30.load_csv_file_to_bigquery
read_csv_preamble_and_headers = _mod_30.read_csv_preamble_and_headers
WIDE_MIN_COLS = getattr(_mod_30, "WIDE_MIN_COLS", 38)
list_blob_names = _gcs.list_blob_names
copy_blob = _gcs.copy_blob
download_blob_to_file = _gcs.download_blob_to_file
upload_string_to_key = _gcs.upload_string_to_key
delete_gcs_uri = _gcs.delete_gcs_uri
compute_sha256 = _gcs.compute_sha256
file_size_bytes = _gcs.file_size_bytes

DATASET = "pt_analytics"
PT_JSON_EXTRACTED_TABLE = "pt_json_extracted_rates"
PT_JSON_REGISTRY_TABLE = "pt_json_registry"
PT_CSV_REGISTRY_TABLE = "pt_csv_registry"
PT_CSV_RAW_TALL = "pt_csv_raw_tall"
PT_CSV_RAW_WIDE = "pt_csv_raw_wide"


def _already_ingested_success(client: bigquery.Client, source_file_name: str) -> bool:
    """True if pt_json_registry has a success row for this source_file_name."""
    query = f"""
    SELECT 1 FROM `{client.project}.{DATASET}.{PT_JSON_REGISTRY_TABLE}`
    WHERE source_file_name = @fname AND status = 'success'
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("fname", "STRING", source_file_name)]
    )
    result = list(client.query(query, job_config=job_config).result())
    return len(result) > 0


def _count_extracted_for_file_run(client: bigquery.Client, source_file_name: str, run_id: str) -> int:
    query = f"""
    SELECT COUNT(*) AS n FROM `{client.project}.{DATASET}.{PT_JSON_EXTRACTED_TABLE}`
    WHERE source_file_name = @fname AND run_id = @run_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("fname", "STRING", source_file_name),
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
    )
    row = next(client.query(query, job_config=job_config).result())
    return row.n or 0


def _count_csv_rows_for_file_run(
    client: bigquery.Client, table: str, source_file_name: str
) -> int:
    """Count rows by source_file_name only (temporary: no ingested_at equality to avoid timestamp precision mismatch)."""
    query = f"""
    SELECT COUNT(*) AS c FROM `{client.project}.{DATASET}.{table}`
    WHERE source_file_name = @fname
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("fname", "STRING", source_file_name)]
    )
    row = next(client.query(query, job_config=job_config).result())
    return row.c or 0


def _append_csv_registry_row(
    client: bigquery.Client,
    run_id: str,
    source_file_name: str,
    ingested_at: str,
    format_hint: str | None,
    header_count: int | None,
    delimiter: str | None,
    encoding: str | None,
    row1_raw: str | None,
    row2_raw: str | None,
    preamble_kv: dict | None,
    rows_loaded: int,
    status: str,
    error_message: str | None,
) -> None:
    """Append one row to pt_csv_registry via load job (no streaming)."""
    table_id = f"{client.project}.{DATASET}.{PT_CSV_REGISTRY_TABLE}"
    row = {
        "source_file_name": source_file_name,
        "run_id": run_id,
        "ingested_at": ingested_at,
        "format_hint": format_hint,
        "header_count": header_count,
        "delimiter": delimiter,
        "encoding": encoding,
        "row1_raw": row1_raw,
        "row2_raw": row2_raw,
        "preamble_kv": preamble_kv,
        "rows_loaded": rows_loaded,
        "status": status,
        "error_message": error_message,
    }
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".ndjson", delete=False) as tmp:
        tmp.write(json.dumps(row, ensure_ascii=False) + "\n")
        tmp_path = tmp.name
    try:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        with open(tmp_path, "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()
    finally:
        os.unlink(tmp_path)


def _append_registry_row(
    client: bigquery.Client,
    run_id: str,
    source_file_name: str,
    local_path: str,
    ingested_at: str,
    status: str,
    rows_extracted: int,
    rows_loaded: int,
    error_message: str | None,
    gcs_uri: str | None = None,
    file_sha256: str | None = None,
    file_size_bytes_val: int | None = None,
) -> None:
    table_id = f"{client.project}.{DATASET}.{PT_JSON_REGISTRY_TABLE}"
    row = {
        "source_file_name": source_file_name,
        "local_path": local_path,
        "gcs_uri": gcs_uri,
        "ingested_at": ingested_at,
        "run_id": run_id,
        "status": status,
        "rows_extracted": rows_extracted,
        "rows_loaded": rows_loaded,
        "file_size_bytes": file_size_bytes_val,
        "file_sha256": file_sha256,
        "error_message": error_message,
    }
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".ndjson", delete=False) as tmp:
        tmp.write(json.dumps(row, ensure_ascii=False) + "\n")
        tmp_path = tmp.name
    try:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        with open(tmp_path, "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()
    finally:
        os.unlink(tmp_path)


def _fail_blob(
    bucket: str,
    processing_key: str,
    basename: str,
    lower: str,
    tb: str,
    client: bigquery.Client,
    run_id: str,
    source_file_name: str,
    local_path: str,
    ingested_at: str,
    gcs_uri: str | None,
    file_sha256_val: str | None,
    file_size_bytes_val: int | None,
    archive_failed_prefix: str,
) -> None:
    """Copy processing -> archive/failed, delete processing, upload _error.txt. Never delete failed blobs."""
    failed_key = f"{archive_failed_prefix}/{basename}"
    copy_blob(bucket, processing_key, failed_key)
    delete_gcs_uri(f"gs://{bucket}/{processing_key}")
    error_log_key = f"{archive_failed_prefix}/{os.path.splitext(basename)[0]}_error.txt"
    upload_string_to_key(bucket, error_log_key, tb)
    if lower.endswith(".json"):
        try:
            _append_registry_row(
                client, run_id, source_file_name, local_path, ingested_at, "failed", 0, 0, tb,
                gcs_uri=gcs_uri, file_sha256=file_sha256_val, file_size_bytes_val=file_size_bytes_val,
            )
        except Exception:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description="GCS-first bulk ingest: list pt_incoming, process, verify, archive.")
    parser.add_argument("--gcs_bucket", required=True, help="GCS bucket name (e.g. kc-pt-landing)")
    parser.add_argument("--gcs_incoming_prefix", default="pt_incoming", help="Prefix for incoming blobs")
    parser.add_argument("--gcs_processing_prefix", default="pt_processing", help="Prefix for in-flight blobs")
    parser.add_argument("--gcs_archive_success_prefix", default="pt_archive/success", help="Prefix for successful archive")
    parser.add_argument("--gcs_archive_failed_prefix", default="pt_archive/failed", help="Prefix for failed archive")
    parser.add_argument("--no_delete_gcs_on_success", action="store_true", help="Keep success blobs in GCS (default: delete to avoid cost)")
    parser.add_argument("--run_id", default=None, help="Run identifier (default: UTC timestamp)")
    parser.add_argument("--json_item_path", default="standard_charge_information.item", help="ijson item path for JSON")
    parser.add_argument("--max_records_json", type=int, default=0, help="Max rows per JSON file (0 = unlimited)")
    parser.add_argument("--project", default=os.environ.get("GOOGLE_CLOUD_PROJECT"), help="GCP project (default: GOOGLE_CLOUD_PROJECT env)")
    parser.add_argument("--debug_gcs_list", action="store_true", help="List first N blobs in bucket (no prefix) and exit")
    parser.add_argument("--debug_limit", type=int, default=50, help="Max blobs to list when --debug_gcs_list (default 50)")
    args = parser.parse_args()

    bucket = args.gcs_bucket
    project = args.project
    incoming_prefix_arg = args.gcs_incoming_prefix
    incoming_prefix_normalized = (incoming_prefix_arg + "/") if (incoming_prefix_arg != "" and not incoming_prefix_arg.endswith("/")) else incoming_prefix_arg
    processing_prefix = args.gcs_processing_prefix.rstrip("/")
    archive_success_prefix = args.gcs_archive_success_prefix.rstrip("/")
    archive_failed_prefix = args.gcs_archive_failed_prefix.rstrip("/")
    delete_gcs_on_success = not args.no_delete_gcs_on_success

    creds_source = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or "ADC"
    print(f"bucket: {bucket}")
    print(f"incoming_prefix (arg): {incoming_prefix_arg!r}")
    print(f"incoming_prefix (normalized): {incoming_prefix_normalized!r}")
    print(f"project: {project}")
    print(f"credentials: {creds_source}")

    if args.debug_gcs_list:
        list_blobs_debug = _gcs.list_blobs_debug
        names = list_blobs_debug(bucket, project=project, limit=args.debug_limit)
        for n in names:
            print(n)
        sys.exit(0)

    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    client = bigquery.Client(project=project)

    try:
        blob_keys = list_blob_names(bucket, incoming_prefix_arg, project=project)
    except Exception as e:
        print(f"Failed to list blobs under prefix {incoming_prefix_normalized!r}: {e!r}", file=sys.stderr)
        sys.exit(2)

    if not blob_keys:
        print(f"No blobs found under prefix: {incoming_prefix_normalized!r}")
        print("Verify objects exist and that the service account has storage.objects.list on the bucket.")
        return

    print(f"Found {len(blob_keys)} blob(s) under incoming prefix.")
    for key in blob_keys:
        print(key)

    for key in blob_keys:
        basename = os.path.basename(key)
        ext = os.path.splitext(basename)[1].lower() if os.path.splitext(basename)[1] else ""
        lower = basename.lower()
        source_file_name = basename

        if lower.endswith(".json"):
            try:
                if _already_ingested_success(client, source_file_name):
                    success_key = f"{archive_success_prefix}/{basename}"
                    copy_blob(bucket, key, success_key)
                    delete_gcs_uri(f"gs://{bucket}/{key}")
                    if delete_gcs_on_success:
                        delete_gcs_uri(f"gs://{bucket}/{success_key}")
                    print(f"Already ingested; archiving without reprocessing: {key}")
                    print(f"Final archive destination: gs://{bucket}/{success_key}")
                    continue
            except Exception as e:
                print(f"Registry check failed for {source_file_name}: {e}", file=sys.stderr)
                continue

        processing_key = f"{processing_prefix}/{basename}"
        try:
            copy_blob(bucket, key, processing_key)
            delete_gcs_uri(f"gs://{bucket}/{key}")
        except Exception as e:
            print(f"Move to processing failed for {key}: {e}", file=sys.stderr)
            continue

        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(basename)[1]) as tmp:
            local_path = tmp.name
        try:
            download_blob_to_file(bucket, processing_key, local_path)
        except Exception as e:
            print(f"[{key}] extension={ext} -> Download failed: {e}", file=sys.stderr)
            try:
                delete_gcs_uri(f"gs://{bucket}/{processing_key}")
            except Exception:
                pass
            continue

        try:
            blob_size = file_size_bytes(local_path)
        except Exception:
            blob_size = None
        ingested_at = datetime.now(tz=timezone.utc).isoformat()
        gcs_uri = f"gs://{bucket}/{processing_key}"
        file_sha256_val = None
        file_size_bytes_val = None
        try:
            file_sha256_val = compute_sha256(local_path)
            file_size_bytes_val = file_size_bytes(local_path)
        except Exception:
            pass

        try:
            if lower.endswith(".json"):
                # --- JSON path: extract -> load -> verify -> then archive ---
                ndjson_path = None
                rows_extracted = 0
                rows_loaded = 0
                verify_count = 0
                with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".ndjson", delete=False) as ndtmp:
                    ndjson_path = ndtmp.name
                try:
                    rows_extracted = run_extract(
                        local_json_path=local_path,
                        output_ndjson_path=ndjson_path,
                        source_file_name=source_file_name,
                        item_path=args.json_item_path,
                        max_records=args.max_records_json,
                        run_id=run_id,
                    )
                    if rows_extracted > 0 and os.path.isfile(ndjson_path):
                        rows_loaded = load_ndjson_to_table(ndjson_path, DATASET, PT_JSON_EXTRACTED_TABLE, project=project)
                finally:
                    if ndjson_path and os.path.isfile(ndjson_path):
                        os.unlink(ndjson_path)

                print(f"[{key}] extension={ext} size={blob_size} path=json -> JSON load rows: {rows_loaded}")
                if rows_loaded <= 0:
                    raise ValueError("JSON load returned 0 rows; verification failed.")

                verify_count = _count_extracted_for_file_run(client, source_file_name, run_id)
                print(f"[{key}] -> JSON verify count: {verify_count}")
                if verify_count == 0:
                    raise ValueError("BigQuery verification returned 0 rows for source_file_name and run_id.")

                _append_registry_row(
                    client, run_id, source_file_name, local_path, ingested_at, "success",
                    rows_extracted, rows_loaded, None,
                    gcs_uri=gcs_uri, file_sha256=file_sha256_val, file_size_bytes_val=file_size_bytes_val,
                )
                success_key = f"{archive_success_prefix}/{basename}"
                print(f"Archiving JSON to success: {success_key}")
                copy_blob(bucket, processing_key, success_key)
                print(f"Deleting processing blob: {processing_key}")
                delete_gcs_uri(f"gs://{bucket}/{processing_key}")
                if delete_gcs_on_success:
                    print(f"Deleting archived success blob (optional): {success_key}")
                    delete_gcs_uri(f"gs://{bucket}/{success_key}")
                print(f"Final archive destination: gs://{bucket}/{success_key}")
                print(f"[{key}] path=json load_rows={rows_loaded} verify_count={verify_count} -> success->archived")

            elif lower.endswith(".csv"):
                # --- CSV path: detect format -> load -> verify -> registry row -> archive ---
                source_file_name = os.path.basename(key)
                format_hint, header_count = detect_csv_format(local_path)
                table = PT_CSV_RAW_TALL if format_hint == "TALL" else PT_CSV_RAW_WIDE
                print(f"[{key}] detected_csv_format={format_hint} routing_table={table} source_file_name={source_file_name} header_count={header_count}")
                (
                    rows_loaded, ingested_at_iso, csv_encoding, csv_delimiter, csv_header_count,
                    row1_raw, row2_raw, preamble_kv,
                ) = load_csv_file_to_bigquery(
                    local_path, client, DATASET, table, format_hint, source_file_name_override=source_file_name
                )
                csv_table_id = f"{client.project}.{DATASET}.{table}"
                print(f"CSV loaded into table: {csv_table_id}")
                print(f"[{key}] extension={ext} size={blob_size} path=csv table={table} -> CSV load rows: {rows_loaded}")
                if rows_loaded <= 0:
                    raise ValueError("CSV load returned 0 rows; verification failed.")

                print(f"CSV verifying table: {client.project}.{DATASET}.{table}")
                verify_count = _count_csv_rows_for_file_run(client, table, source_file_name)
                print(f"[{key}] -> CSV verify count: {verify_count}")
                if verify_count == 0:
                    raise ValueError("BigQuery verification returned 0 rows for source_file_name.")

                _append_csv_registry_row(
                    client, run_id, source_file_name, ingested_at_iso,
                    format_hint, csv_header_count, csv_delimiter, csv_encoding,
                    row1_raw, row2_raw, preamble_kv, rows_loaded, "success", None,
                )
                success_key = f"{archive_success_prefix}/{basename}"
                print(f"Archiving CSV to success: {success_key}")
                copy_blob(bucket, processing_key, success_key)
                print(f"Deleting processing blob: {processing_key}")
                delete_gcs_uri(f"gs://{bucket}/{processing_key}")
                if delete_gcs_on_success:
                    print(f"Deleting archived success blob (optional): {success_key}")
                    delete_gcs_uri(f"gs://{bucket}/{success_key}")
                print(f"Final archive destination: gs://{bucket}/{success_key}")
                print(f"[{key}] path=csv table={table} load_rows={rows_loaded} verify_count={verify_count} -> success->archived")

            else:
                tb = f"Unknown file type: {basename}"
                failed_key = f"{archive_failed_prefix}/{basename}"
                _fail_blob(
                    bucket, processing_key, basename, lower, tb, client, run_id,
                    source_file_name, local_path, ingested_at, gcs_uri, file_sha256_val, file_size_bytes_val,
                    archive_failed_prefix,
                )
                print(f"Final archive destination (failed): gs://{bucket}/{failed_key}")
                print(f"[{key}] extension={ext} -> failed->archived+error (unknown type)")

        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            failed_key = f"{archive_failed_prefix}/{basename}"
            _fail_blob(
                bucket, processing_key, basename, lower, tb, client, run_id,
                source_file_name, local_path, ingested_at, gcs_uri, file_sha256_val, file_size_bytes_val,
                archive_failed_prefix,
            )
            if lower.endswith(".csv"):
                try:
                    enc, delim, r1, r2, hdrs = read_csv_preamble_and_headers(local_path)
                    hc = len(hdrs)
                    fh = "WIDE" if hc >= WIDE_MIN_COLS else "TALL"
                    pkv = {}
                    for i in range(0, len(r1) - 1, 2):
                        if r1[i].strip():
                            pkv[r1[i]] = r1[i + 1]
                    _append_csv_registry_row(
                        client, run_id, source_file_name, ingested_at,
                        fh, hc, delim, enc, json.dumps(r1), json.dumps(r2), pkv,
                        0, "failed", tb,
                    )
                except Exception:
                    pass
            print(f"Final archive destination (failed): gs://{bucket}/{failed_key}")
            print(f"[{key}] extension={ext} size={blob_size} -> failed->archived+error: {e!r}", file=sys.stderr)
        finally:
            if os.path.isfile(local_path):
                try:
                    os.unlink(local_path)
                except OSError:
                    pass

    print(f"Bulk run {run_id} finished.")


if __name__ == "__main__":
    main()
