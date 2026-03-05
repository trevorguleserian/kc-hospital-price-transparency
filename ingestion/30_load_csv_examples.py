"""
Load CSV price-transparency files (tall and wide) into pt_analytics.pt_csv_raw_*.

Exposes: detect_csv_format(), get_csv_header_count(), csv_to_ndjson_temp(), load_csv_file_to_bigquery().
Each CSV row becomes one row: source_file_name, ingested_at, raw (dict with
_meta_format_hint, _meta_row_number, _meta_header_column_count).
Uses encoding detection (chardet), delimiter sniffing (csv.Sniffer), BigQuery
Load Jobs (NDJSON) only.

CLI (unchanged):
  python ingestion/30_load_csv_examples.py [tall.csv] [wide.csv]
"""
import csv
import json
import os
import sys
import tempfile
from datetime import datetime, timezone

import chardet
from google.cloud import bigquery

DATASET = "pt_analytics"
TABLE = "pt_csv_raw"
PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT")
SAMPLE_BYTES = 100 * 1024  # ~100KB for encoding/delimiter detection

# TALL/WIDE by header count only (row 3 authoritative)
TALL_MAX_COLS = 35
TALL_PAD = 2
WIDE_MIN_COLS = TALL_MAX_COLS + TALL_PAD + 1  # 38

# For logging only; must NOT force WIDE
WIDE_TOKENS = set((
    "negotiated_dollar", "negotiated_percentage", "gross_charge", "discounted_cash",
    "payer_specific", "contracting_method",
))

FILE_CONFIG: list[tuple[str, str]] = [
    (os.path.join(os.path.dirname(__file__), "..", "samples", "sample_tall.csv"), "TALL"),
    (os.path.join(os.path.dirname(__file__), "..", "samples", "sample_wide.csv"), "WIDE"),
]


DELIMITER_CANDIDATES = (",", "|", "\t", ";")
SNIFFER_DELIMITERS = ",|\t;"


def _choose_delimiter_by_preamble(file_path: str) -> tuple[str, str]:
    """Return (encoding, delimiter) where delimiter maximizes row1 column count. Used when header_count > 200 or validation fails."""
    with open(file_path, "rb") as f:
        raw = f.read(SAMPLE_BYTES)
    detected = chardet.detect(raw)
    encoding = detected.get("encoding") or "utf-8"
    if encoding.lower() in ("ascii", "utf-8", "utf8"):
        encoding = "utf-8"
    sample = raw.decode(encoding, errors="replace")
    lines = sample.splitlines()
    line0 = lines[0] if lines else ""
    best_delimiter = ","
    best_count = 0
    for d in DELIMITER_CANDIDATES:
        row1 = next(csv.reader([line0], delimiter=d), [])
        if len(row1) > best_count:
            best_count = len(row1)
            best_delimiter = d
    return encoding, best_delimiter


def _detect_encoding_and_delimiter(file_path: str) -> tuple[str, str]:
    """Detect encoding with chardet; choose delimiter via Sniffer (or heuristic), then validate preamble."""
    with open(file_path, "rb") as f:
        raw = f.read(SAMPLE_BYTES)
    detected = chardet.detect(raw)
    encoding = detected.get("encoding") or "utf-8"
    if encoding.lower() in ("ascii", "utf-8", "utf8"):
        encoding = "utf-8"
    sample = raw.decode(encoding, errors="replace")
    lines = sample.splitlines()
    # Try Sniffer first
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=SNIFFER_DELIMITERS)
        best_delimiter = dialect.delimiter
    except (csv.Error, Exception):
        # Fallback: use line 3 (headers) or line 0 to maximize column count
        line_for_delimiter = lines[2] if len(lines) > 2 else (lines[0] if lines else "")
        best_delimiter = ","
        best_count = 0
        for d in DELIMITER_CANDIDATES:
            row = next(csv.reader([line_for_delimiter], delimiter=d), [])
            if len(row) > best_count:
                best_count = len(row)
                best_delimiter = d
    # Validation: preamble rows should be wide (row1_cols >= 5, row2_cols >= 5)
    line0 = lines[0] if lines else ""
    line1 = lines[1] if len(lines) > 1 else ""
    row1 = next(csv.reader([line0], delimiter=best_delimiter), [])
    row2 = next(csv.reader([line1], delimiter=best_delimiter), [])
    if len(row1) < 5 or len(row2) < 5:
        best_delimiter = ","
        best_count = 0
        for d in DELIMITER_CANDIDATES:
            r1 = next(csv.reader([line0], delimiter=d), [])
            if len(r1) > best_count:
                best_count = len(r1)
                best_delimiter = d
    return encoding, best_delimiter


def detect_csv_format(file_path: str) -> tuple[str, int]:
    """
    Classify CSV as TALL or WIDE by row 3 header count only (authoritative).
    Returns (format_hint, header_count).
    WIDE iff header_count >= WIDE_MIN_COLS. Token-based wide_hits is for logging only.
    Guardrail: if header_count > 200 OR header_count <= 1, retry delimiter by preamble and re-read.
    """
    encoding, delimiter = _detect_encoding_and_delimiter(file_path)
    with open(file_path, "r", encoding=encoding, newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)
        row1 = next(reader, [])
        row2 = next(reader, [])
        headers = next(reader, [])  # row 3 = actual column headers
    header_count = len(headers)
    while header_count > 200 or header_count <= 1:
        encoding, delimiter = _choose_delimiter_by_preamble(file_path)
        with open(file_path, "r", encoding=encoding, newline="") as f:
            reader = csv.reader(f, delimiter=delimiter)
            row1 = next(reader, [])
            row2 = next(reader, [])
            headers = next(reader, [])
        header_count = len(headers)
    header_joined = " ".join(headers).lower()
    wide_hits = sum(1 for t in WIDE_TOKENS if t in header_joined)  # logging only
    if header_count >= WIDE_MIN_COLS:
        format_hint = "WIDE"
    else:
        format_hint = "TALL"
    print(
        f"[detect_csv_format] delimiter={delimiter!r} encoding={encoding} row1_cols={len(row1)} row2_cols={len(row2)} "
        f"header_count={header_count} format_hint={format_hint}",
        file=sys.stderr,
    )
    return format_hint, header_count


def read_csv_preamble(file_path: str) -> tuple[list[str], list[str]]:
    """Read first two rows (metadata + attestation). Uses _detect_encoding_and_delimiter."""
    encoding, delimiter, row1, row2, _ = read_csv_preamble_and_headers(file_path)
    return (row1, row2)


def read_csv_preamble_and_headers(file_path: str) -> tuple[str, str, list[str], list[str], list[str]]:
    """Return (encoding, delimiter, row1, row2, headers) using existing delimiter/encoding detection."""
    encoding, delimiter = _detect_encoding_and_delimiter(file_path)
    with open(file_path, "r", encoding=encoding, newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)
        row1 = next(reader, [])
        row2 = next(reader, [])
        headers = next(reader, [])  # row 3
    return (encoding, delimiter, row1, row2, headers)


def get_csv_header_count(file_path: str) -> int:
    """Return the number of header columns (row 3 in CMS PT structure)."""
    encoding, delimiter = _detect_encoding_and_delimiter(file_path)
    with open(file_path, "r", encoding=encoding, newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)
        next(reader, None)
        next(reader, None)
        headers = next(reader, [])
    return len(headers)


def csv_to_ndjson_temp(
    file_path: str,
    format_hint: str,
    source_file_name_override: str | None = None,
    encoding: str | None = None,
    delimiter: str | None = None,
    row1: list[str] | None = None,
    row2: list[str] | None = None,
    headers: list[str] | None = None,
) -> tuple[str, int, str]:
    """
    Write CSV rows to a temp NDJSON file. Returns (tmp_ndjson_path, row_count, ingested_at_iso).
    Skips rows 1–2, uses row 3 as headers; no preamble duplication in raw rows.
    If encoding, delimiter, row1, row2, headers are provided, use them; else call read_csv_preamble_and_headers.
    """
    if not os.path.isfile(file_path):
        raise FileNotFoundError(file_path)
    if encoding is None or delimiter is None or row1 is None or row2 is None or headers is None:
        encoding, delimiter, row1, row2, headers = read_csv_preamble_and_headers(file_path)
    ingested_at_iso = datetime.now(tz=timezone.utc).isoformat()
    source_file_name = (
        source_file_name_override if source_file_name_override is not None else os.path.basename(file_path)
    )
    header_count = len(headers)
    tmp = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".ndjson", delete=False)
    tmp_path = tmp.name
    row_count = 0
    with open(file_path, "r", encoding=encoding, newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)
        next(reader, None)  # skip row 1
        next(reader, None)  # skip row 2
        next(reader, None)  # skip row 3 (we already have headers)
        for row_number, row in enumerate(reader, start=4):
            obj = dict(zip(headers, row)) if row else {}
            obj["_meta_format_hint"] = format_hint
            obj["_meta_row_number"] = row_number
            obj["_meta_header_column_count"] = header_count
            out_row = {
                "source_file_name": source_file_name,
                "ingested_at": ingested_at_iso,
                "raw": obj,
            }
            tmp.write(json.dumps(out_row, ensure_ascii=False) + "\n")
            row_count += 1
    tmp.close()
    return tmp_path, row_count, ingested_at_iso


def load_csv_file_to_bigquery(
    file_path: str,
    client: bigquery.Client,
    dataset: str,
    table: str,
    format_hint: str,
    source_file_name_override: str | None = None,
) -> tuple[int, str, str, str, int, str, str, dict]:
    """
    Convert CSV to NDJSON temp file, load to BigQuery via load job (WRITE_APPEND).
    Returns (rows_loaded, ingested_at_iso, encoding, delimiter, header_count, row1_raw, row2_raw, preamble_kv).
    """
    encoding, delimiter, row1, row2, headers = read_csv_preamble_and_headers(file_path)
    preamble_kv = {}
    # Prefer row2 values when row1 looks like labels and row2 has values (CMS PT: row1=labels, row2=data).
    if row2 and len(row2) >= len(row1):
        for i in range(len(row1)):
            if row1[i].strip():
                preamble_kv[row1[i].strip()] = row2[i] if i < len(row2) else ""
    else:
        # Fallback: key-value pairs within row1 (alternating key, value).
        for i in range(0, len(row1) - 1, 2):
            if row1[i].strip():
                preamble_kv[row1[i].strip()] = row1[i + 1]
    row1_raw = json.dumps(row1, ensure_ascii=False)
    row2_raw = json.dumps(row2, ensure_ascii=False)
    header_count = len(headers)
    tmp_path, _, ingested_at_iso = csv_to_ndjson_temp(
        file_path, format_hint, source_file_name_override=source_file_name_override,
        encoding=encoding, delimiter=delimiter, row1=row1, row2=row2, headers=headers,
    )
    try:
        project = client.project
        if not project:
            raise ValueError("BigQuery client must have project set")
        table_id = f"{project}.{dataset}.{table}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        with open(tmp_path, "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()
        rows_loaded = job.output_rows if job.output_rows is not None else 0
        return (rows_loaded, ingested_at_iso, encoding, delimiter, header_count, row1_raw, row2_raw, preamble_kv)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def load_csv_into_raw(
    file_path: str,
    format_hint: str,
    client: bigquery.Client,
    table_id: str,
) -> int:
    """Legacy: load one CSV into a single table (full table_id). Kept for backward compatibility."""
    if not os.path.isfile(file_path):
        print(f"Skipping (not found): {file_path}")
        return 0
    parts = table_id.split(".")
    if len(parts) >= 3:
        dataset, table = parts[-2], parts[-1]
    else:
        dataset, table = DATASET, TABLE
    rows_loaded, *_ = load_csv_file_to_bigquery(file_path, client, dataset, table, format_hint)
    return rows_loaded


def main() -> None:
    if len(sys.argv) >= 3:
        config = [(sys.argv[1], "TALL"), (sys.argv[2], "WIDE")]
    else:
        config = FILE_CONFIG

    client = bigquery.Client(project=PROJECT)
    table_id = f"{client.project}.{DATASET}.{TABLE}"

    for path, hint in config:
        loaded = load_csv_into_raw(path, hint, client, table_id)
        print(f"Loaded {loaded} rows from {path} (_meta_format_hint={hint}) into {table_id}")


if __name__ == "__main__":
    main()
