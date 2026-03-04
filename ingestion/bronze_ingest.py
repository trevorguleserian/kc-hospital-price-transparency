"""
Bronze ingestion: read raw CSV/JSON from local data/raw_drop or GCS, write Parquet to lake/bronze.
Idempotent by file_hash; manifest in DuckDB (local) or optional cloud manifest.
Supports NDJSON, single JSON object, and JSON array; preserves raw payload; Silver standardizes.
"""
import hashlib
import json
import os
import re
import traceback
from datetime import datetime
import csv
from io import BytesIO, StringIO
from pathlib import Path
from typing import List, Literal, Optional, Tuple

import pandas as pd

from ingestion.storage import get_storage

# Max array length to normalize; beyond this we store as payload_json. Configurable via PT_JSON_NORMALIZE_MAX_ROWS (default 500000).
_JSON_NORMALIZE_MAX_ROWS = int(os.getenv("PT_JSON_NORMALIZE_MAX_ROWS", "500000"))
# Batch size for chunked json_normalize when array is large (avoids one huge normalize call)
_JSON_NORMALIZE_CHUNK_SIZE = 500_000
# Preview length in error messages (sanitized)
_ERROR_PREVIEW_LEN = 200

# Manifest status
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_PENDING = "PENDING"


def file_hash_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _decode_json_bytes(data: bytes) -> str:
    """Decode file bytes to text: utf-8-sig (BOM), then utf-8, then latin-1. Strip; empty -> ValueError."""
    text = None
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text = data.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        raise ValueError("Could not decode file as utf-8-sig, utf-8, or latin-1")
    text = text.strip()
    if not text:
        raise ValueError("Empty JSON file")
    return text


def _looks_like_ndjson(text: str) -> bool:
    """True if content appears to be NDJSON: multiple lines each starting with '{' and ending with '}'."""
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    if len(lines) < 2:
        return False
    # At least two lines that look like complete JSON objects (single line each)
    count = 0
    for ln in lines:
        if ln.startswith("{") and ln.endswith("}"):
            count += 1
            if count >= 2:
                return True
    return False


def _sanitize_preview(text: str, max_len: int = _ERROR_PREVIEW_LEN) -> str:
    """Return a safe preview for error messages: strip control chars, truncate."""
    if not text:
        return ""
    # Replace control characters and excessive whitespace for safe logging
    sanitized = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", " ", text[: max_len * 2])
    sanitized = re.sub(r"\s+", " ", sanitized).strip()
    return (sanitized[:max_len] + "...") if len(sanitized) > max_len else sanitized


def _bronze_metadata_columns(path: str, fhash: str, ingest_date: str, source_format: str) -> dict:
    """Standard metadata columns for every Bronze row."""
    return {
        "source_path": path,
        "file_hash": fhash,
        "ingest_ts": datetime.utcnow().isoformat() + "Z",
        "ingest_date": ingest_date,
        "source_format": source_format,
    }


def _parse_json_to_dataframe(
    text: str,
    path: str,
    fhash: str,
    ingest_date: str,
) -> pd.DataFrame:
    """
    Parse JSON text into a DataFrame. Tries NDJSON first; else single object or array.
    Always adds Bronze metadata: source_path, file_hash, ingest_ts, ingest_date, source_format.
    """
    meta = _bronze_metadata_columns(path, fhash, ingest_date, "json")

    # 1) Try NDJSON
    if _looks_like_ndjson(text):
        try:
            df = pd.read_json(StringIO(text), lines=True, dtype=False)
            if not df.empty:
                for k, v in meta.items():
                    df[k] = v
                return df
        except Exception:
            pass  # Fall through to standard JSON

    # 2) Standard JSON
    try:
        obj = json.loads(text)
    except json.JSONDecodeError as e:
        preview = _sanitize_preview(text, _ERROR_PREVIEW_LEN)
        raise ValueError(f"Invalid JSON: {e}. File preview: {preview!r}")

    # 3) Single object -> one row with payload_json
    if isinstance(obj, dict):
        row = {**meta, "payload_json": json.dumps(obj)}
        return pd.DataFrame([row])

    # 4) Array
    if isinstance(obj, list):
        if not obj:
            return pd.DataFrame([{**meta, "payload_json": "[]"}])
        allow_normalize = len(obj) <= _JSON_NORMALIZE_MAX_ROWS
        if allow_normalize and all(isinstance(x, dict) for x in obj):
            try:
                if len(obj) <= _JSON_NORMALIZE_CHUNK_SIZE:
                    df = pd.json_normalize(obj)
                else:
                    # Chunked normalize to avoid one huge json_normalize call
                    parts: List[pd.DataFrame] = []
                    for i in range(0, len(obj), _JSON_NORMALIZE_CHUNK_SIZE):
                        chunk = obj[i : i + _JSON_NORMALIZE_CHUNK_SIZE]
                        parts.append(pd.json_normalize(chunk))
                    df = pd.concat(parts, axis=0, ignore_index=True)
                for k, v in meta.items():
                    df[k] = v
                return df
            except Exception:
                pass
        # Over limit or not list-of-dicts or normalize failed -> one row with full payload
        row = {**meta, "payload_json": json.dumps(obj)}
        return pd.DataFrame([row])

    # 5) Other (number, string, etc.)
    row = {**meta, "payload_json": json.dumps(obj)}
    return pd.DataFrame([row])


def _raw_drop_local(base_dir: str) -> Path:
    """Local raw drop directory: RAW_DROP_DIR env, or data/sample if SAMPLE_DATA=1, else data/raw_drop."""
    base = Path(base_dir or ".").resolve()
    raw_env = os.environ.get("RAW_DROP_DIR", "").strip()
    if raw_env:
        p = Path(raw_env)
        return (base / p) if not p.is_absolute() else p
    if os.environ.get("SAMPLE_DATA", "").strip() in ("1", "true", "yes"):
        return base / "data" / "sample"
    return base / "data" / "raw_drop"


# Column names (case-insensitive) that indicate a charge-table CSV vs preamble
_CSV_CODE_COLUMNS = ("billing_code", "code", "cpt", "hcpcs", "billing code", "service code")
_CSV_RATE_COLUMNS = (
    "negotiated_rate", "gross_charge", "discounted_cash_price", "standard_charge",
    "cash_price", "minimum", "maximum", "negotiated rate", "gross charge",
)

# Canonical column names Silver tabular extractor expects (must match silver_build._CSV_RATE_COLUMNS / row keys)
_CANONICAL_CSV_COLUMNS = (
    "billing_code", "description", "gross_charge", "discounted_cash_price",
    "negotiated_rate", "payer_name", "plan_name",
)
# Preferred source column names (normalized) for each canonical
_CANONICAL_CSV_SOURCES = {
    "billing_code": ("billing_code", "code|1", "code"),
    "description": ("description",),
    "gross_charge": ("standard_charge|gross",),
    "discounted_cash_price": ("standard_charge|discounted_cash",),
    "negotiated_rate": ("standard_charge|negotiated_dollar",),  # fallback: col with 'negotiated' and 'dollar'
    "payer_name": ("payer_name",),
    "plan_name": ("plan_name",),
}

# Header detection: score +2 per code-like and +2 per rate-like column name (substring match, case-insensitive)
_CSV_HEADER_SNIFF_LINES = 15
_CSV_HEADER_CANDIDATES = (0, 1, 2, 3, 4)
_CSV_CODE_LIKE = ("billing_code", "code", "cpt", "hcpcs", "revenue", "ndc", "msdrg", "drg")
_CSV_RATE_LIKE = ("negotiated", "gross", "cash", "min", "max", "standard_charge", "rate", "charge", "price")


def _normalize_col(c: str) -> str:
    """Normalize column name for matching: lower, strip, replace spaces with underscore."""
    if c is None:
        return ""
    return str(c).strip().lower().replace(" ", "_")


def _build_canonical_csv_map(df: pd.DataFrame) -> dict:
    """Build mapping: canonical_col -> original_column_name (or None). Uses normalized names."""
    norm_to_orig: dict = {}
    for col in df.columns:
        n = _normalize_col(col)
        if n and n not in norm_to_orig:
            norm_to_orig[n] = col

    out: dict = {}
    for can in _CANONICAL_CSV_COLUMNS:
        out[can] = None
        for candidate in _CANONICAL_CSV_SOURCES.get(can, ()):
            if candidate in norm_to_orig:
                out[can] = norm_to_orig[candidate]
                break
        if out[can] is None and can == "negotiated_rate":
            for orig in df.columns:
                norm = _normalize_col(orig)
                if "negotiated" in norm and "dollar" in norm:
                    out[can] = orig
                    break
    return out


def _has_value(series: pd.Series) -> pd.Series:
    """True where value is non-null and non-empty string."""
    return series.notna() & series.astype(str).str.strip().str.len().gt(0)


def _map_csv_to_canonical_and_filter(
    df: pd.DataFrame, path: str, fhash: str, ingest_date: str
) -> pd.DataFrame:
    """
    Map CSV columns to canonical names (billing_code, description, gross_charge, etc.),
    keep only charge rows (billing_code present and at least one rate present), add meta.
    """
    meta = _bronze_metadata_columns(path, fhash, ingest_date, "csv")
    if df.empty:
        return pd.DataFrame([meta])

    col_map = _build_canonical_csv_map(df)
    result = {}
    for can in _CANONICAL_CSV_COLUMNS:
        orig = col_map.get(can)
        result[can] = df[orig].astype(str) if orig and orig in df.columns else pd.Series([""] * len(df))

    out_df = pd.DataFrame(result)
    for k, v in meta.items():
        out_df[k] = v

    # Filter: keep only rows where billing_code is non-empty and at least one rate is non-empty
    bc_ok = _has_value(out_df["billing_code"])
    rate_ok = (
        _has_value(out_df["gross_charge"])
        | _has_value(out_df["discounted_cash_price"])
        | _has_value(out_df["negotiated_rate"])
    )
    out_df = out_df.loc[bc_ok & rate_ok].copy()
    if out_df.empty:
        out_df = pd.DataFrame([{**meta, **{c: "" for c in _CANONICAL_CSV_COLUMNS}}])
    return out_df


def _decode_csv_bytes(data: bytes) -> str:
    """Decode CSV file bytes to text (utf-8-sig, utf-8, latin-1)."""
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return data.decode(encoding).strip()
        except UnicodeDecodeError:
            continue
    raise ValueError("Could not decode CSV as utf-8-sig, utf-8, or latin-1")


def _parse_csv_header_line(line: str) -> List[str]:
    """Parse a single line as CSV header; return list of column names (stripped)."""
    if not line.strip():
        return []
    try:
        row = next(csv.reader(StringIO(line)))
        return [str(c).strip() for c in row]
    except Exception:
        return [c.strip() for c in line.split(",")]


def _score_header_columns(columns: List[str]) -> int:
    """Score column list: +2 per code-like, +2 per rate-like (case-insensitive substring)."""
    code_set = {c.lower() for c in _CSV_CODE_LIKE}
    rate_set = {c.lower() for c in _CSV_RATE_LIKE}
    score = 0
    for col in columns:
        c = (col or "").lower()
        if not c:
            continue
        for kw in code_set:
            if kw in c:
                score += 2
                break
        for kw in rate_set:
            if kw in c:
                score += 2
                break
    return score


def _detect_csv_header_row(lines: List[str]) -> Tuple[int, int, List[str]]:
    """
    Sniff first N lines; try candidate header rows 0..4. Return (best_row_index, score, column_names).
    Require score >= 2 else fallback to row 0.
    """
    if not lines:
        return (0, 0, [])
    sniff = lines[:_CSV_HEADER_SNIFF_LINES]
    best_row, best_score, best_cols = 0, 0, _parse_csv_header_line(sniff[0]) if sniff else []
    for cand in _CSV_HEADER_CANDIDATES:
        if cand >= len(sniff):
            break
        cols = _parse_csv_header_line(sniff[cand])
        if not cols:
            continue
        score = _score_header_columns(cols)
        if score > best_score:
            best_row, best_score, best_cols = cand, score, cols
    if best_score < 2:
        best_row, best_score, best_cols = 0, _score_header_columns(_parse_csv_header_line(sniff[0])) if sniff else 0, _parse_csv_header_line(sniff[0]) if sniff else []
    return (best_row, best_score, best_cols)


def _scan_csv_headers(raw_drop: Path) -> List[dict]:
    """Scan CSVs under raw_drop; use header detection; substring-based code/rate detection."""
    out: List[dict] = []
    if not raw_drop.exists():
        return out
    code_substrings = ("code", "cpt", "hcpcs", "revenue", "ndc", "msdrg", "drg", "billing")
    rate_substrings = ("gross", "cash", "negotiated", "min", "max", "standard_charge", "rate", "charge", "price")
    for f in sorted(raw_drop.rglob("*.csv")):
        if not f.is_file():
            continue
        try:
            with open(f, "r", encoding="utf-8-sig", errors="replace") as fp:
                sniff_lines = [fp.readline() for _ in range(_CSV_HEADER_SNIFF_LINES)]
            header_row, score, columns = _detect_csv_header_row(sniff_lines)
            has_code = False
            has_rate = False
            for col in columns:
                norm = _normalize_col(col)
                if not norm:
                    continue
                for kw in code_substrings:
                    if kw in norm:
                        has_code = True
                        break
                for kw in rate_substrings:
                    if kw in norm:
                        has_rate = True
                        break
            out.append({
                "path": str(f.relative_to(raw_drop) if raw_drop != f else f.name),
                "columns": columns,
                "header_row": header_row,
                "header_score": score,
                "has_code_col": has_code,
                "has_rate_col": has_rate,
                "looks_like_charge_table": has_code and has_rate,
            })
        except Exception:
            out.append({
                "path": str(f.relative_to(raw_drop) if raw_drop != f else f.name),
                "columns": [],
                "header_row": 0,
                "header_score": 0,
                "has_code_col": False,
                "has_rate_col": False,
                "looks_like_charge_table": False,
            })
    return out


def _print_csv_raw_drop_diagnostics(base_dir: str) -> None:
    """Print CSVs in data/raw_drop: header row selected, score, top 20 columns (local only)."""
    raw = _raw_drop_local(base_dir)
    rows = _scan_csv_headers(raw)
    if not rows:
        return
    print("[Bronze] raw_drop CSV diagnostics (local):")
    for r in rows:
        code_ok = "yes" if r["has_code_col"] else "no"
        rate_ok = "yes" if r["has_rate_col"] else "no"
        charge_ok = "CHARGE TABLE" if r["looks_like_charge_table"] else "preamble/other"
        header_row = r.get("header_row", 0)
        score = r.get("header_score", 0)
        cols = r.get("columns") or []
        top20 = ", ".join(cols[:20])
        if len(cols) > 20:
            top20 += ", ..."
        print(f"  {r['path']}")
        print(f"    header_row={header_row} (1-based: {header_row + 1}) score={score}")
        print(f"    columns({len(cols)}): {top20}")
        print(f"    code_col={code_ok} rate_col={rate_ok} => {charge_ok}")


def list_raw_files_local(base_dir: str) -> List[Tuple[str, str]]:
    """List (path_for_storage, format) for data/raw_drop; path is relative to base_dir for LocalStorage."""
    raw = _raw_drop_local(base_dir)
    base = Path(base_dir or ".").resolve()
    out: List[Tuple[str, str]] = []
    if not raw.exists():
        return out
    for f in raw.rglob("*"):
        if not f.is_file():
            continue
        suf = f.suffix.lower()
        rel = str(f.relative_to(base)).replace("\\", "/")
        if suf == ".csv":
            out.append((rel, "csv"))
        elif suf == ".json":
            out.append((rel, "json"))
    return out


def list_raw_files_gcs(storage) -> List[Tuple[str, str]]:
    """List (key, format) under GCS raw drop prefix (e.g. pt_landing/raw_drop)."""
    prefix = getattr(storage, "prefix", "") or ""
    raw_prefix = f"{prefix}/raw_drop".strip("/") or "raw_drop"
    keys = storage.list_files(raw_prefix)
    out: List[Tuple[str, str]] = []
    for k in keys:
        if k.endswith(".csv"):
            out.append((k, "csv"))
        elif k.endswith(".json"):
            out.append((k, "json"))
    return out


def ingest_one_file(
    storage,
    path: str,
    fmt: Literal["csv", "json"],
    ingest_date: str,
    manifest_callback=None,
) -> Tuple[str, Optional[str]]:
    """
    Read file, compute file_hash, write Parquet to lake/bronze/pt_{csv|json}/ingest_date=YYYY-MM-DD/.
    Returns (file_hash, error_message or None).
    """
    try:
        data = storage.read_bytes(path)
    except Exception as e:
        err_msg = f"{path} read failed: {e}\n{traceback.format_exc()}"
        if manifest_callback:
            manifest_callback(path, None, STATUS_FAILED, str(e))
        return (hashlib.sha256(b"").hexdigest(), err_msg)

    fhash = file_hash_sha256(data)
    lake_prefix = "lake/bronze/pt_csv" if fmt == "csv" else "lake/bronze/pt_json"
    partition = f"{lake_prefix}/ingest_date={ingest_date}"
    # One parquet per file: partition/file_hash.parquet (or safe filename)
    safe_name = fhash[:16] + ".parquet"
    out_path = f"{partition}/{safe_name}"

    try:
        if fmt == "csv":
            text = _decode_csv_bytes(data)
            lines = text.splitlines()
            try:
                header_row, _score, _cols = _detect_csv_header_row(lines)
            except Exception:
                header_row = 0
            df = pd.read_csv(StringIO(text), dtype=str, on_bad_lines="skip", header=header_row)
            df = _map_csv_to_canonical_and_filter(df, path, fhash, ingest_date)
        else:
            text = _decode_json_bytes(data)
            df = _parse_json_to_dataframe(text, path, fhash, ingest_date)
            if df.empty:
                meta = _bronze_metadata_columns(path, fhash, ingest_date, "json")
                df = pd.DataFrame([{**meta, "payload_json": ""}])
        storage.write_parquet(df, out_path)
        if manifest_callback:
            manifest_callback(path, fhash, STATUS_SUCCESS, None)
        return (fhash, None)
    except Exception as e:
        preview = ""
        if fmt == "json":
            try:
                raw = _decode_json_bytes(data)
                preview = f" File preview: {_sanitize_preview(raw, _ERROR_PREVIEW_LEN)!r}"
            except Exception:
                preview = " (decode failed for preview)"
        err_msg = f"{path} ({fmt}) failed: {e}.{preview}\n{traceback.format_exc()}"
        if manifest_callback:
            manifest_callback(path, fhash, STATUS_FAILED, str(e))
        return (fhash, err_msg)


def get_manifest_path_local(base_dir: str) -> Path:
    return Path(base_dir or ".").resolve() / "warehouse" / "duckdb" / "file_manifest.duckdb"


def ensure_manifest_table_duckdb(duckdb_path: str) -> None:
    import duckdb
    Path(duckdb_path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(duckdb_path)
    con.execute("""
        CREATE TABLE IF NOT EXISTS file_manifest (
            source_path VARCHAR,
            file_hash VARCHAR,
            status VARCHAR,
            ingest_date VARCHAR,
            updated_at TIMESTAMP,
            error_message VARCHAR
        )
    """)
    con.close()


def manifest_upsert_duckdb(duckdb_path: str, source_path: str, file_hash: Optional[str], status: str, error_message: Optional[str], ingest_date: str) -> None:
    import duckdb
    con = duckdb.connect(duckdb_path)
    con.execute(
        """
        INSERT INTO file_manifest (source_path, file_hash, status, ingest_date, updated_at, error_message)
        VALUES (?, ?, ?, ?, current_timestamp, ?)
        """,
        [source_path, file_hash or "", status, ingest_date, error_message or ""],
    )
    con.close()


# Allowed values for force_reingest_sources (source keys used in lake paths)
FORCE_REINGEST_SOURCE_KEYS = ("pt_csv", "pt_json")


def run_bronze_ingest(
    ingest_date: Optional[str] = None,
    base_dir: Optional[str] = None,
    skip_existing_success: bool = True,
    force_reingest: bool = False,
    force_reingest_sources: Optional[List[str]] = None,
) -> dict:
    """
    Main entry: list raw files, skip if file_hash already SUCCESS, else ingest; write manifest.
    If force_reingest=True, ignore the manifest and re-process all files for this ingest_date.
    If force_reingest_sources is set (e.g. ["pt_csv"]), ignore manifest only for those source types.
    Returns counts and any errors.
    """
    backend = os.environ.get("STORAGE_BACKEND", "local").strip().lower()
    ingest_date = ingest_date or datetime.utcnow().strftime("%Y-%m-%d")
    base_dir = base_dir or "."
    force_sources = set(force_reingest_sources or []) & set(FORCE_REINGEST_SOURCE_KEYS)

    storage = get_storage(base_dir=base_dir)
    if backend == "local":
        files = list_raw_files_local(base_dir)
        # CSV diagnostics: report raw_drop CSVs and whether they look like charge tables (local only)
        _print_csv_raw_drop_diagnostics(base_dir)
        manifest_path = str(get_manifest_path_local(base_dir))
        ensure_manifest_table_duckdb(manifest_path)
        # Load existing success hashes (unless we're forcing full reingest)
        existing = set()
        if not force_reingest:
            import duckdb
            con = duckdb.connect(manifest_path, read_only=True)
            try:
                existing = set(
                    row[0] for row in con.execute(
                        "SELECT file_hash FROM file_manifest WHERE status = ?", [STATUS_SUCCESS]
                    ).fetchall()
                )
            except Exception:
                pass
            con.close()
    else:
        files = list_raw_files_gcs(storage)
        existing = set()  # Cloud: no local DuckDB manifest; idempotency by path/date if you add a table
        manifest_path = None

    def manifest_cb(path: str, fhash: Optional[str], status: str, err: Optional[str]) -> None:
        if manifest_path and backend == "local":
            manifest_upsert_duckdb(manifest_path, path, fhash, status, err, ingest_date)

    ingested = 0
    skipped = 0
    failed = 0
    errors: List[str] = []

    for path_for_storage, fmt in files:
        try:
            data = storage.read_bytes(path_for_storage)
        except Exception as e:
            failed += 1
            errors.append(f"{path_for_storage} ({fmt}) failed: {e}\n{traceback.format_exc()}")
            continue
        fhash = file_hash_sha256(data)
        source_key = "pt_csv" if fmt == "csv" else "pt_json"
        skip = (
            skip_existing_success
            and backend == "local"
            and fhash in existing
            and not force_reingest
            and not (force_sources and source_key in force_sources)
        )
        if skip:
            skipped += 1
            continue
        fhash2, err = ingest_one_file(storage, path_for_storage, fmt, ingest_date, manifest_callback=manifest_cb)
        if err:
            failed += 1
            errors.append(f"{path_for_storage} ({fmt}) failed: {err}")
        else:
            ingested += 1

    return {"ingested": ingested, "skipped": skipped, "failed": failed, "errors": errors, "ingest_date": ingest_date}
