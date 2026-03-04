"""
Silver build: read Bronze partition for a date, standardize to canonical schema (one row per rate),
write good rows to lake/silver/standard_charges, bad rows to lake/silver/quarantine with reason_code.
Emits DQ metrics (counts, null-rates, quarantine-rate).
"""
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_log = logging.getLogger(__name__)

import pandas as pd

# Add project root for ingestion.storage
import sys
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from ingestion.storage import get_storage

# Canonical Silver columns (one row per rate)
CANONICAL_COLUMNS = [
    "source_file_name",
    "source_format",
    "ingest_date",
    "billing_code",
    "description",
    "rate_type",
    "rate_amount",
    "ingested_at",
    "payer_name",
    "plan_name",
]

# JSON: keys we look for (case-insensitive) to find the list of charge items
_JSON_CHARGE_KEYS = ("standardcharges", "standard_charge_information")

# JSON: per-item rate keys -> Silver rate_type (flat structure)
_JSON_RATE_MAP = [
    ("negotiated_rate", "NEGOTIATED"),
    ("standard_charge", "STANDARD"),
    ("gross_charge", "GROSS"),
    ("cash_discount", "CASH"),
]

# CMS structure: keys inside element["standard_charges"][] -> rate_type (uppercased)
_CMS_RATE_KEYS = [
    "negotiated_rate",
    "gross_charge",
    "discounted_cash_price",
    "minimum",
    "maximum",
]

# CSV: column names (case-insensitive) -> rate_type
_CSV_RATE_COLUMNS = [
    ("negotiated_rate", "NEGOTIATED"),
    ("standard_charge", "STANDARD"),
    ("gross_charge", "GROSS"),
    ("discounted_cash_price", "CASH"),
]


def _coerce_numeric(val: Any) -> Optional[float]:
    if val is None or (isinstance(val, str) and not val.strip()):
        return None
    try:
        f = float(val)
        return f
    except (TypeError, ValueError):
        return None


def parse_rate(x: Any) -> Optional[float]:
    """
    Parse a rate value from int/float or string (handles $, commas, n/a, etc.).
    Returns float or None.
    """
    if x is None:
        return None
    if isinstance(x, (int, float)):
        try:
            return float(x)
        except (TypeError, ValueError):
            return None
    if not isinstance(x, str):
        return None
    s = x.strip()
    if not s or s.lower() in ("n/a", "na", "null", "-"):
        return None
    # Remove $ and commas, strip to first numeric token (support decimals)
    s = s.replace("$", "").replace(",", "").strip()
    match = re.search(r"-?\d+\.?\d*", s)
    if not match:
        return None
    try:
        return float(match.group())
    except (TypeError, ValueError):
        return None


def _find_key_case_insensitive(obj: dict, *candidates: str) -> Any:
    if not isinstance(obj, dict):
        return None
    lower = {k.lower(): k for k in obj}
    for c in candidates:
        key = c.lower()
        if key in lower:
            return obj[lower[key]]
    return None


def _billing_code_from_code_information(code_info: Any) -> Optional[str]:
    """
    Extract billing_code from code_information (dict or list of dicts).
    Tries: billing_code, billingCode, code_information_code, code_value, code, cpt, hcpcs.
    If code_info['code'] is a dict, tries dict.get('value') or dict.get('code').
    """
    if isinstance(code_info, list) and len(code_info) > 0:
        code_info = code_info[0]
    if not isinstance(code_info, dict):
        return None
    keys_to_try = (
        "billing_code", "billingCode", "code_information_code", "code_value",
        "code", "cpt", "hcpcs",
    )
    for key in keys_to_try:
        val = code_info.get(key)
        if val is None:
            continue
        if isinstance(val, dict):
            # Nested: try value or code
            v = val.get("value") or val.get("code")
            if v is not None and str(v).strip():
                return str(v).strip()
            continue
        if str(val).strip():
            return str(val).strip()
    return None


def _extract_rates_from_json_payload(
    payload_json: str,
    source_file_name: str,
    source_format: str,
    ingest_date: str,
    ingested_at: str,
) -> Tuple[Optional[List[dict]], List[dict]]:
    """
    Parse payload_json; if standardCharges or standard_charge_information present, return (rate_rows, quarantine_rows).
    rate_rows is None only when top-level structure is unsupported (caller will add JSON_UNSUPPORTED_SHAPE).
    quarantine_rows are per-element MISSING_CODE / MISSING_RATE from CMS structure.
    """
    def meta() -> dict:
        return {
            "source_file_name": source_file_name,
            "source_format": source_format,
            "ingest_date": ingest_date,
            "ingested_at": ingested_at,
        }

    if not payload_json or not str(payload_json).strip():
        return (None, [])
    try:
        obj = json.loads(payload_json)
    except (json.JSONDecodeError, TypeError):
        return (None, [])
    if not isinstance(obj, dict):
        return (None, [])

    charge_list = _find_key_case_insensitive(obj, "standardCharges", "standard_charge_information")
    if charge_list is None or not isinstance(charge_list, list):
        return (None, [])

    rows: List[dict] = []
    quarantine: List[dict] = []
    for element in charge_list:
        if not isinstance(element, dict):
            continue

        # CMS structure: element has code_information and standard_charges
        code_info = element.get("code_information")
        standard_charges = element.get("standard_charges")
        if code_info is not None and standard_charges is not None:
            billing_code = _billing_code_from_code_information(code_info)
            description = element.get("description")
            description = str(description) if description is not None else ""
            if billing_code is None or not billing_code:
                quarantine.append({
                    **meta(),
                    "billing_code": None,
                    "description": description,
                    "rate_type": None,
                    "rate_amount": None,
                    "reason_code": "MISSING_CODE",
                })
                continue
            if not isinstance(standard_charges, list) or len(standard_charges) == 0:
                quarantine.append({
                    **meta(),
                    "billing_code": billing_code,
                    "description": description,
                    "rate_type": None,
                    "rate_amount": None,
                    "reason_code": "MISSING_RATE",
                })
                continue
            for sub in standard_charges:
                if not isinstance(sub, dict):
                    continue
                # CMS payers_information: list of {payer_name, plan_name, standard_charge_dollar, ...} -> one row per payer
                payers_info = sub.get("payers_information")
                if isinstance(payers_info, list) and len(payers_info) > 0:
                    for p in payers_info:
                        if not isinstance(p, dict):
                            continue
                        amount = parse_rate(p.get("standard_charge_dollar") or p.get("negotiated_rate") or p.get("standard_charge"))
                        if amount is None:
                            continue
                        pn = p.get("payer_name") or p.get("payer")
                        payer_name = (str(pn).strip() or None) if pn is not None else None
                        pln = p.get("plan_name") or p.get("plan")
                        plan_name = (str(pln).strip() or None) if pln is not None else None
                        rows.append({
                            **meta(),
                            "billing_code": str(billing_code),
                            "description": str(description),
                            "rate_type": "NEGOTIATED",
                            "rate_amount": float(amount),
                            "payer_name": payer_name,
                            "plan_name": plan_name,
                        })
                # Sub-level rate keys (minimum, maximum, etc.) – no payer at this level
                payer_name = element.get("payer_name") or element.get("payer")
                payer_name = str(payer_name).strip() if payer_name else None
                plan_name = element.get("plan_name") or element.get("plan")
                plan_name = str(plan_name).strip() if plan_name else None
                for key in _CMS_RATE_KEYS:
                    val = sub.get(key)
                    if val is None:
                        continue
                    amount = parse_rate(val)
                    if amount is None:
                        continue
                    rate_type = str(key).upper()
                    rows.append({
                        **meta(),
                        "billing_code": str(billing_code),
                        "description": str(description),
                        "rate_type": rate_type,
                        "rate_amount": float(amount),
                        "payer_name": payer_name,
                        "plan_name": plan_name,
                    })
            continue

        # Existing flat structure (JSON negotiated rates: include payer/plan when present)
        billing_code = element.get("code") or element.get("billing_code")
        billing_code = str(billing_code).strip() if billing_code is not None else ""
        description = element.get("description")
        description = str(description) if description is not None else ""
        payer_name = element.get("payer_name") or element.get("payer")
        payer_name = str(payer_name).strip() if payer_name else None
        plan_name = element.get("plan_name") or element.get("plan")
        plan_name = str(plan_name).strip() if plan_name else None

        for key, rate_type in _JSON_RATE_MAP:
            val = element.get(key)
            if val is None:
                continue
            amount = parse_rate(val)
            if amount is None:
                continue
            rows.append({
                **meta(),
                "billing_code": str(billing_code),
                "description": str(description),
                "rate_type": str(rate_type),
                "rate_amount": float(amount),
                "payer_name": payer_name,
                "plan_name": plan_name,
            })

    return (rows, quarantine)


def _get_tabular_rate_columns(row_dict: dict) -> List[tuple]:
    """Return [(col_name, rate_type), ...] for columns that exist and have a value."""
    out: List[tuple] = []
    keys = list(row_dict.keys()) if isinstance(row_dict, dict) else []
    key_lower = {k.lower(): k for k in keys}
    for col_candidate, rate_type in _CSV_RATE_COLUMNS:
        c = col_candidate.lower()
        if c in key_lower:
            name = key_lower[c]
            val = row_dict.get(name)
            if val is not None and (not isinstance(val, str) or str(val).strip() != ""):
                out.append((name, rate_type))
    return out


def _extract_rates_from_tabular_row(
    row_dict: dict,
    source_file_name: str,
    source_format: str,
    ingest_date: str,
    ingested_at: str,
) -> List[dict]:
    """
    Assume row is tabular. Find billing_code; find rate columns; emit one row per rate.
    Rows with missing billing_code or no rate will be validated later (quarantine).
    """
    def _get(*keys: str, default: str = "") -> str:
        for k in keys:
            v = row_dict.get(k)
            if v is not None and str(v).strip() != "":
                return str(v).strip()
        return default

    billing_code = _get("billing_code", "code", "Billing Code") or ""
    if not billing_code and "billing_code" in row_dict:
        billing_code = str(row_dict["billing_code"] or "")
    if not billing_code and "code" in row_dict:
        billing_code = str(row_dict["code"] or "")
    description = str(row_dict.get("description") or row_dict.get("Description") or "")
    _pn = _get("payer_name", "payer", "Payer Name", "Payer")
    _pln = _get("plan_name", "plan", "Plan Name", "Plan")
    payer_name = _pn if _pn else None
    plan_name = _pln if _pln else None

    rate_cols = _get_tabular_rate_columns(row_dict)
    rows: List[dict] = []
    for col_name, rate_type in rate_cols:
        val = row_dict.get(col_name)
        amount = _coerce_numeric(val)
        if amount is None:
            continue
        rows.append({
            "source_file_name": source_file_name,
            "source_format": source_format,
            "ingest_date": ingest_date,
            "billing_code": billing_code,
            "description": description,
            "rate_type": rate_type,
            "rate_amount": amount,
            "ingested_at": ingested_at,
            "payer_name": payer_name,
            "plan_name": plan_name,
        })
    return rows


def _validate_rate_row(row: dict) -> Optional[str]:
    """Return reason_code if row is bad, else None. Good = billing_code present, rate_amount not null, rate_amount >= 0."""
    if not row.get("source_file_name"):
        return "missing_source_file_name"
    bc = row.get("billing_code")
    if bc is None or (isinstance(bc, str) and not bc.strip()):
        return "MISSING_CODE"
    rate = row.get("rate_amount")
    if rate is None:
        return "MISSING_RATE"
    try:
        f = float(rate)
    except (TypeError, ValueError):
        return "BAD_RATE_VALUE"
    if f < 0:
        return "BAD_RATE_VALUE"
    return None


def read_bronze_partition(storage, ingest_date: str) -> pd.DataFrame:
    """Read all Parquet under lake/bronze/*/ingest_date=... for given date."""
    import io
    all_dfs: List[pd.DataFrame] = []
    for prefix in ["lake/bronze/pt_csv", "lake/bronze/pt_json"]:
        part = f"{prefix}/ingest_date={ingest_date}"
        files = storage.list_files(part)
        for f in files:
            if not f.endswith(".parquet"):
                continue
            try:
                if hasattr(storage, "base") and getattr(storage, "base", None):
                    full_path = Path(storage.base) / f.replace("/", os.sep)
                    if full_path.exists():
                        df = pd.read_parquet(full_path)
                    else:
                        continue
                else:
                    buf = io.BytesIO(storage.read_bytes(f))
                    df = pd.read_parquet(buf)
                df["_bronze_path"] = f
                df["_source_system"] = "csv_tall" if "pt_csv" in prefix else "json"
                all_dfs.append(df)
            except Exception:
                continue
    if not all_dfs:
        return pd.DataFrame()
    return pd.concat(all_dfs, ignore_index=True)


def build_silver_for_date(
    ingest_date: str,
    base_dir: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Read Bronze for ingest_date, expand to one row per rate, split good/quarantine, write Silver; return DQ metrics.
    """
    base_dir = base_dir or "."
    storage = get_storage(base_dir=base_dir)
    ingested_at = f"{ingest_date}T00:00:00"

    df_bronze = read_bronze_partition(storage, ingest_date)
    if df_bronze.empty:
        return {
            "ingest_date": ingest_date,
            "total_rows": 0,
            "good_rows": 0,
            "quarantine_rows": 0,
            "quarantine_rate": 0.0,
            "null_rates": {},
            "errors": ["no bronze data for date"],
        }

    # Temporary: bronze row counts by source
    bronze_csv = int((df_bronze["_source_system"] == "csv_tall").sum()) if "_source_system" in df_bronze.columns else 0
    bronze_json = int((df_bronze["_source_system"] == "json").sum()) if "_source_system" in df_bronze.columns else 0
    print(f"[Silver] bronze rows: pt_csv={bronze_csv}, pt_json={bronze_json}")

    good_rows: List[dict] = []
    quarantine_rows: List[dict] = []
    good_from_json = 0
    good_from_csv = 0

    source_file_col = "source_path" if "source_path" in df_bronze.columns else "source_file_name"
    if source_file_col not in df_bronze.columns:
        source_file_col = df_bronze.columns[0] if len(df_bronze.columns) else "source_path"
    source_format_col = "source_format" if "source_format" in df_bronze.columns else None
    ingest_date_col = "ingest_date" if "ingest_date" in df_bronze.columns else None

    for _, row in df_bronze.iterrows():
        row_dict = row.to_dict()
        src_file = str(row_dict.get(source_file_col, ""))
        src_fmt = str(row_dict.get(source_format_col, row_dict.get("_source_system", "json"))) if source_format_col else str(row_dict.get("_source_system", "json"))
        ingest_d = str(row_dict.get(ingest_date_col, ingest_date)) if ingest_date_col else ingest_date

        payload = row_dict.get("payload_json")
        # CSV rows have no payload_json; concat with JSON can leave NaN, which must not be treated as JSON
        has_json_payload = (
            payload is not None
            and not (isinstance(payload, float) and pd.isna(payload))
            and bool(str(payload).strip())
        )
        if has_json_payload:
            rate_rows, payload_quarantine = _extract_rates_from_json_payload(
                str(payload), src_file, src_fmt, ingest_d, ingested_at
            )
            if rate_rows is None:
                quarantine_rows.append({
                    "source_file_name": src_file,
                    "source_format": src_fmt,
                    "ingest_date": ingest_d,
                    "billing_code": None,
                    "description": None,
                    "rate_type": None,
                    "rate_amount": None,
                    "ingested_at": ingested_at,
                    "reason_code": "JSON_UNSUPPORTED_SHAPE",
                })
                continue
            quarantine_rows.extend(payload_quarantine)
            for r in rate_rows:
                reason = _validate_rate_row(r)
                if reason:
                    r["reason_code"] = reason
                    quarantine_rows.append(r)
                else:
                    good_rows.append(r)
                    good_from_json += 1
        else:
            rate_rows = _extract_rates_from_tabular_row(
                row_dict, src_file, src_fmt, ingest_d, ingested_at
            )
            if not rate_rows:
                quarantine_rows.append({
                    "source_file_name": src_file,
                    "source_format": src_fmt,
                    "ingest_date": ingest_d,
                    "billing_code": row_dict.get("billing_code") or row_dict.get("code"),
                    "description": row_dict.get("description"),
                    "rate_type": None,
                    "rate_amount": None,
                    "ingested_at": ingested_at,
                    "reason_code": "MISSING_RATE",
                })
                continue
            for r in rate_rows:
                reason = _validate_rate_row(r)
                if reason:
                    r["reason_code"] = reason
                    quarantine_rows.append(r)
                else:
                    good_rows.append(r)
                    good_from_csv += 1

    print(f"[Silver] silver good rows by branch: JSON={good_from_json}, CSV/tabular={good_from_csv}")

    # Quarantine reason_code counts for CSV/tabular rows (diagnostics for CSV branch yielding 0)
    csv_quarantine = [r for r in quarantine_rows if (str(r.get("source_format") or "").lower()) in ("csv", "csv_tall")]
    if csv_quarantine:
        from collections import Counter
        reason_counts = Counter(r.get("reason_code") or "unknown" for r in csv_quarantine)
        print(f"[Silver] CSV/tabular quarantine reason_code counts: {dict(reason_counts)}")

    # Diagnostics: candidate rate rows (before split) – rows that had rate_type/rate_amount from extraction
    candidate_rate_rows = good_rows + [r for r in quarantine_rows if r.get("rate_type") is not None]
    if candidate_rate_rows:
        n_c = len(candidate_rate_rows)
        null_bc = sum(1 for r in candidate_rate_rows if r.get("billing_code") is None or (isinstance(r.get("billing_code"), str) and not r.get("billing_code", "").strip()))
        null_amt = sum(1 for r in candidate_rate_rows if r.get("rate_amount") is None)
        pct_null_bc = round(100.0 * null_bc / n_c, 2)
        pct_null_amt = round(100.0 * null_amt / n_c, 2)
        _log.info("Silver candidates: n=%s, %% null billing_code=%s, %% null rate_amount=%s", n_c, pct_null_bc, pct_null_amt)
        print(f"[Silver] candidates n={n_c}, % null billing_code={pct_null_bc}, % null rate_amount={pct_null_amt}")
        rate_types = pd.Series([r.get("rate_type") for r in candidate_rate_rows]).value_counts()
        top_rates = rate_types.head(10).to_dict()
        _log.info("Silver rate_type sample (top 10): %s", top_rates)
        print(f"[Silver] rate_type sample (top 10): {top_rates}")

    total = len(good_rows) + len(quarantine_rows)
    quarantine_rate = len(quarantine_rows) / total if total else 0.0

    if good_rows:
        df_good = pd.DataFrame(good_rows)
        for c in CANONICAL_COLUMNS:
            if c not in df_good.columns:
                df_good[c] = None
        df_good = df_good[[c for c in CANONICAL_COLUMNS if c in df_good.columns]]
        out_good = f"lake/silver/standard_charges/ingest_date={ingest_date}/data.parquet"
        storage.write_parquet(df_good, out_good)

    if quarantine_rows:
        df_q = pd.DataFrame(quarantine_rows)
        out_q = f"lake/silver/quarantine/ingest_date={ingest_date}/data.parquet"
        storage.write_parquet(df_q, out_q)

    null_rates: Dict[str, float] = {}
    if good_rows:
        df_g = pd.DataFrame(good_rows)
        n = len(df_g)
        for col in ["billing_code", "rate_amount"]:
            if col in df_g.columns:
                null_rates[col] = round(float(df_g[col].isna().sum()) / n, 4)

    return {
        "ingest_date": ingest_date,
        "total_rows": total,
        "good_rows": len(good_rows),
        "quarantine_rows": len(quarantine_rows),
        "quarantine_rate": round(quarantine_rate, 4),
        "null_rates": null_rates,
        "errors": [],
    }
