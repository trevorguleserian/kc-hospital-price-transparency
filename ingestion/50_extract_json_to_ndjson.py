"""
Stream-extract JSON records to NDJSON using ijson.items() or line-by-line JSONL.
Supports huge files. Handles BOM, invalid leading bytes, and NDJSON vs single-JSON.

Each NDJSON line matches pt_json_extracted_rates schema. Writes line-by-line
(no large in-memory lists). raw_rate is always a dict (object), not a string.

St. Luke's schema (--item_path standard_charge_information.item): dedicated extractor
that expands service -> standard_charges -> payers_information into one row per payer,
with compact raw_rate. Use --max_records for development.

Usage (St. Luke's style):
  python ingestion/50_extract_json_to_ndjson.py --local_json_path "C:\\path\\file.json" --output_ndjson_path "C:\\path\\rates.ndjson" --source_file_name "file.json" --item_path "standard_charge_information.item" [--max_records 1000]

Usage (generic single JSON):
  python ingestion/50_extract_json_to_ndjson.py ... --item_path "reporting_structure.item.in_network_rates.item"

Usage (NDJSON input; --item_path ignored):
  python ingestion/50_extract_json_to_ndjson.py --local_json_path "C:\\path\\lines.jsonl" --output_ndjson_path "C:\\path\\rates.ndjson" --source_file_name "lines.jsonl"
"""
import argparse
import json
import numbers
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal

import ijson

UTF8_BOM = b"\xef\xbb\xbf"


def _json_safe(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    return obj
DEFAULT_MAX_SCAN_BYTES = 1024 * 1024
STLUKES_ITEM_PATH = "standard_charge_information.item"


def find_json_start_offset(file_path: str, max_scan_bytes: int = DEFAULT_MAX_SCAN_BYTES) -> int | None:
    """
    Read up to max_scan_bytes; skip UTF-8 BOM and leading whitespace/control chars;
    return byte offset of first b'{' or b'[', or None if not found.
    """
    with open(file_path, "rb") as f:
        chunk = f.read(max_scan_bytes)
    if not chunk:
        return None
    offset = 0
    if chunk.startswith(UTF8_BOM):
        offset = len(UTF8_BOM)
    while offset < len(chunk):
        b = chunk[offset]
        if b in (ord(b"{"), ord(b"[")):
            return offset
        if b >= 0x20 or b in (ord(b"\t"), ord(b"\n"), ord(b"\r")):
            offset += 1
        else:
            offset += 1
    return None


def _get(obj: dict, *keys: str, default=None):
    """First key that exists in obj (case-sensitive)."""
    for k in keys:
        if k in obj and obj[k] is not None:
            return obj[k]
    return default


def _get_str(obj: dict, *keys: str) -> str | None:
    v = _get(obj, *keys)
    if v is None:
        return None
    return str(v).strip() or None


def _get_num(obj: dict, *keys: str):
    v = _get(obj, *keys)
    if v is None:
        return None
    if isinstance(v, numbers.Number):
        return v
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _parse_num(val) -> float | None:
    """Parse value to JSON number for negotiated_rate; return None if not parseable."""
    if val is None:
        return None
    if isinstance(val, numbers.Number):
        return float(val)
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _extract_row(
    record: dict,
    record_path: str,
    source_file_name: str,
    ingested_at: str,
    run_id: str | None = None,
) -> dict:
    """Map a single JSON object to pt_json_extracted_rates row (generic best-effort). raw_rate = full object."""
    return {
        "source_file_name": source_file_name,
        "run_id": run_id,
        "ingested_at": ingested_at,
        "record_path": record_path,
        "billing_code": _get_str(record, "billing_code", "code", "billingCode", "service_code"),
        "billing_code_type": _get_str(record, "billing_code_type", "code_type", "billing_code_type", "type", "billingCodeType"),
        "description": _get_str(record, "description", "desc", "name", "billing_code_description"),
        "payer": _get_str(record, "payer", "payer_name", "payerName"),
        "plan": _get_str(record, "plan", "plan_name", "planName"),
        "negotiated_rate": _get_num(record, "negotiated_rate", "rate", "negotiatedRate", "price", "billed_charge"),
        "rate_type": _get_str(record, "rate_type", "rate_type", "rateType", "type"),
        "billing_class": _get_str(record, "billing_class", "billing_class", "billingClass", "billing_classification"),
        "payment_methodology": _get_str(record, "payment_methodology", "payment_methodology", "paymentMethodology"),
        "payment_unit": _get_str(record, "payment_unit", "payment_unit", "paymentUnit", "unit"),
        "raw_rate": record,
    }


def _stlukes_extract_rows(
    service_item: dict,
    record_path: str,
    source_file_name: str,
    ingested_at: str,
    run_id: str | None = None,
):
    """
    St. Luke's schema: service_item has code_information[], standard_charges[],
    each sc has payers_information[]. Yield one row per (service, standard_charge, payer).
    raw_rate is compact: { service, standard_charge, payer }.
    """
    description = service_item.get("description")
    if isinstance(description, str):
        description = description.strip() or None
    else:
        description = None

    code_info_list = service_item.get("code_information") or []
    if not isinstance(code_info_list, list) or len(code_info_list) == 0:
        code_info = {}
    else:
        code_info = code_info_list[0] if isinstance(code_info_list[0], dict) else {}
    billing_code = _get_str(code_info, "code")
    billing_code_type = _get_str(code_info, "type")

    service_minimal = {
        "description": description,
        "billing_code": billing_code,
        "billing_code_type": billing_code_type,
    }

    for sc in service_item.get("standard_charges") or []:
        if not isinstance(sc, dict):
            continue
        setting = _get_str(sc, "setting")
        gross_charge = _get_num(sc, "gross_charge")
        discounted_cash = _get_num(sc, "discounted_cash")
        minimum = _get_num(sc, "minimum")
        maximum = _get_num(sc, "maximum")
        standard_charge_minimal = {
            "setting": setting,
            "gross_charge": gross_charge,
            "discounted_cash": discounted_cash,
            "minimum": minimum,
            "maximum": maximum,
        }

        payers = sc.get("payers_information") or []
        if not isinstance(payers, list):
            continue
        for payer in payers:
            if not isinstance(payer, dict):
                continue
            payer_name = _get_str(payer, "payer_name")
            plan_name = _get_str(payer, "plan_name")
            methodology = _get_str(payer, "methodology")
            estimated_amount = _parse_num(payer.get("estimated_amount"))

            payer_minimal = {
                "payer_name": payer_name,
                "plan_name": plan_name,
                "methodology": methodology,
                "estimated_amount": estimated_amount,
            }

            raw_rate = {
                "service": service_minimal,
                "standard_charge": standard_charge_minimal,
                "payer": payer_minimal,
            }

            row = {
                "source_file_name": source_file_name,
                "run_id": run_id,
                "ingested_at": ingested_at,
                "record_path": record_path,
                "billing_code": billing_code,
                "billing_code_type": billing_code_type,
                "description": description,
                "payer": payer_name,
                "plan": plan_name,
                "negotiated_rate": estimated_amount,
                "rate_type": "estimated_amount",
                "billing_class": setting,
                "payment_methodology": methodology,
                "payment_unit": None,
                "raw_rate": raw_rate,
            }
            yield row


def run_extract(
    local_json_path: str,
    output_ndjson_path: str,
    source_file_name: str,
    item_path: str | None = None,
    max_scan_bytes: int = DEFAULT_MAX_SCAN_BYTES,
    max_records: int = 0,
    run_id: str | None = None,
) -> int:
    """
    Extract JSON/NDJSON to NDJSON file. Returns number of rows emitted.
    Raises on error (file not found, or single-JSON without item_path).
    """
    json_path = os.path.abspath(local_json_path)
    if not os.path.isfile(json_path):
        raise FileNotFoundError(f"File not found: {json_path}")

    ingested_at = datetime.now(tz=timezone.utc).isoformat()
    out_path = os.path.abspath(output_ndjson_path)
    parent = os.path.dirname(out_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    offset = find_json_start_offset(json_path, max_scan_bytes=max_scan_bytes)
    count = 0
    max_records_limit = max_records if max_records > 0 else None

    if offset is not None and item_path:
        record_path = item_path
        use_stlukes = record_path == STLUKES_ITEM_PATH

        with open(json_path, "rb") as f_in, open(out_path, "w", encoding="utf-8") as f_out:
            f_in.seek(offset)
            for record in ijson.items(f_in, record_path):
                if not isinstance(record, dict):
                    continue
                if use_stlukes:
                    for row in _stlukes_extract_rows(record, record_path, source_file_name, ingested_at, run_id=run_id):
                        row = _json_safe(row)
                        f_out.write(json.dumps(row, ensure_ascii=False) + "\n")
                        count += 1
                        if max_records_limit is not None and count >= max_records_limit:
                            break
                else:
                    row = _extract_row(record, record_path, source_file_name, ingested_at, run_id=run_id)
                    row = _json_safe(row)
                    f_out.write(json.dumps(row, ensure_ascii=False) + "\n")
                    count += 1
                    if max_records_limit is not None and count >= max_records_limit:
                        break
                if max_records_limit is not None and count >= max_records_limit:
                    break
    elif offset is None:
        record_path = "ndjson_line"
        with open(json_path, "r", encoding="utf-8-sig", errors="replace") as f_in, open(out_path, "w", encoding="utf-8") as f_out:
            for line in f_in:
                line = line.strip()
                if not line or not line.startswith("{"):
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(record, dict):
                    continue
                row = _extract_row(record, record_path, source_file_name, ingested_at, run_id=run_id)
                row = _json_safe(row)
                f_out.write(json.dumps(row, ensure_ascii=False) + "\n")
                count += 1
                if max_records_limit is not None and count >= max_records_limit:
                    break
    else:
        raise ValueError("Single-JSON start found but item_path not provided; provide item_path from profiler.")

    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream-extract JSON array items or NDJSON lines to NDJSON for BigQuery load.")
    parser.add_argument("--local_json_path", required=True, help="Path to the source JSON or NDJSON file")
    parser.add_argument("--output_ndjson_path", required=True, help="Path for output NDJSON file")
    parser.add_argument("--source_file_name", required=True, help="Value for source_file_name column")
    parser.add_argument("--item_path", default=None, help="ijson item path (from profiler); required for single-JSON, ignored for NDJSON")
    parser.add_argument("--max-scan-bytes", type=int, default=DEFAULT_MAX_SCAN_BYTES, help="Bytes to scan for JSON start (default 1MB)")
    parser.add_argument("--max_records", type=int, default=0, help="Max rows to emit (0 = unlimited); useful for development")
    parser.add_argument("--run_id", default=None, help="Bulk run id to store in each row (optional)")
    args = parser.parse_args()

    try:
        count = run_extract(
            local_json_path=args.local_json_path,
            output_ndjson_path=args.output_ndjson_path,
            source_file_name=args.source_file_name,
            item_path=args.item_path,
            max_scan_bytes=args.max_scan_bytes,
            max_records=args.max_records,
            run_id=args.run_id,
        )
        print(f"Emitted {count} rows to {args.output_ndjson_path}")
    except (FileNotFoundError, ValueError) as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
