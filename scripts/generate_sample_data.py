"""
Generate sample CSV and JSON under data/raw_drop for local lakehouse testing.
Run from repo root: python scripts/generate_sample_data.py
"""
import csv
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_DROP = REPO_ROOT / "data" / "raw_drop"


def main() -> None:
    RAW_DROP.mkdir(parents=True, exist_ok=True)

    # Sample tall CSV (row per rate)
    tall_path = RAW_DROP / "sample_tall_rates.csv"
    with open(tall_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["hospital_name", "billing_code", "description", "payer_name", "plan_name", "rate_type", "estimated_amount"])
        w.writerow(["Sample Hospital", "99213", "Office visit", "Payer A", "Plan 1", "negotiated_rate", "150.00"])
        w.writerow(["Sample Hospital", "99214", "Office visit level 4", "Payer A", "Plan 1", "negotiated_rate", "220.50"])
    print(f"Wrote {tall_path}")

    # Sample JSON (array of rate objects)
    sample_json = [
        {"billing_code": "99213", "billing_code_type": "CPT", "description": "Office visit", "payer": "Payer B", "plan": "Plan 2", "rate_type": "negotiated_rate", "negotiated_rate": 145.00},
        {"billing_code": "99214", "billing_code_type": "CPT", "description": "Office visit 4", "payer": "Payer B", "plan": "Plan 2", "rate_type": "negotiated_rate", "negotiated_rate": 210.00},
    ]
    json_path = RAW_DROP / "sample_rates.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(sample_json, f, indent=2)
    print(f"Wrote {json_path}")

    print("Sample data ready in data/raw_drop. Run Bronze ingest for today's partition.")


if __name__ == "__main__":
    main()
