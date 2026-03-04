# Sample data (quickstart)

This folder holds **tiny sample files** for a quick local run without downloading full hospital data.

- **Purpose:** Quickstart and CI-friendly ingestion; each file is small (< 1MB).
- **Full raw data** is intentionally **not** in the repo. For real runs, place files in `data/raw_drop/` (gitignored) or use your own storage.

## Files

| File | Format | Description |
|------|--------|-------------|
| `sample_standardcharges.json` | JSON | CMS-style structure with `standardCharges` and `payers_information`. |
| `sample_standardcharges.csv` | CSV | Header on row 3 (two preamble rows); columns compatible with Bronze CSV mapping. |

## Use sample data

Set `SAMPLE_DATA=1` (and optionally `RAW_DROP_DIR=data/sample`) before running Bronze ingest so the pipeline reads from this folder instead of `data/raw_drop/`. See README **Quickstart (Sample Data)**.

## Adding more sample files

Keep files under 1MB. Ensure JSON has `standardCharges` or `standard_charge_information` (or a single object for payload); CSV should have a header row with code/rate-like column names (e.g. `billing_code`, `gross_charge`, `negotiated_rate`).
