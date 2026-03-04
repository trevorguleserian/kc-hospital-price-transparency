"""
Dagster definitions: daily partitioned job for Bronze -> Silver -> Gold (dbt) with DQ gate and audit.
Local mode: EXECUTION_MODE=local, STORAGE_BACKEND=local, dbt-duckdb.
Cloud mode: EXECUTION_MODE=bq, STORAGE_BACKEND=gcs, dbt-bigquery.
"""
import os
from datetime import datetime
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    Definitions,
    EnvVar,
    asset,
    define_asset_job,
    job,
    op,
    run_request,
    schedule_from_partitions,
)
from dagster import DailyPartitionsDefinition

# Partitions: one partition per calendar day
daily_partitions = DailyPartitionsDefinition(
    start_date="2025-01-01",
    timezone="UTC",
    end_offset=0,
)


@asset(partitions_def=daily_partitions, retry_policy={"max_retries": 2})
def ingest_bronze(context: AssetExecutionContext) -> dict:
    """Ingest raw CSV/JSON to lake/bronze; idempotent by file_hash."""
    from ingestion.bronze_ingest import run_bronze_ingest
    partition_date = context.partition_key
    base = os.environ.get("LAKE_BASE_DIR", os.getcwd())
    result = run_bronze_ingest(ingest_date=partition_date, base_dir=base, skip_existing_success=True)
    context.log.info(f"Bronze ingest: {result}")
    return result


@asset(partitions_def=daily_partitions, retry_policy={"max_retries": 2})
def build_silver(context: AssetExecutionContext) -> dict:
    """Build Silver from Bronze partition; good -> standard_charges, bad -> quarantine."""
    from transform.silver_build import build_silver_for_date
    partition_date = context.partition_key
    base = os.environ.get("LAKE_BASE_DIR", os.getcwd())
    result = build_silver_for_date(ingest_date=partition_date, base_dir=base)
    context.log.info(f"Silver build: {result}")
    return result


@asset(partitions_def=daily_partitions)
def dq_gate_silver(context: AssetExecutionContext, build_silver: dict) -> dict:
    """Fail if quarantine_rate > threshold (e.g. 0.5)."""
    threshold = float(os.environ.get("DQ_QUARANTINE_RATE_THRESHOLD", "0.5"))
    rate = build_silver.get("quarantine_rate", 1.0)
    if rate > threshold:
        raise ValueError(f"DQ gate failed: quarantine_rate {rate} > {threshold}")
    return build_silver


def _dbt_cmd_local(repo_root: Path) -> list:
    """Build dbt command for local_duckdb: profiles-dir, target, vars, exclude BQ-only models."""
    import json
    base = os.environ.get("LAKE_BASE_DIR", str(repo_root))
    silver_glob = os.environ.get("DBT_SILVER_GLOB")
    if not silver_glob:
        # Windows-safe: use forward slashes for DuckDB
        p = (Path(base) / "lake" / "silver" / "standard_charges" / "**" / "*.parquet").resolve()
        silver_glob = str(p).replace("\\", "/")
    vars_json = json.dumps({"execution_mode": "local", "silver_parquet_glob": silver_glob})
    dbt_dir = os.environ.get("DBT_PROJECT_DIR", str(repo_root / "dbt"))
    profiles_dir = os.environ.get("DBT_PROFILES_DIR", dbt_dir)
    exclude_selector = "stg_pt_json_rates stg_pt_csv_tall stg_pt_csv_wide int_pt_standard_charges_union"
    cmd = [
        "dbt", "build",
        "--project-dir", dbt_dir,
        "--profiles-dir", profiles_dir,
        "--target", "local_duckdb",
        "--vars", vars_json,
        "--exclude", exclude_selector,
    ]
    return cmd


@asset(partitions_def=daily_partitions, retry_policy={"max_retries": 1})
def dbt_build_gold(context: AssetExecutionContext) -> dict:
    """Run dbt build: local = dbt-duckdb (Silver as source); bq = existing BigQuery."""
    import subprocess
    mode = os.environ.get("EXECUTION_MODE", "bq").strip().lower()
    repo_root = Path(__file__).resolve().parent.parent.parent
    dbt_dir = os.environ.get("DBT_PROJECT_DIR", str(repo_root / "dbt"))
    profiles_dir = os.environ.get("DBT_PROFILES_DIR", dbt_dir)
    if mode == "local":
        cmd = _dbt_cmd_local(repo_root)
    else:
        cmd = ["dbt", "build", "--project-dir", dbt_dir, "--profiles-dir", profiles_dir]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(repo_root))
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise RuntimeError(f"dbt build failed: {result.stderr[:500]}")
    return {"status": "ok", "mode": mode}


@asset(partitions_def=daily_partitions)
def dbt_test_gate(context: AssetExecutionContext) -> dict:
    """Run dbt test; local = same profile/target/vars and exclude BQ-only."""
    import subprocess
    import json
    mode = os.environ.get("EXECUTION_MODE", "bq").strip().lower()
    repo_root = Path(__file__).resolve().parent.parent.parent
    dbt_dir = os.environ.get("DBT_PROJECT_DIR", str(repo_root / "dbt"))
    profiles_dir = os.environ.get("DBT_PROFILES_DIR", dbt_dir)
    if mode == "local":
        base = os.environ.get("LAKE_BASE_DIR", str(repo_root))
        silver_glob = os.environ.get("DBT_SILVER_GLOB") or str((Path(base) / "lake" / "silver" / "standard_charges" / "**" / "*.parquet").resolve()).replace("\\", "/")
        vars_json = json.dumps({"execution_mode": "local", "silver_parquet_glob": silver_glob})
        exclude_selector = "stg_pt_json_rates stg_pt_csv_tall stg_pt_csv_wide int_pt_standard_charges_union"
        cmd = [
            "dbt", "test",
            "--project-dir", dbt_dir,
            "--profiles-dir", profiles_dir,
            "--target", "local_duckdb",
            "--vars", vars_json,
            "--exclude", exclude_selector,
        ]
    else:
        cmd = ["dbt", "test", "--project-dir", dbt_dir, "--profiles-dir", profiles_dir]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(repo_root))
    if result.returncode != 0:
        raise RuntimeError(f"dbt test failed: {result.stderr[:500]}")
    return {"status": "ok"}


@asset
def write_run_audit(context: AssetExecutionContext) -> dict:
    """Always runs: write run audit (e.g. last run time) for observability."""
    return {"last_run_ts": datetime.utcnow().isoformat(), "run_id": str(context.run_id)}


# Job: run all assets in order for a partition
assets_for_job = [ingest_bronze, build_silver, dq_gate_silver, dbt_build_gold, dbt_test_gate, write_run_audit]

daily_job = define_asset_job(
    name="daily_lakehouse_job",
    selection=assets_for_job,
    partitions_def=daily_partitions,
)

defs = Definitions(
    assets=assets_for_job,
    jobs=[daily_job],
)
