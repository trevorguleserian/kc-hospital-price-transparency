"""
Auto-bootstrap dbt/exports from data/sample/ when SAMPLE_DATA=1 and exports are missing.
Used on Streamlit Community Cloud where dbt/exports is not committed.
No credentials required; runs Bronze -> Silver -> dbt build -> export (DuckDB).
"""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

# Repo root: apps/streamlit_app/lib/bootstrap.py -> 4 levels up
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent

# Required export tables (parquet or csv) to consider "exports exist"
_REQUIRED_EXPORTS = ("dim_hospital", "fct_standard_charges_semantic")
_DEMO_INGEST_DATE = "2026-03-03"
_bootstrap_done = False  # in-process guard so we only run once


def _export_dir() -> Path:
    rel = os.environ.get("LOCAL_EXPORT_DIR", "dbt/exports").strip()
    return (_REPO_ROOT / rel).resolve()


def _exports_exist() -> bool:
    export_dir = _export_dir()
    for table in _REQUIRED_EXPORTS:
        if (export_dir / f"{table}.parquet").exists() or (export_dir / f"{table}.csv").exists():
            continue
        return False
    return True


def _ensure_repo_in_path() -> None:
    repo = str(_REPO_ROOT)
    if repo not in sys.path:
        sys.path.insert(0, repo)


def _run_bronze(st) -> None:
    _ensure_repo_in_path()
    os.environ["STORAGE_BACKEND"] = "local"
    os.environ["SAMPLE_DATA"] = "1"
    from ingestion.bronze_ingest import run_bronze_ingest
    result = run_bronze_ingest(ingest_date=_DEMO_INGEST_DATE, base_dir=str(_REPO_ROOT))
    if st:
        st.write(f"Bronze: ingested={result.get('ingested', 0)}, skipped={result.get('skipped', 0)}, failed={result.get('failed', 0)}")
    if result.get("failed", 0) > 0 and result.get("ingested", 0) == 0:
        raise RuntimeError(f"Bronze ingest failed: {result.get('errors', [])}")


def _run_silver(st) -> None:
    _ensure_repo_in_path()
    from transform.silver_build import build_silver_for_date
    out = build_silver_for_date(_DEMO_INGEST_DATE, base_dir=str(_REPO_ROOT))
    if st:
        st.write(f"Silver: good_rows={out.get('good_rows', 0)}, quarantine={out.get('quarantine_rows', 0)}")


def _run_dbt_build_and_export(st) -> None:
    (_REPO_ROOT / "dbt" / "exports").mkdir(parents=True, exist_ok=True)
    (_REPO_ROOT / "warehouse" / "duckdb").mkdir(parents=True, exist_ok=True)

    # Absolute path for Silver glob (Linux and Windows)
    silver_glob = (_REPO_ROOT / "lake" / "silver" / "standard_charges").resolve()
    silver_glob_str = str(silver_glob).replace("\\", "/") + "/**/*.parquet"

    profiles_dir = tempfile.mkdtemp(prefix="dbt_profiles_")
    profiles_file = Path(profiles_dir) / "profiles.yml"
    duckdb_path = (_REPO_ROOT / "warehouse" / "duckdb" / "gold.duckdb").resolve()
    # Path relative to dbt/ (where we run dbt from)
    duckdb_rel = os.path.relpath(duckdb_path, _REPO_ROOT / "dbt")
    if duckdb_rel.startswith(".."):
        duckdb_rel = duckdb_rel.replace("\\", "/")
    else:
        duckdb_rel = "../" + duckdb_rel.replace("\\", "/")

    profiles_content = f"""# Auto-generated for demo bootstrap (no credentials)
kc_hospital_price_transparency:
  target: local_duckdb
  outputs:
    local_duckdb:
      type: duckdb
      path: "{duckdb_rel}"
      threads: 4
"""
    profiles_file.write_text(profiles_content, encoding="utf-8")

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = profiles_dir
    env["DBT_SILVER_GLOB"] = silver_glob_str
    dbt_dir = _REPO_ROOT / "dbt"
    vars_json = f'{{"execution_mode": "local", "silver_parquet_glob": "{silver_glob_str}"}}'

    def run_cmd(cmd: list[str], step_name: str) -> None:
        if st:
            st.write(f"Running: {' '.join(cmd)}")
        r = subprocess.run(
            cmd,
            cwd=str(dbt_dir),
            env=env,
            capture_output=True,
            text=True,
        )
        if r.returncode != 0:
            err = (r.stderr or "").strip() or (r.stdout or "").strip()
            cmd_str = " ".join(cmd)
            if st:
                st.error(f"**{step_name} failed** (exit {r.returncode})")
                st.write(f"Command: `{cmd_str}`")
                st.code(err[:2000] if err else "No stderr", language="text")
            raise RuntimeError(f"{step_name} failed. Command: {cmd_str}. {err[:500]}")
        if st and r.stdout:
            st.write(r.stdout[:500] if len(r.stdout) > 500 else r.stdout)

    run_cmd(["dbt", "deps"], "dbt deps")
    run_cmd([
        "dbt", "build", "--target", "local_duckdb", "--full-refresh",
        "--select", "path:models/staging/local+",
        "--vars", vars_json,
    ], "dbt build")
    run_cmd([
        "dbt", "run-operation", "export_bi_outputs",
        "--target", "local_duckdb",
        "--vars", vars_json,
    ], "dbt run-operation export_bi_outputs")


def ensure_demo_exports_exist(st=None) -> bool:
    """
    If SAMPLE_DATA=1 and required exports are missing, run Bronze -> Silver -> dbt -> export.
    Idempotent: if exports exist or we already ran in this process, skip. No credentials required.
    Uses st for logging (spinner, write) when provided.
    Returns True if exports exist (or were just created), False if bootstrap not applicable.
    """
    global _bootstrap_done
    if os.environ.get("SAMPLE_DATA", "").strip().lower() not in ("1", "true", "yes"):
        return False
    if _bootstrap_done or _exports_exist():
        return True

    _ensure_repo_in_path()
    if st:
        with st.status("Demo bootstrap: generating dbt/exports from data/sample/ (Bronze → Silver → dbt → export)...", expanded=True):
            try:
                st.write("Step 1/3: Bronze ingest from data/sample/...")
                _run_bronze(st)
                st.write("Step 2/3: Silver build...")
                _run_silver(st)
                st.write("Step 3/3: dbt build + export...")
                _run_dbt_build_and_export(st)
                st.success("Bootstrap complete. Exports ready.")
            except Exception as e:
                st.error(str(e))
                raise
    else:
        _run_bronze(None)
        _run_silver(None)
        _run_dbt_build_and_export(None)

    _bootstrap_done = True
    return _exports_exist()
