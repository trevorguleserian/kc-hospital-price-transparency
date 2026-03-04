# MVP local: optional force re-ingest + Silver + local BI export + Streamlit (APP_MODE=local).
# Run from repo root. Requires venv activated.
# With SAMPLE_DATA=1: reads from data/sample/ and runs Bronze+Silver if needed.
# Usage:
#   .\scripts\mvp_local.ps1
#   $env:SAMPLE_DATA = "1"; .\scripts\mvp_local.ps1
#   .\scripts\mvp_local.ps1 -IngestDate 2026-03-03 -ForceReingest

param(
    [string] $IngestDate = (Get-Date -Format "yyyy-MM-dd"),
    [switch] $ForceReingest
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$env:PYTHONPATH = $repoRoot
$env:STORAGE_BACKEND = "local"
$env:APP_MODE = "local"

$runIngest = $ForceReingest -or ($env:SAMPLE_DATA -match "^(1|true|yes)$")
if ($runIngest) {
    if ($env:SAMPLE_DATA -match "^(1|true|yes)$") {
        Write-Host "Sample data mode: ingesting from data/sample/..."
    } else {
        Write-Host "Force re-ingest for $IngestDate..."
    }
    if ($ForceReingest) {
        .\scripts\reingest_local_bronze.ps1 -IngestDate $IngestDate -Sources pt_csv -Force
    } else {
        python -c "from ingestion.bronze_ingest import run_bronze_ingest; print(run_bronze_ingest(ingest_date='$IngestDate'))"
    }
    if ($LASTEXITCODE -ne 0) { throw "Bronze ingest failed" }

    Write-Host "Building Silver for $IngestDate..."
    python -c "from transform.silver_build import build_silver_for_date; print(build_silver_for_date('$IngestDate', base_dir='.'))"
    if ($LASTEXITCODE -ne 0) { throw "Silver build failed" }
}

$env:DBT_SILVER_GLOB = "$repoRoot\lake\silver\standard_charges\**\*.parquet"
Write-Host "Running local BI export..."
.\scripts\run_local_bi.ps1
if ($LASTEXITCODE -ne 0) { throw "run_local_bi failed" }

Write-Host "Starting Streamlit (APP_MODE=local)..."
streamlit run apps/streamlit_app/Home.py
