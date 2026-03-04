# MVP local: optional force re-ingest + Silver + local BI export + Streamlit (APP_MODE=local).
# Run from repo root. Requires venv activated and (if -ForceReingest) data in data/raw_drop.
# Usage:
#   .\scripts\mvp_local.ps1
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

if ($ForceReingest) {
    Write-Host "Force re-ingest CSV for $IngestDate..."
    .\scripts\reingest_local_bronze.ps1 -IngestDate $IngestDate -Sources pt_csv -Force
    if ($LASTEXITCODE -ne 0) { throw "Reingest failed" }

    Write-Host "Building Silver for $IngestDate..."
    python -c "from transform.silver_build import build_silver_for_date; build_silver_for_date('$IngestDate', base_dir='.')"
    if ($LASTEXITCODE -ne 0) { throw "Silver build failed" }
}

$env:DBT_SILVER_GLOB = "$repoRoot\lake\silver\standard_charges\**\*.parquet"
Write-Host "Running local BI export..."
.\scripts\run_local_bi.ps1
if ($LASTEXITCODE -ne 0) { throw "run_local_bi failed" }

Write-Host "Starting Streamlit (APP_MODE=local)..."
streamlit run apps/streamlit_app/Home.py
