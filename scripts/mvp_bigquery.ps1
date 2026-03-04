# MVP BigQuery: run BigQuery gold build then start Streamlit with APP_MODE=bigquery.
# Run from repo root. Set GOOGLE_APPLICATION_CREDENTIALS or use gcloud ADC.
# Optional: set BQ_PROJECT, BQ_DATASET, or use .env (see .env.example).
# Usage: .\scripts\mvp_bigquery.ps1

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

Write-Host "Running BigQuery gold build..."
.\scripts\run_bigquery_gold.ps1
if ($LASTEXITCODE -ne 0) { throw "run_bigquery_gold failed" }

$env:APP_MODE = "bigquery"
Write-Host "Starting Streamlit (APP_MODE=bigquery)..."
streamlit run apps/streamlit_app/Home.py
