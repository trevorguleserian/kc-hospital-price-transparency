# Run dbt build for BigQuery gold (marts) layer.
# Uses dbt target "bigquery" (must exist in dbt/profiles.yml; see docs/dbt_profiles_template.yml).
# Auth: set GOOGLE_APPLICATION_CREDENTIALS for service account, or use gcloud ADC (see README).
# Run from repo root.

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$dbtDir = Join-Path $repoRoot "dbt"
Push-Location $dbtDir

try {
    Write-Host "Running dbt deps..."
    dbt deps
    if ($LASTEXITCODE -ne 0) { throw "dbt deps failed" }

    Write-Host "Running dbt build (bigquery, marts + upstream)..."
    dbt build --target bigquery --select "+path:models/marts"
    if ($LASTEXITCODE -ne 0) { throw "dbt build failed" }
} finally {
    Pop-Location
}
