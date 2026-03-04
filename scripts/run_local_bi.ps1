# Run local dbt build and BI export (DuckDB target).
# Requires: activated Python venv (e.g. .venv\Scripts\Activate.ps1 from repo root).
# Requires: DBT_SILVER_GLOB set (e.g. $env:DBT_SILVER_GLOB = "$PWD\lake\silver\standard_charges\**\*.parquet").
# Run from repo root.

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

if (-not $env:DBT_SILVER_GLOB) {
    Write-Error "DBT_SILVER_GLOB is not set. Set it to the Silver Parquet glob, e.g.: `$env:DBT_SILVER_GLOB = `"`$PWD\lake\silver\standard_charges\**\*.parquet`""
    exit 1
}

$dbtDir = Join-Path $repoRoot "dbt"
$exportsDir = Join-Path $dbtDir "exports"
Push-Location $dbtDir

try {
    if (-not (Test-Path -LiteralPath $exportsDir)) {
        New-Item -ItemType Directory -Path $exportsDir -Force | Out-Null
    }
    Write-Host "Running dbt deps..."
    dbt deps
    if ($LASTEXITCODE -ne 0) { throw "dbt deps failed" }

    Write-Host "Running dbt build (local_duckdb, staging/local+, full-refresh)..."
    dbt build --target local_duckdb --select "path:models/staging/local+" --full-refresh --vars "{execution_mode: local, silver_parquet_glob: '$env:DBT_SILVER_GLOB'}"
    if ($LASTEXITCODE -ne 0) { throw "dbt build failed" }

    Write-Host "Running export_bi_outputs..."
    dbt run-operation export_bi_outputs --target local_duckdb --vars "{execution_mode: local, silver_parquet_glob: '$env:DBT_SILVER_GLOB'}"
    if ($LASTEXITCODE -ne 0) { throw "dbt run-operation export_bi_outputs failed" }

    $files = @(Get-ChildItem -Path $exportsDir -File -ErrorAction SilentlyContinue)
    if ($files.Count -gt 0) {
        Write-Host "Exported files in dbt/exports:"
        foreach ($f in $files) { Write-Host "  $($f.Name)" }
    } else {
        Write-Host "No files found in dbt/exports."
    }
} finally {
    Pop-Location
}
