# Force re-ingest local bronze from data/raw_drop (ignore file_manifest for selected sources).
# Use when CSVs were skipped because they were previously ingested as preamble-only and you want to re-process.
# Requires: STORAGE_BACKEND=local (default). Run from repo root.
# Example: .\scripts\reingest_local_bronze.ps1 -IngestDate 2026-03-03 -Sources pt_csv -Force

param(
    [string] $IngestDate = (Get-Date -Format "yyyy-MM-dd"),
    [string[]] $Sources = @("pt_csv"),
    [switch] $Force
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$env:PYTHONPATH = $repoRoot
if (-not $env:STORAGE_BACKEND) { $env:STORAGE_BACKEND = "local" }

$sourceList = $Sources -join " "
$args = @("--ingest-date", $IngestDate, "--base-dir", ".")
if ($Force) {
    $args += "--force"
    if ($sourceList) { $args += "--sources"; $args += $Sources }
}

& python (Join-Path $PSScriptRoot "run_reingest_bronze.py") @args
if ($LASTEXITCODE -ne 0) { throw "Bronze re-ingest failed" }
