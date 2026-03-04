# Preflight repo audit: no big files, no secrets, no raw/lake/warehouse in git.
# Run from repo root before pushing to GitHub. Exits with 1 on failure.
# Usage: .\scripts\preflight_repo_audit.ps1

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$failed = $false
$MAX_SIZE_MB = 95

# 1) Tracked files in forbidden directories
$forbiddenDirs = @(
    "data/raw_drop/",
    "incoming/",
    "lake/",
    "warehouse/",
    "dbt/target/",
    "dbt/exports/"
)
$tracked = git ls-files 2>$null
foreach ($dir in $forbiddenDirs) {
    $bad = $tracked | Where-Object { $_ -like "$dir*" -or $_ -eq $dir.TrimEnd("/") }
    if ($bad) {
        Write-Host "FAIL: Tracked files in forbidden path '$dir':" -ForegroundColor Red
        $bad | ForEach-Object { Write-Host "  $_" }
        Write-Host "  Remediation: git rm --cached <file> and add to .gitignore if needed." -ForegroundColor Yellow
        $failed = $true
    }
}

# 2) Tracked secrets-like files
$secretsPatterns = @(
    { param($p) $p -match "profiles\.yml$" -and $p -notmatch "template|example" },
    { param($p) $p -match "\.(key|pem)$" },
    { param($p) $p -match "service-account.*\.json$" },
    { param($p) $p -match "credentials.*\.json$" }
)
foreach ($path in $tracked) {
    $pathStr = $path -replace "\\", "/"
    if ($pathStr -match "profiles\.yml$" -and $pathStr -notmatch "template|example") {
        Write-Host "FAIL: Tracked profile (not template): $path" -ForegroundColor Red
        Write-Host "  Remediation: git rm --cached $path; use profiles.template.yml only." -ForegroundColor Yellow
        $failed = $true
    }
    if ($pathStr -match "\.(key|pem)$") {
        Write-Host "FAIL: Tracked key/pem: $path" -ForegroundColor Red
        Write-Host "  Remediation: git rm --cached $path; add to .gitignore." -ForegroundColor Yellow
        $failed = $true
    }
    if ($pathStr -match "service-account.*\.json$" -or $pathStr -match "credentials.*\.json$") {
        Write-Host "FAIL: Tracked credentials-like file: $path" -ForegroundColor Red
        Write-Host "  Remediation: git rm --cached $path; never commit keys." -ForegroundColor Yellow
        $failed = $true
    }
}

# 2b) File content: GOOGLE_APPLICATION_CREDENTIALS pointing to a path (risky if committed path is real)
# We only flag if a tracked file contains a path-like string after GOOGLE_APPLICATION_CREDENTIALS (e.g. C:\keys\...)
# Skip templates and docs. Simple check: tracked .env or secrets.toml
$envLike = $tracked | Where-Object { $_ -match "\.env$" -and $_ -notmatch "\.example" }
if ($envLike) {
    Write-Host "FAIL: Tracked .env (may contain secrets): $($envLike -join ', ')" -ForegroundColor Red
    Write-Host "  Remediation: git rm --cached <file>; use .env.example only." -ForegroundColor Yellow
    $failed = $true
}

# 3) Tracked file > 95MB
$maxBytes = $MAX_SIZE_MB * 1MB
foreach ($path in $tracked) {
    $full = Join-Path $repoRoot $path
    if (Test-Path $full -PathType Leaf) {
        $len = (Get-Item $full).Length
        if ($len -gt $maxBytes) {
            $mb = [math]::Round($len / 1MB, 2)
            Write-Host "FAIL: Tracked file exceeds ${MAX_SIZE_MB}MB (GitHub 100MB limit): $path (${mb} MB)" -ForegroundColor Red
            Write-Host "  Remediation: remove from git (git rm --cached $path), add to .gitignore, use LFS or external storage." -ForegroundColor Yellow
            $failed = $true
        }
    }
}

if ($failed) {
    Write-Host ""
    Write-Host "Preflight failed. Fix the issues above before pushing." -ForegroundColor Red
    exit 1
}
Write-Host "Preflight passed: no forbidden paths, no tracked secrets, no files > ${MAX_SIZE_MB}MB." -ForegroundColor Green
exit 0
