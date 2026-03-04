#!/usr/bin/env bash
# Preflight repo audit: no big files, no secrets, no raw/lake/warehouse in git.
# Run from repo root before pushing to GitHub. Exits with 1 on failure.
# Usage: ./scripts/preflight_repo_audit.sh

set -u
cd "$(dirname "$0")/.."
REPO_ROOT="$(pwd)"
FAILED=0
MAX_SIZE_MB=95
MAX_BYTES=$((MAX_SIZE_MB * 1024 * 1024))

tracked() { git ls-files; }

# 1) Tracked files in forbidden directories
for dir in data/raw_drop/ incoming/ lake/ warehouse/ dbt/target/ dbt/exports/; do
  bad=$(tracked | grep -E "^${dir}" || true)
  if [ -n "$bad" ]; then
    echo "FAIL: Tracked files in forbidden path '$dir':"
    echo "$bad" | sed 's/^/  /'
    echo "  Remediation: git rm --cached <file> and add to .gitignore if needed."
    FAILED=1
  fi
done

# 2) Tracked secrets-like files
while IFS= read -r path; do
  path_unix="${path//\\/\/}"
  if [[ "$path_unix" == *profiles.yml ]] && [[ "$path_unix" != *template* ]] && [[ "$path_unix" != *example* ]]; then
    echo "FAIL: Tracked profile (not template): $path"
    echo "  Remediation: git rm --cached $path; use profiles.template.yml only."
    FAILED=1
  fi
  if [[ "$path_unix" == *.key ]] || [[ "$path_unix" == *.pem ]]; then
    echo "FAIL: Tracked key/pem: $path"
    echo "  Remediation: git rm --cached $path; add to .gitignore."
    FAILED=1
  fi
  if [[ "$path_unix" == *service-account*\.json ]] || [[ "$path_unix" == *credentials*\.json ]]; then
    echo "FAIL: Tracked credentials-like file: $path"
    echo "  Remediation: git rm --cached $path; never commit keys."
    FAILED=1
  fi
  if [[ "$path_unix" == .env ]] || [[ "$path_unix" == */.env ]] && [[ "$path_unix" != *\.example ]]; then
    echo "FAIL: Tracked .env (may contain secrets): $path"
    echo "  Remediation: git rm --cached <file>; use .env.example only."
    FAILED=1
  fi
done < <(tracked)

# 3) Tracked file > 95MB
while IFS= read -r path; do
  [ -z "$path" ] && continue
  full="$REPO_ROOT/$path"
  if [ -f "$full" ]; then
    size=$(stat -c%s "$full" 2>/dev/null || stat -f%z "$full" 2>/dev/null)
    if [ -n "$size" ] && [ "$size" -gt "$MAX_BYTES" ]; then
      mb=$((size / 1024 / 1024))
      echo "FAIL: Tracked file exceeds ${MAX_SIZE_MB}MB (GitHub 100MB limit): $path (${mb} MB)"
      echo "  Remediation: remove from git (git rm --cached $path), add to .gitignore, use LFS or external storage."
      FAILED=1
    fi
  fi
done < <(tracked)

if [ $FAILED -eq 1 ]; then
  echo ""
  echo "Preflight failed. Fix the issues above before pushing."
  exit 1
fi
echo "Preflight passed: no forbidden paths, no tracked secrets, no files > ${MAX_SIZE_MB}MB."
exit 0
