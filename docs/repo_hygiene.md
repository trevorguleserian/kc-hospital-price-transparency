# Repo hygiene

Why certain paths are excluded from git, how to work with full data, and how to run the preflight audit before pushing.

---

## Why raw_drop, incoming, lake, and warehouse are excluded

- **`data/raw_drop/`** — Raw hospital CSV/JSON files. Large, often sensitive or proprietary; not for the repo. Readers use **`data/sample/`** for quickstart or their own files locally.
- **`incoming/`** — Optional landing area for incoming data; same as above.
- **`lake/`** — Bronze and Silver Parquet written by the pipeline. Regenerated from raw data; can be large.
- **`warehouse/`** — Local DuckDB manifest and gold DB; regenerated.

These are in **`.gitignore`**. We never commit them so the repo stays small and free of secrets or PII.

---

## How to fetch or prepare full data

- **Sample data (quickstart):** Use `data/sample/` (committed). Set `SAMPLE_DATA=1` so Bronze ingest reads from `data/sample/` instead of `data/raw_drop/`.
- **Your own data:** Place files in `data/raw_drop/` (or set `RAW_DROP_DIR` to another path). Run Bronze ingest → Silver build → dbt → Streamlit. Do not commit `data/raw_drop/`.
- **Download at runtime:** If you have a script or URL to fetch files, write them to `data/raw_drop/` (or another dir and set `RAW_DROP_DIR`) before running the pipeline.

---

## Preflight script (run before pushing)

To avoid accidentally pushing secrets, large files, or tracked raw/lake/warehouse paths:

**PowerShell (from repo root):**
```powershell
.\scripts\preflight_repo_audit.ps1
```

**Bash:**
```bash
./scripts/preflight_repo_audit.sh
```

The script **fails** if it finds:

1. **Tracked files** in: `data/raw_drop/`, `incoming/`, `lake/`, `warehouse/`, `dbt/target/`, `dbt/exports/`
2. **Tracked secrets-like files:** `profiles.yml` (other than templates/examples), `*.key`, `*.pem`, `*service-account*.json`, `.env` (other than `.env.example`)
3. **Any tracked file > 95MB** (GitHub limit 100MB)

It prints **remediation steps** (e.g. `git rm --cached <file>`). Fix the issues, then run the script again before pushing.
