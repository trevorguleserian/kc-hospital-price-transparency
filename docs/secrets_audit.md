# Secrets / credentials audit

One-time audit of the repo for credentials risk. **No literal secrets were found.** Findings and fixes below.

---

## 1. `dbt/profiles.yml` — **should be ignored**

| Path | Line(s) | Risk |
|------|---------|------|
| `dbt/profiles.yml` | 1–35 | File contains `keyfile: "{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}"` and project/dataset. If committed, users may overwrite with real paths or (if someone pastes a key path) expose intent. README already says "keep dbt/profiles.yml in .gitignore". |

**Fix:** Add `dbt/profiles.yml` to `.gitignore` so it is never committed. Use `docs/dbt_profiles_template.yml` as the only committed reference. **Done:** `dbt/profiles.yml` has been added to `.gitignore`. If the file is already tracked, run: `git rm --cached dbt/profiles.yml` and commit.

---

## 2. Grep: credential-related strings (project code only; `.venv` excluded)

### `GOOGLE_APPLICATION_CREDENTIALS`

| File | Line | Note |
|------|------|------|
| `README.md` | 48, 95, 107, 109, 185–186 | Documentation only (env var name, no values). OK. |
| `RUNBOOK.md` | 61, 79 | Documentation only. OK. |
| `docs/runbook.md` | 56 | Documentation only. OK. |
| `docs/bi/bigquery_publish.md` | 10 | Documentation only. OK. |
| `apps/streamlit_app/lib/data.py` | 445 | Error message telling user to set the env var. OK. |
| `scripts/mvp_bigquery.ps1` | 2 | Comment. OK. |
| `scripts/run_bigquery_gold.ps1` | 3 | Comment. OK. |
| `ingestion/99_run_bulk_ingestion.py` | 247 | `os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")` — reads env only, no literal. OK. |
| `ingestion/60_load_ndjson_to_bigquery.py` | 6 | Docstring. OK. |
| `ingestion/20_load_json_example.py` | 8 | Docstring. OK. |

### `keyfile`

| File | Line | Note |
|------|------|------|
| `dbt/profiles.yml` | 14, 25 | Points to `env_var('GOOGLE_APPLICATION_CREDENTIALS')`. File should be gitignored (see above). |
| `docs/dbt_profiles_template.yml` | 5–6, 21, 41, 52 | Template only; documents keyfile usage. OK. |
| `README.md` | 185–186 | Docs reference. OK. |

### `private_key`, `BEGIN PRIVATE KEY`, `api_key`, `token`, `password`

- **Project code:** No hits for `private_key`, `BEGIN PRIVATE KEY`, or literal `api_key`/`token`/`password` credentials in project-owned files.
- **False positives:** `transform/silver_build.py` line 95 — "token" in a string/numeric parsing comment. `dbt/models/staging/local/stg_silver_standard_charges.sql` — `hospital_token` is a column name, not a secret. No action.

---

## 3. Paths to credentials

- No hardcoded paths to key files (e.g. `C:\keys\service-account.json`) in project code.
- All GCP auth uses env vars (`GOOGLE_APPLICATION_CREDENTIALS`) or gcloud ADC; no embedded paths.

---

## 4. Recommendations

1. **Keep `dbt/profiles.yml` out of the repo:** Already added to `.gitignore`. If it was previously committed, run `git rm --cached dbt/profiles.yml` and re-commit.
2. **Continue using the template:** Keep `docs/dbt_profiles_template.yml` as the only committed profile example; document in README that real `profiles.yml` lives in `.dbt` or is gitignored in `dbt/`.
3. **No literal secrets found:** No cleanup of committed secrets required. Keep `.env` and `*credentials*.json` (and similar) in `.gitignore`.
4. **CI:** The GitHub Actions workflow uses a placeholder `profiles.yml` in `$HOME/.dbt` and does not use repo `dbt/profiles.yml` or any secrets; no change needed.

---

*Generated from a one-time grep-based audit. Re-run or extend as needed for compliance.*
