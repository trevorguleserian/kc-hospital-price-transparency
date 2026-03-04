# Preflight checklist before public GitHub release

Run through this list before pushing the repo as a public portfolio.

---

## 1. No secrets committed

- [ ] **dbt/profiles.yml** — Must be ignored. Confirm: `git check-ignore dbt/profiles.yml` (should output the path). If it was ever committed, run `git rm --cached dbt/profiles.yml` and commit.
- [ ] **.gitignore** includes: `dbt/profiles.yml`, `lake/`, `warehouse/`, `dbt/exports/`, `secrets.toml`, `.env`, `*credentials*.json`, `*-key*.json`, `*.pem`.
- [ ] No literal keys or passwords in code. Grep (from repo root):
  - `GOOGLE_APPLICATION_CREDENTIALS` — OK if only in docs/comments or as env_var name in templates.
  - `keyfile:` — OK only in template files (`dbt/profiles.template.yml`, `docs/dbt_profiles_template.yml`) pointing to `env_var('GOOGLE_APPLICATION_CREDENTIALS')`.
  - `BEGIN PRIVATE KEY` — Must be zero hits in project code (exclude .venv).

---

## 2. README and docs

- [ ] README has: project overview, architecture (Bronze/Silver/Gold + dbt), local quickstart (venv, pip, bronze, silver, dbt, exports), BigQuery quickstart (env vars, profiles.template.yml, run_bigquery_gold.ps1), Streamlit MVP/demo instructions, troubleshooting.
- [ ] Docs exist: `docs/architecture.md`, `docs/runbook.md`, `docs/bigquery_publish.md`.
- [ ] Runbook covers: bronze re-ingest, header sniffing (CSV), common issues.
- [ ] All doc links from README work (relative paths).

---

## 3. dbt profile for readers

- [ ] `dbt/profiles.template.yml` exists (safe template, no real credentials).
- [ ] README and docs say: copy to `dbt/profiles.yml` (do not commit). `dbt/profiles.yml` is in `.gitignore`.

---

## 4. Streamlit demo mode

- [ ] Default data source is Local (no credentials). If `dbt/exports/` has Parquet/CSV, app loads them; filters: hospital, payer, billing_code_type, rate_category, top-N table.
- [ ] If exports don’t exist, app shows a friendly message with steps to generate them (Bronze → Silver → dbt build + export or mvp_local.ps1).

---

## 5. CI and tests

- [ ] GitHub Actions CI runs without secrets (e.g. `dbt parse`, Streamlit data-layer import). No credentials in workflow.

---

## 6. Optional before push

- [ ] Remove or redact any internal project names / dataset names in README or docs if they shouldn’t be public.
- [ ] Ensure `docs/secrets_audit.md` (if present) doesn’t expose anything sensitive.
- [ ] Run `dbt parse` and Streamlit locally once to confirm nothing is broken.
