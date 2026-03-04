# BigQuery publish

Steps to build and publish the Gold (marts) layer to BigQuery and validate.

**Quick steps:** Set `GOOGLE_APPLICATION_CREDENTIALS`, copy `dbt/profiles.template.yml` to `dbt/profiles.yml` and set project/dataset, then from repo root run `scripts/run_bigquery_gold.ps1`.

**Full guide (prerequisites, dbt commands, validation queries, sanity checks):** [bi/bigquery_publish.md](bi/bigquery_publish.md).
