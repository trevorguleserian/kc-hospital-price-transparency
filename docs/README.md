# Documentation

This folder holds architecture, runbooks, and reference docs for the KC Hospital Price Transparency project.

---

## Folder structure (suggested)

```
docs/
├── README.md                 # This file — docs overview and structure
├── architecture.md           # Pipeline (Bronze/Silver/dbt/Streamlit), diagram, design choices
├── data_quality.md           # Quarantine reason codes, dbt tests, known limitations
├── runbook.md                # Reruns, backfills, failure scenarios, env vars
├── dbt_profiles_template.yml # dbt profile template (copy to profiles.yml; do not commit)
├── secrets_audit.md          # One-time credentials/secrets audit (reference)
│
├── diagrams/                 # Optional: architecture and flow diagrams
│   └── .gitkeep              # (add architecture.png or .svg when ready)
│
├── screenshots/              # Optional: Streamlit app screenshots for README
│   └── .gitkeep              # (e.g. home.png, search_compare.png, data_quality.png)
│
└── bi/                       # BI and publish guides
    ├── data_dictionary.md    # Data dictionary and semantics
    ├── modeling_notes.md     # Modeling decisions and conventions
    ├── bigquery_publish.md   # BigQuery publish steps and validation
    └── powerbi_setup.md      # Power BI connection (if used)
```

---

## Suggested files (current vs optional)

| File / folder | Status | Purpose |
|---------------|--------|---------|
| `architecture.md` | Present | Pipeline stages, ASCII diagram, design choices, backends. |
| `data_quality.md` | Present | Quarantine codes, dbt tests, limitations. |
| `runbook.md` | Present | Operational runbook: reruns, backfills, failures, env. |
| `dbt_profiles_template.yml` | Present | Template for dbt profile; copy locally, do not commit. |
| `secrets_audit.md` | Present | Credentials audit; reference only. |
| `bi/data_dictionary.md` | Present | Field definitions, semantics. |
| `bi/modeling_notes.md` | Present | Modeling and naming conventions. |
| `bi/bigquery_publish.md` | Present | Publish marts to BigQuery and validate. |
| `bi/powerbi_setup.md` | Present | Power BI setup if applicable. |
| `diagrams/` | Optional | Add `architecture.png` or `.svg`; link from README and architecture.md. |
| `screenshots/` | Optional | Add app screenshots; link from main README Screenshots section. |
| `troubleshooting.md` | Optional | Can consolidate common issues here; currently in README + runbook. |

---

## Diagram placeholder

The main README and [architecture.md](architecture.md) reference a pipeline diagram. To add one:

1. Create or export a diagram (e.g. Mermaid, draw.io, or ASCII in architecture.md).
2. Save as `docs/diagrams/architecture.png` (or `.svg`).
3. In README Architecture section, replace the placeholder with:  
   `![Pipeline](docs/diagrams/architecture.png)`  
4. In architecture.md, add the same image or keep the ASCII version there.

---

## Screenshots placeholder

To fill the README Screenshots section:

1. Run the Streamlit app (local or BigQuery).
2. Capture Home, Search & Compare, and Data Quality views.
3. Save as `docs/screenshots/home.png`, `search_compare.png`, `data_quality.png`.
4. In README, replace the placeholder table with image links, e.g.:  
   `![Home](docs/screenshots/home.png)`.
