#!/usr/bin/env python3
"""CLI for bronze re-ingest. Used by scripts/reingest_local_bronze.ps1."""
import argparse
import os
import sys

# Repo root on PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.bronze_ingest import run_bronze_ingest

ALLOWED_SOURCES = ("pt_csv", "pt_json")


def main() -> None:
    p = argparse.ArgumentParser(description="Run bronze ingest with optional force re-ingest.")
    p.add_argument("--ingest-date", default=None, help="Ingest date YYYY-MM-DD (default: today)")
    p.add_argument("--sources", nargs="*", default=None, help="Force re-ingest only these: pt_csv pt_json")
    p.add_argument("--force", action="store_true", help="Ignore manifest and re-ingest (all or --sources)")
    p.add_argument("--base-dir", default=".", help="Project base directory")
    args = p.parse_args()

    from datetime import datetime
    ingest_date = args.ingest_date or datetime.utcnow().strftime("%Y-%m-%d")
    force_sources = None
    if args.force and args.sources:
        force_sources = [s for s in args.sources if s in ALLOWED_SOURCES]
    elif args.force:
        force_sources = list(ALLOWED_SOURCES)

    result = run_bronze_ingest(
        ingest_date=ingest_date,
        base_dir=args.base_dir,
        force_reingest=bool(args.force),
        force_reingest_sources=force_sources,
    )
    print("Result:", result)


if __name__ == "__main__":
    main()
