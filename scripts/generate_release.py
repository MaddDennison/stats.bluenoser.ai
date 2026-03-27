"""Generate a sample CPI release.

Usage:
    python -m scripts.generate_release 2026-02-01          # Generate Feb 2026 CPI
    python -m scripts.generate_release 2026-02-01 --dry-run # Generate but don't save to DB
    python -m scripts.generate_release 2026-02-01 --context-only  # Just show the data context
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import date

from pipeline import db
from pipeline.analyzer import (
    build_cpi_context,
    calculate_significance_score,
    generate_cpi_release,
)
from pipeline.db import close_pool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m scripts.generate_release <YYYY-MM-DD> [--dry-run] [--context-only]")
        print("Example: python -m scripts.generate_release 2026-02-01")
        sys.exit(1)

    ref_period = date.fromisoformat(sys.argv[1])
    dry_run = "--dry-run" in sys.argv
    context_only = "--context-only" in sys.argv

    logger.info(f"Reference period: {ref_period.strftime('%B %Y')}")

    # Build and display context
    context = build_cpi_context(ref_period)
    significance = calculate_significance_score(context)

    print(f"\n{'='*60}")
    print(f"  CPI Data Context — {context['ref_month']} {context['ref_year']}")
    print(f"{'='*60}\n")

    for desc, vals in sorted(context["series"].items()):
        current = vals.get("current", "N/A")
        yoy = vals.get("yoy_pct", "N/A")
        mom = vals.get("mom_pct", "N/A")
        print(f"  {desc}: {current}  (YoY: {yoy}%, MoM: {mom}%)")

    print(f"\n  Significance score: {significance:.2f}")

    if context_only:
        close_pool()
        return

    # Generate release
    print(f"\n{'='*60}")
    print(f"  Generating release via Claude API...")
    print(f"{'='*60}\n")

    result = generate_cpi_release(ref_period, dry_run=dry_run)

    print(f"\n{'='*60}")
    print(f"  {result['title']}")
    print(f"{'='*60}\n")
    print(result["body_markdown"])

    if not dry_run and "release_id" in result:
        print(f"\n  Saved to database: release_id={result['release_id']}")
    elif dry_run:
        print(f"\n  (Dry run — not saved to database)")

    close_pool()


if __name__ == "__main__":
    main()
