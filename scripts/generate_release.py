"""Generate a sample release (CPI or LFS).

Usage:
    python -m scripts.generate_release cpi 2026-02-01              # CPI release
    python -m scripts.generate_release lfs 2026-02-01              # Labour Market release
    python -m scripts.generate_release cpi 2026-02-01 --dry-run    # Generate but don't save
    python -m scripts.generate_release cpi 2026-02-01 --context-only  # Just show data context
"""

from __future__ import annotations

import logging
import sys
from datetime import date

from pipeline import db
from pipeline.analyzer import (
    build_cpi_context,
    build_lfs_context,
    calculate_significance_score,
    generate_cpi_release,
    generate_lfs_release,
)
from pipeline.db import close_pool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

RELEASE_TYPES = {
    "cpi": {
        "context_fn": build_cpi_context,
        "generate_fn": generate_cpi_release,
        "label": "CPI",
    },
    "lfs": {
        "context_fn": build_lfs_context,
        "generate_fn": generate_lfs_release,
        "label": "Labour Market Trends",
    },
}


def main():
    if len(sys.argv) < 3 or sys.argv[1] not in RELEASE_TYPES:
        print("Usage: python -m scripts.generate_release <type> <YYYY-MM-DD> [--dry-run] [--context-only]")
        print(f"Types: {', '.join(RELEASE_TYPES.keys())}")
        print("Example: python -m scripts.generate_release cpi 2026-02-01")
        sys.exit(1)

    release_type = sys.argv[1]
    ref_period = date.fromisoformat(sys.argv[2])
    dry_run = "--dry-run" in sys.argv
    context_only = "--context-only" in sys.argv

    config = RELEASE_TYPES[release_type]
    logger.info(f"{config['label']} release for {ref_period.strftime('%B %Y')}")

    # Build and display context
    context = config["context_fn"](ref_period)

    print(f"\n{'='*60}")
    print(f"  {config['label']} Data Context — {context['ref_month']} {context['ref_year']}")
    print(f"{'='*60}\n")

    for desc, vals in sorted(context["series"].items()):
        current = vals.get("current", "N/A")
        parts = []
        for key in ["yoy_pct", "yoy_change", "mom_pct", "mom_change"]:
            if key in vals:
                parts.append(f"{key}={vals[key]}")
        changes = ", ".join(parts) if parts else "no changes calculated"
        print(f"  {desc}: {current}  ({changes})")

    if context_only:
        close_pool()
        return

    # Generate release
    print(f"\n{'='*60}")
    print(f"  Generating release via Claude API...")
    print(f"{'='*60}\n")

    result = config["generate_fn"](ref_period, dry_run=dry_run)

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
