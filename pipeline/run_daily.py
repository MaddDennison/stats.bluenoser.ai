"""Daily pipeline entry point for Stats Bluenoser.

Runs the full CHECK → INGEST → ANALYZE → LOG cycle.

Usage:
    python -m pipeline.run_daily              # Normal daily run
    python -m pipeline.run_daily --force      # Skip change detection, ingest all watchlist tables
    python -m pipeline.run_daily --no-analyze # Ingest only, skip AI release generation

Schedule: cron `31 8 * * 1-5` (08:31 ET, weekdays)
"""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass, field
from datetime import date

from pipeline import db
from pipeline.config import WATCHLIST
from pipeline.db import close_pool
from pipeline.ingester import IngestionStats, ingest_from_vectors
from pipeline.statcan_client import StatCanClient, StatCanError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """Summary of a daily pipeline run."""

    date: date
    tables_checked: int = 0
    tables_updated: int = 0
    tables_ingested: int = 0
    releases_generated: int = 0
    total_inserted: int = 0
    total_updated: int = 0
    total_revisions: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def exit_code(self) -> int:
        if not self.errors:
            return 0
        if self.tables_ingested > 0:
            return 1  # partial failure
        return 2  # total failure

    def __str__(self):
        status = "OK" if not self.errors else f"{len(self.errors)} error(s)"
        return (
            f"Pipeline run {self.date} — {status}\n"
            f"  Tables: {self.tables_checked} checked, "
            f"{self.tables_updated} updated, {self.tables_ingested} ingested\n"
            f"  Data: {self.total_inserted} inserted, "
            f"{self.total_updated} updated, {self.total_revisions} revisions\n"
            f"  Releases: {self.releases_generated} generated"
        )


def check_for_updates(client: StatCanClient, today: date) -> set[str]:
    """Step 1 — CHECK: which watchlist tables updated today?"""
    logger.info(f"Checking for updates since {today}...")

    try:
        changes = client.get_changed_cube_list(today)
    except StatCanError as e:
        logger.error(f"Failed to check for updates: {e}")
        return set()

    if not isinstance(changes, list):
        logger.warning(f"Unexpected response from getChangedCubeList: {type(changes)}")
        return set()

    # Filter against our watchlist
    changed_pids = set()
    for change in changes:
        pid = str(change.get("productId", ""))
        if pid in WATCHLIST:
            changed_pids.add(pid)
            logger.info(
                f"  Watchlist table updated: {pid} — "
                f"{WATCHLIST[pid]['title']} (released {change.get('releaseTime', 'unknown')})"
            )

    if not changed_pids:
        logger.info("  No watchlist tables updated today.")

    return changed_pids


def ingest_tables(
    client: StatCanClient, pids: set[str], result: PipelineResult
):
    """Step 2 — INGEST: pull updated vectors and upsert data."""
    for pid in sorted(pids):
        config = WATCHLIST[pid]
        vectors = config.get("vectors", {})
        if not vectors:
            logger.warning(f"  {pid}: no vectors configured — skipping")
            continue

        logger.info(f"Ingesting {pid}: {config['title']} ({len(vectors)} vectors)...")
        try:
            stats = ingest_from_vectors(client, pid, vectors, n_periods=3)
            result.tables_ingested += 1
            result.total_inserted += stats.points_inserted
            result.total_updated += stats.points_updated
            result.total_revisions += stats.revisions_detected
            logger.info(f"  {pid}: {stats}")
        except Exception as e:
            result.errors.append(f"Ingest {pid}: {e}")
            logger.error(f"  {pid}: ingestion failed — {e}")


def analyze_tables(pids: set[str], result: PipelineResult):
    """Step 3 — ANALYZE: generate AI releases for updated tables.

    Only runs for tables that have prompt templates and an API key configured.
    """
    import os

    if not os.environ.get("ANTHROPIC_API_KEY"):
        logger.info("ANTHROPIC_API_KEY not set — skipping analysis step")
        return

    for pid in sorted(pids):
        config = WATCHLIST[pid]
        topic_slug = config.get("topic_slug", "")

        # Map topic slugs to generator functions
        generators = {
            "consumer-price-index": "generate_cpi_release",
            "labour-market-monthly": "generate_lfs_release",
        }

        generator_name = generators.get(topic_slug)
        if not generator_name:
            logger.info(f"  {pid}: no prompt template yet — skipping analysis")
            continue

        try:
            from pipeline import analyzer, db

            generator_fn = getattr(analyzer, generator_name)

            row = db.execute_one(
                """SELECT MAX(dp.ref_period) as latest
                   FROM data_points dp
                   JOIN series s ON dp.series_id = s.series_id
                   JOIN data_tables dt ON s.table_id = dt.table_id
                   WHERE dt.source_pid = %s""",
                (pid,),
            )
            if row and row["latest"]:
                ref_period = row["latest"]
                logger.info(f"Generating {topic_slug} release for {ref_period}...")
                release = generator_fn(ref_period)
                result.releases_generated += 1
                logger.info(
                    f"  Generated: {release['title']} "
                    f"(significance={release['significance_score']:.2f})"
                )
        except Exception as e:
            result.errors.append(f"Analyze {pid}: {e}")
            logger.error(f"  {pid}: analysis failed — {e}")


def main():
    force = "--force" in sys.argv
    no_analyze = "--no-analyze" in sys.argv
    today = date.today()

    logger.info(f"Stats Bluenoser — Daily Pipeline")
    logger.info(f"Date: {today}")
    logger.info(f"Watchlist: {len(WATCHLIST)} tables")
    if force:
        logger.info("Mode: FORCE (ingesting all watchlist tables)")
    if no_analyze:
        logger.info("Mode: NO-ANALYZE (skipping AI release generation)")

    result = PipelineResult(date=today)

    with StatCanClient() as client:
        # Step 1 — CHECK
        if force:
            updated_pids = {
                pid
                for pid, cfg in WATCHLIST.items()
                if cfg.get("vectors")
            }
            result.tables_checked = len(WATCHLIST)
            result.tables_updated = len(updated_pids)
        else:
            result.tables_checked = len(WATCHLIST)
            updated_pids = check_for_updates(client, today)
            result.tables_updated = len(updated_pids)

        if not updated_pids:
            logger.info("No updates — pipeline complete.")
            close_pool()
            print(result)
            sys.exit(0)

        # Step 2 — INGEST
        ingest_tables(client, updated_pids, result)

    # Step 3 — ANALYZE
    if not no_analyze and result.tables_ingested > 0:
        analyze_tables(updated_pids, result)

    # Step 4 — CHARTS
    if result.tables_ingested > 0:
        try:
            from pipeline.chart_generator import generate_cpi_chart, generate_labour_chart

            logger.info("Generating charts...")
            if "18100004" in updated_pids:
                generate_cpi_chart()
            if "14100287" in updated_pids:
                generate_labour_chart()
        except Exception as e:
            logger.warning(f"Chart generation failed (non-fatal): {e}")

    # Step 5 — PUBLISH
    if result.releases_generated > 0:
        try:
            from pipeline.publisher import (
                build_site, compile_daily_digest, publish_releases, send_newsletter,
            )

            logger.info("Publishing releases to Hugo site...")
            paths = publish_releases(published_only=False)
            if paths:
                build_site()

            # Send newsletter digest
            releases = db.execute(
                """SELECT r.*, t.slug as topic_slug
                   FROM releases r
                   LEFT JOIN topics t ON r.topic_id = t.topic_id
                   WHERE r.created_at::date = CURRENT_DATE
                   ORDER BY r.created_at DESC""",
            )
            if releases:
                subject, html = compile_daily_digest(releases)
                send_newsletter(subject, html)

        except Exception as e:
            result.errors.append(f"Publish: {e}")
            logger.error(f"Publishing failed: {e}")

    # Step 5 — LOG
    logger.info(f"\n{result}")
    if result.errors:
        logger.error("Errors:")
        for err in result.errors:
            logger.error(f"  - {err}")

    close_pool()
    sys.exit(result.exit_code)


if __name__ == "__main__":
    main()
