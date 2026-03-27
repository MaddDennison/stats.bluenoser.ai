"""Publishing pipeline for Stats Bluenoser.

Converts release records from the database into Hugo content files,
builds the static site, and optionally deploys via git push.
"""

from __future__ import annotations

import logging
import subprocess
from datetime import datetime
from pathlib import Path

from pipeline import db

logger = logging.getLogger(__name__)

SITE_DIR = Path(__file__).parent.parent / "site"
CONTENT_DIR = SITE_DIR / "content" / "releases"


def generate_hugo_markdown(release: dict) -> Path:
    """Convert a release DB record to a Hugo content file.

    Args:
        release: Dict with keys from the releases table:
            title, slug, body_markdown, published_at (or created_at),
            ref_period, geography_scope, source_table_pids, topic_slug, etc.

    Returns:
        Path to the generated markdown file.
    """
    slug = release["slug"]
    title = release["title"]
    body = release["body_markdown"]

    # Determine date for Hugo front matter
    pub_date = release.get("published_at") or release.get("created_at") or datetime.now()
    if isinstance(pub_date, str):
        pub_date = datetime.fromisoformat(pub_date)
    date_str = pub_date.strftime("%Y-%m-%dT%H:%M:%S%z") or pub_date.strftime("%Y-%m-%d")

    # Build front matter
    front_matter_lines = [
        "---",
        f'title: "{title}"',
        f"date: {date_str}",
    ]

    if release.get("ref_period"):
        front_matter_lines.append(f'ref_period: "{release["ref_period"]}"')

    if release.get("geography_scope"):
        front_matter_lines.append(f'geography: "{release["geography_scope"]}"')

    # Topic (for Hugo taxonomy)
    topic_slug = release.get("topic_slug")
    if topic_slug:
        front_matter_lines.append(f"topics:")
        front_matter_lines.append(f'  - "{topic_slug}"')

    # Source tables
    pids = release.get("source_table_pids")
    if pids and isinstance(pids, list):
        front_matter_lines.append(f"source_tables:")
        for pid in pids:
            front_matter_lines.append(f'  - "{pid}"')

    if release.get("ai_generated", True):
        front_matter_lines.append("ai_generated: true")

    front_matter_lines.append("---")

    content = "\n".join(front_matter_lines) + "\n\n" + body

    # Write file
    CONTENT_DIR.mkdir(parents=True, exist_ok=True)
    filepath = CONTENT_DIR / f"{slug}.md"
    filepath.write_text(content)

    logger.info(f"Generated Hugo content: {filepath}")
    return filepath


def publish_releases(published_only: bool = True) -> list[Path]:
    """Generate Hugo markdown for all releases in the database.

    Args:
        published_only: If True, only publish releases marked as published.
            If False, publish all releases (useful for backfilling).

    Returns:
        List of paths to generated files.
    """
    if published_only:
        releases = db.execute(
            """SELECT r.*, t.slug as topic_slug
               FROM releases r
               LEFT JOIN topics t ON r.topic_id = t.topic_id
               WHERE r.published = TRUE
               ORDER BY r.published_at DESC""",
        )
    else:
        releases = db.execute(
            """SELECT r.*, t.slug as topic_slug
               FROM releases r
               LEFT JOIN topics t ON r.topic_id = t.topic_id
               ORDER BY r.created_at DESC""",
        )

    paths = []
    for release in releases:
        try:
            path = generate_hugo_markdown(release)
            paths.append(path)
        except Exception as e:
            logger.error(f"Failed to generate content for {release.get('slug')}: {e}")

    logger.info(f"Published {len(paths)} releases to Hugo content directory")
    return paths


def build_site() -> bool:
    """Run `hugo build` to generate the static site.

    Returns:
        True if build succeeded, False otherwise.
    """
    try:
        result = subprocess.run(
            ["hugo"],
            cwd=SITE_DIR,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode == 0:
            logger.info(f"Hugo build succeeded: {result.stdout.strip()}")
            return True
        else:
            logger.error(f"Hugo build failed: {result.stderr}")
            return False
    except FileNotFoundError:
        logger.error("Hugo not found. Install it: brew install hugo")
        return False
    except subprocess.TimeoutExpired:
        logger.error("Hugo build timed out after 60 seconds")
        return False


def deploy_site() -> bool:
    """Deploy the site by committing and pushing to trigger Cloudflare Pages.

    Returns:
        True if deploy succeeded, False otherwise.
    """
    try:
        project_root = SITE_DIR.parent

        # Check for changes
        status = subprocess.run(
            ["git", "status", "--porcelain", "site/"],
            cwd=project_root,
            capture_output=True,
            text=True,
        )
        if not status.stdout.strip():
            logger.info("No site changes to deploy")
            return True

        # Stage site changes
        subprocess.run(
            ["git", "add", "site/content/", "site/public/"],
            cwd=project_root,
            check=True,
            capture_output=True,
        )

        # Commit
        subprocess.run(
            ["git", "commit", "-m", f"Publish releases — {datetime.now().strftime('%Y-%m-%d')}"],
            cwd=project_root,
            check=True,
            capture_output=True,
        )

        # Push
        subprocess.run(
            ["git", "push"],
            cwd=project_root,
            check=True,
            capture_output=True,
            timeout=30,
        )

        logger.info("Site deployed via git push")
        return True

    except subprocess.CalledProcessError as e:
        logger.error(f"Deploy failed: {e.stderr if hasattr(e, 'stderr') else e}")
        return False
    except subprocess.TimeoutExpired:
        logger.error("Git push timed out")
        return False
