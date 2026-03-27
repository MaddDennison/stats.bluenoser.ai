"""Publishing pipeline for Stats Bluenoser.

Converts release records from the database into Hugo content files,
builds the static site, sends newsletters via Resend, and optionally
deploys via git push.
"""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime
from pathlib import Path

from pipeline import db
from pipeline.config import WATCHLIST

logger = logging.getLogger(__name__)

SITE_DIR = Path(__file__).parent.parent / "site"
CONTENT_DIR = SITE_DIR / "content" / "releases"
TEMPLATES_DIR = Path(__file__).parent / "templates"
SITE_URL = "https://stats.bluenoser.ai"


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


# -- Newsletter --------------------------------------------------------------


def compile_daily_digest(releases: list[dict], date_str: str | None = None) -> tuple[str, str]:
    """Build the daily digest email HTML from today's releases.

    Args:
        releases: List of release dicts (from DB query).
        date_str: Optional override for the date line.

    Returns:
        Tuple of (subject, html_body).
    """
    if not date_str:
        date_str = datetime.now().strftime("%B %d, %Y")

    subject = f"NS Economic Data — {date_str}"

    # Build individual release HTML blocks
    if releases:
        blocks = []
        for r in releases:
            title = r.get("title", "Untitled")
            slug = r.get("slug", "")
            ref_period = r.get("ref_period", "")
            body = r.get("body_markdown", "")
            # Extract first sentence as summary
            summary = body.split(". ")[0] + "." if body else ""
            if len(summary) > 250:
                summary = summary[:247] + "..."

            topic = r.get("topic_slug", "")

            block = (
                f'<div class="release">'
                f'<h2><a href="{SITE_URL}/releases/{slug}/">{title}</a></h2>'
                f'<div class="meta">{ref_period}'
                f'{f" — {topic}" if topic else ""}</div>'
                f'<div class="summary">{summary}</div>'
                f'</div>'
            )
            blocks.append(block)

        releases_html = "\n".join(blocks)
    else:
        releases_html = '<p class="no-releases">No new releases today.</p>'

    # Load and fill template
    template_path = TEMPLATES_DIR / "daily_digest.html"
    template = template_path.read_text()

    html = template.replace("{subject}", subject)
    html = html.replace("{date_formatted}", date_str)
    html = html.replace("{releases_html}", releases_html)
    html = html.replace("{site_url}", SITE_URL)

    return subject, html


def send_newsletter(subject: str, html_body: str, to: list[str] | None = None) -> bool:
    """Send the daily digest email via Resend API.

    Args:
        subject: Email subject line.
        html_body: Full HTML email body.
        to: List of recipient email addresses. If None, uses NEWSLETTER_RECIPIENTS env var.

    Returns:
        True if send succeeded.
    """
    api_key = os.environ.get("RESEND_API_KEY")
    if not api_key:
        logger.info("RESEND_API_KEY not set — skipping newsletter send")
        return False

    if not to:
        recipients_env = os.environ.get("NEWSLETTER_RECIPIENTS", "")
        to = [e.strip() for e in recipients_env.split(",") if e.strip()]

    if not to:
        logger.warning("No newsletter recipients configured")
        return False

    try:
        import resend

        resend.api_key = api_key
        from_addr = os.environ.get("NEWSLETTER_FROM", "Stats Bluenoser <digest@stats.bluenoser.ai>")

        result = resend.Emails.send({
            "from": from_addr,
            "to": to,
            "subject": subject,
            "html": html_body,
        })

        logger.info(f"Newsletter sent: {subject} to {len(to)} recipient(s)")
        log_newsletter_send(subject, len(to))
        return True

    except Exception as e:
        logger.error(f"Newsletter send failed: {e}")
        return False


def log_newsletter_send(subject: str, recipient_count: int):
    """Record a newsletter send in the database."""
    db.execute(
        """INSERT INTO newsletter_sends (subject, recipient_count)
           VALUES (%s, %s)""",
        (subject, recipient_count),
    )
