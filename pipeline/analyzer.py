"""AI-powered release generation for Stats Bluenoser.

Pulls structured data from the database, injects it into prompt templates,
calls Claude API to generate analytical releases, and stores the results.
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import date
from decimal import Decimal
from pathlib import Path

import anthropic

from pipeline import db
from pipeline.config import AI_MAX_TOKENS, AI_MODEL

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"

# System prompt for all release generation
SYSTEM_PROMPT = (
    "You are an economic data analyst generating statistical releases for "
    "Nova Scotia. Your releases are read by executives, analysts, and "
    "policymakers who need accurate, concise summaries of the latest "
    "economic data. Be precise with numbers, neutral in tone, and "
    "analytical in structure. Never fabricate data or speculate beyond "
    "what the numbers show."
)


# -- Data context builders ---------------------------------------------------


def build_cpi_context(ref_period: date) -> dict:
    """Build the data context for a CPI release.

    Queries the database for the current month, prior month, and year-ago
    month across all CPI vectors (NS, Halifax, Canada, components).

    Args:
        ref_period: The reference period (first day of the month).

    Returns:
        Dict with structured data for template injection.
    """
    # Calculate comparison periods
    if ref_period.month == 1:
        prior_month = date(ref_period.year - 1, 12, 1)
    else:
        prior_month = date(ref_period.year, ref_period.month - 1, 1)
    year_ago = date(ref_period.year - 1, ref_period.month, 1)

    # Pull data for all three periods
    rows = db.execute(
        """SELECT s.description, s.geo_name, dp.ref_period, dp.value
           FROM data_points dp
           JOIN series s ON dp.series_id = s.series_id
           JOIN data_tables dt ON s.table_id = dt.table_id
           WHERE dt.source_pid = '18100004'
             AND dp.ref_period IN (%s, %s, %s)
           ORDER BY s.description, dp.ref_period""",
        (ref_period, prior_month, year_ago),
    )

    # Organize into a structured dict
    series_data = {}
    for row in rows:
        desc = row["description"]
        if desc not in series_data:
            series_data[desc] = {}
        period = row["ref_period"]
        value = float(row["value"]) if row["value"] is not None else None
        if period == ref_period:
            series_data[desc]["current"] = value
        elif period == prior_month:
            series_data[desc]["prior_month"] = value
        elif period == year_ago:
            series_data[desc]["year_ago"] = value

    # Calculate YoY and MoM changes
    for desc, values in series_data.items():
        current = values.get("current")
        year_ago_val = values.get("year_ago")
        prior_val = values.get("prior_month")

        if current is not None and year_ago_val is not None and year_ago_val != 0:
            values["yoy_pct"] = round((current - year_ago_val) / year_ago_val * 100, 1)
        if current is not None and prior_val is not None and prior_val != 0:
            values["mom_pct"] = round((current - prior_val) / prior_val * 100, 1)

    context = {
        "ref_period": ref_period.isoformat(),
        "ref_month": ref_period.strftime("%B"),
        "ref_year": ref_period.year,
        "prior_month_period": prior_month.isoformat(),
        "prior_month_name": prior_month.strftime("%B %Y"),
        "year_ago_period": year_ago.isoformat(),
        "year_ago_name": year_ago.strftime("%B %Y"),
        "series": series_data,
    }

    return context


# -- Prompt building ---------------------------------------------------------


def load_template(template_name: str) -> str:
    """Load a prompt template from the templates directory."""
    path = TEMPLATES_DIR / template_name
    if not path.exists():
        raise FileNotFoundError(f"Template not found: {path}")
    return path.read_text()


def build_analysis_prompt(template: str, data_context: dict) -> str:
    """Inject structured data into a prompt template.

    Replaces {data_json} with the formatted data context, and
    {ref_month_upper}, {ref_year} with formatted period strings.
    """
    data_json = json.dumps(data_context["series"], indent=2, default=str)

    prompt = template.replace("{data_json}", data_json)
    prompt = prompt.replace("{ref_month_upper}", data_context["ref_month"].upper())
    prompt = prompt.replace("{ref_year}", str(data_context["ref_year"]))

    # Add comparison period context
    context_note = (
        f"\nComparison periods:\n"
        f"- Current: {data_context['ref_month']} {data_context['ref_year']}\n"
        f"- Prior month: {data_context['prior_month_name']}\n"
        f"- Year ago: {data_context['year_ago_name']}\n"
    )
    prompt = prompt + context_note

    return prompt


# -- Claude API integration --------------------------------------------------


def generate_release(prompt: str) -> str:
    """Call Claude API to generate a release from a prompt.

    Args:
        prompt: The complete prompt with data and instructions.

    Returns:
        Generated markdown text.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set")

    client = anthropic.Anthropic(api_key=api_key)

    logger.info(f"Calling Claude API ({AI_MODEL})...")
    message = client.messages.create(
        model=AI_MODEL,
        max_tokens=AI_MAX_TOKENS,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )

    text = message.content[0].text
    logger.info(
        f"Release generated: {len(text)} chars, "
        f"input_tokens={message.usage.input_tokens}, "
        f"output_tokens={message.usage.output_tokens}"
    )
    return text


# -- Significance scoring ----------------------------------------------------


def calculate_significance_score(data_context: dict) -> float:
    """Calculate a simple significance score for a release.

    Heuristics:
    1. How far is the NS YoY rate from its recent trend?
    2. Did the direction of inflation change?
    3. Is NS diverging from the national trend?

    Returns a score from 0 (routine) to 1 (highly significant).
    """
    score = 0.0
    series = data_context.get("series", {})

    ns_all = series.get("Nova Scotia;All-items", {})
    ca_all = series.get("Canada;All-items", {})

    ns_yoy = ns_all.get("yoy_pct")
    ca_yoy = ca_all.get("yoy_pct")

    if ns_yoy is not None:
        # High inflation (>4%) or deflation (<0%) is notable
        if abs(ns_yoy) > 4:
            score += 0.3
        elif ns_yoy < 0:
            score += 0.4

    if ns_yoy is not None and ca_yoy is not None:
        # NS diverging from national by >1 percentage point
        divergence = abs(ns_yoy - ca_yoy)
        if divergence > 1.0:
            score += 0.3
        elif divergence > 0.5:
            score += 0.15

    # Check for direction change (compare current MoM to prior direction)
    ns_mom = ns_all.get("mom_pct")
    if ns_mom is not None:
        # Large monthly move (>0.5%) is notable for CPI
        if abs(ns_mom) > 0.5:
            score += 0.2

    return min(score, 1.0)


# -- Release record management ----------------------------------------------


def create_release_record(
    title: str,
    slug: str,
    body_markdown: str,
    topic_slug: str,
    ref_period: str,
    geography_scope: str = "Nova Scotia",
    source_table_pids: list[str] | None = None,
    significance_score: float | None = None,
    series_ids: list[int] | None = None,
) -> int:
    """Insert a release record into the database. Returns release_id."""
    # Look up topic_id
    topic = db.execute_one(
        "SELECT topic_id FROM topics WHERE slug = %s", (topic_slug,)
    )
    topic_id = topic["topic_id"] if topic else None

    row = db.execute_one(
        """INSERT INTO releases
           (title, slug, topic_id, ref_period, geography_scope,
            body_markdown, source_table_pids, ai_model, ai_generated,
            significance_score)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s)
           RETURNING release_id""",
        (
            title, slug, topic_id, ref_period, geography_scope,
            body_markdown, source_table_pids or [], AI_MODEL,
            significance_score,
        ),
    )
    release_id = row["release_id"]

    # Link release to series
    if series_ids:
        for sid in series_ids:
            db.execute(
                """INSERT INTO release_series (release_id, series_id)
                   VALUES (%s, %s) ON CONFLICT DO NOTHING""",
                (release_id, sid),
            )

    logger.info(f"Created release record: id={release_id} slug={slug}")
    return release_id


# -- High-level release generation -------------------------------------------


def generate_cpi_release(ref_period: date, dry_run: bool = False) -> dict:
    """Generate a complete CPI release for a given reference period.

    Args:
        ref_period: First day of the reference month.
        dry_run: If True, generate text but don't save to DB.

    Returns:
        Dict with title, slug, body, significance_score.
    """
    month_name = ref_period.strftime("%B")
    year = ref_period.year
    title = f"CONSUMER PRICE INDEX, {month_name.upper()} {year}"
    slug = f"cpi-{month_name.lower()}-{year}"

    logger.info(f"Generating CPI release for {month_name} {year}...")

    # Build data context
    context = build_cpi_context(ref_period)

    # Check we have data
    if not context["series"]:
        raise ValueError(f"No CPI data found for {ref_period}")

    ns_all = context["series"].get("Nova Scotia;All-items", {})
    if "current" not in ns_all:
        raise ValueError(f"No NS All-items CPI data for {ref_period}")

    # Build prompt
    template = load_template("cpi_release.md")
    prompt = build_analysis_prompt(template, context)

    # Generate release text
    body_markdown = generate_release(prompt)

    # Calculate significance
    significance = calculate_significance_score(context)

    result = {
        "title": title,
        "slug": slug,
        "body_markdown": body_markdown,
        "ref_period": f"{month_name} {year}",
        "significance_score": significance,
        "data_context": context,
    }

    if not dry_run:
        # Get series IDs for linking
        series_rows = db.execute(
            """SELECT s.series_id FROM series s
               JOIN data_tables dt ON s.table_id = dt.table_id
               WHERE dt.source_pid = '18100004'""",
        )
        series_ids = [r["series_id"] for r in series_rows]

        release_id = create_release_record(
            title=title,
            slug=slug,
            body_markdown=body_markdown,
            topic_slug="consumer-price-index",
            ref_period=f"{month_name} {year}",
            source_table_pids=["18-10-0004-01"],
            significance_score=significance,
            series_ids=series_ids,
        )
        result["release_id"] = release_id

    logger.info(
        f"CPI release generated: {title} (significance={significance:.2f})"
    )
    return result


# -- Labour Force Survey (LFS) release --------------------------------------


def build_lfs_context(ref_period: date) -> dict:
    """Build the data context for a Labour Market Trends release.

    Queries the database for the current month, prior month, and year-ago
    month across all LFS vectors (headlines, age, gender breakdowns).

    Args:
        ref_period: The reference period (first day of the month).

    Returns:
        Dict with structured data for template injection.
    """
    if ref_period.month == 1:
        prior_month = date(ref_period.year - 1, 12, 1)
    else:
        prior_month = date(ref_period.year, ref_period.month - 1, 1)
    year_ago = date(ref_period.year - 1, ref_period.month, 1)

    rows = db.execute(
        """SELECT s.description, s.geo_name, dp.ref_period, dp.value
           FROM data_points dp
           JOIN series s ON dp.series_id = s.series_id
           JOIN data_tables dt ON s.table_id = dt.table_id
           WHERE dt.source_pid = '14100287'
             AND dp.ref_period IN (%s, %s, %s)
           ORDER BY s.description, dp.ref_period""",
        (ref_period, prior_month, year_ago),
    )

    series_data = {}
    for row in rows:
        desc = row["description"]
        if desc not in series_data:
            series_data[desc] = {}
        period = row["ref_period"]
        value = float(row["value"]) if row["value"] is not None else None
        if period == ref_period:
            series_data[desc]["current"] = value
        elif period == prior_month:
            series_data[desc]["prior_month"] = value
        elif period == year_ago:
            series_data[desc]["year_ago"] = value

    # Calculate changes
    for desc, values in series_data.items():
        current = values.get("current")
        year_ago_val = values.get("year_ago")
        prior_val = values.get("prior_month")

        # For rates (unemployment rate, etc.): report point changes
        # For levels (employment, etc.): report level and percentage changes
        is_rate = any(
            r in desc for r in ["rate", "Participation rate", "Employment rate"]
        )

        if current is not None and prior_val is not None:
            values["mom_change"] = round(current - prior_val, 1)
            if not is_rate and prior_val != 0:
                values["mom_pct"] = round(
                    (current - prior_val) / prior_val * 100, 1
                )

        if current is not None and year_ago_val is not None:
            values["yoy_change"] = round(current - year_ago_val, 1)
            if not is_rate and year_ago_val != 0:
                values["yoy_pct"] = round(
                    (current - year_ago_val) / year_ago_val * 100, 1
                )

    return {
        "ref_period": ref_period.isoformat(),
        "ref_month": ref_period.strftime("%B"),
        "ref_year": ref_period.year,
        "prior_month_period": prior_month.isoformat(),
        "prior_month_name": prior_month.strftime("%B %Y"),
        "year_ago_period": year_ago.isoformat(),
        "year_ago_name": year_ago.strftime("%B %Y"),
        "series": series_data,
    }


def generate_lfs_release(ref_period: date, dry_run: bool = False) -> dict:
    """Generate a Labour Market Trends release for a given reference period."""
    month_name = ref_period.strftime("%B")
    year = ref_period.year
    title = f"LABOUR MARKET TRENDS, {month_name.upper()} {year}"
    slug = f"labour-market-{month_name.lower()}-{year}"

    logger.info(f"Generating LFS release for {month_name} {year}...")

    context = build_lfs_context(ref_period)

    if not context["series"]:
        raise ValueError(f"No LFS data found for {ref_period}")

    ns_emp = context["series"].get("Nova Scotia;Employment", {})
    if "current" not in ns_emp:
        raise ValueError(f"No NS Employment data for {ref_period}")

    template = load_template("labour_release.md")
    prompt = build_analysis_prompt(template, context)
    body_markdown = generate_release(prompt)

    # Significance: check unemployment rate changes
    ns_ur = context["series"].get("Nova Scotia;Unemployment rate", {})
    significance = 0.0
    ca_ur = context["series"].get("Canada;Unemployment rate", {})
    ns_ur_current = ns_ur.get("current")
    ca_ur_current = ca_ur.get("current")
    ns_ur_mom = ns_ur.get("mom_change")

    if ns_ur_mom is not None and abs(ns_ur_mom) >= 0.5:
        significance += 0.3
    if ns_ur_current is not None and ca_ur_current is not None:
        gap = abs(ns_ur_current - ca_ur_current)
        if gap > 1.5:
            significance += 0.3
    ns_ur_yoy = ns_ur.get("yoy_change")
    if ns_ur_yoy is not None and abs(ns_ur_yoy) >= 1.0:
        significance += 0.2

    significance = min(significance, 1.0)

    result = {
        "title": title,
        "slug": slug,
        "body_markdown": body_markdown,
        "ref_period": f"{month_name} {year}",
        "significance_score": significance,
        "data_context": context,
    }

    if not dry_run:
        series_rows = db.execute(
            """SELECT s.series_id FROM series s
               JOIN data_tables dt ON s.table_id = dt.table_id
               WHERE dt.source_pid = '14100287'""",
        )
        series_ids = [r["series_id"] for r in series_rows]

        release_id = create_release_record(
            title=title,
            slug=slug,
            body_markdown=body_markdown,
            topic_slug="labour-market-monthly",
            ref_period=f"{month_name} {year}",
            source_table_pids=["14-10-0287-01"],
            significance_score=significance,
            series_ids=series_ids,
        )
        result["release_id"] = release_id

    logger.info(
        f"LFS release generated: {title} (significance={significance:.2f})"
    )
    return result
