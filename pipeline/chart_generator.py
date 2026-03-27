"""Static chart generation for Stats Bluenoser.

Generates clean, professional PNG charts from database time-series data
for embedding in releases and the Hugo site.

Style: minimal, accessible, inspired by government statistical publications.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # Non-interactive backend for server/CI use
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker

from pipeline import db

logger = logging.getLogger(__name__)

CHARTS_DIR = Path(__file__).parent.parent / "site" / "static" / "charts"

# Accessible colour palette (colourblind-safe, high contrast)
COLOURS = {
    "Nova Scotia": "#1d4ed8",    # blue
    "Canada": "#6b7280",         # grey
    "Halifax": "#059669",        # green
    "Men+": "#2563eb",           # blue
    "Women+": "#dc2626",         # red
    "15 to 24 years": "#f59e0b", # amber
    "25 to 54 years": "#1d4ed8", # blue
    "55 years and over": "#059669",  # green
}
DEFAULT_COLOUR = "#1d4ed8"

# Chart styling
plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor": "white",
    "axes.edgecolor": "#d4d4d4",
    "axes.grid": True,
    "grid.color": "#e5e5e5",
    "grid.linewidth": 0.5,
    "font.family": "sans-serif",
    "font.size": 10,
    "axes.titlesize": 12,
    "axes.titleweight": "bold",
    "axes.labelsize": 10,
    "legend.fontsize": 9,
    "legend.frameon": False,
    "figure.dpi": 150,
})


def _get_colour(label: str) -> str:
    """Get colour for a series label, checking partial matches."""
    for key, colour in COLOURS.items():
        if key in label:
            return colour
    return DEFAULT_COLOUR


def _save_chart(fig: plt.Figure, slug: str) -> Path:
    """Save chart to the static charts directory."""
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    path = CHARTS_DIR / f"{slug}.png"
    fig.savefig(path, bbox_inches="tight", pad_inches=0.3)
    plt.close(fig)
    logger.info(f"Chart saved: {path}")
    return path


def generate_cpi_chart(months: int = 24) -> Path:
    """Generate a line chart: NS CPI vs. national over the last N months.

    Returns the path to the saved PNG.
    """
    # Pull data from DB
    rows = db.execute(
        """SELECT s.description, dp.ref_period, dp.value
           FROM data_points dp
           JOIN series s ON dp.series_id = s.series_id
           JOIN data_tables dt ON s.table_id = dt.table_id
           WHERE dt.source_pid = '18100004'
             AND s.description IN (
                 'Nova Scotia;All-items',
                 'Canada;All-items',
                 'Halifax;All-items'
             )
             AND dp.ref_period >= (
                 SELECT MAX(ref_period) - interval '%s months'
                 FROM data_points dp2
                 JOIN series s2 ON dp2.series_id = s2.series_id
                 JOIN data_tables dt2 ON s2.table_id = dt2.table_id
                 WHERE dt2.source_pid = '18100004'
             )
           ORDER BY s.description, dp.ref_period""",
        (months,),
    )

    # Organise by series
    series: dict[str, tuple[list[date], list[float]]] = {}
    for row in rows:
        desc = row["description"]
        geo = desc.split(";")[0]
        if geo not in series:
            series[geo] = ([], [])
        series[geo][0].append(row["ref_period"])
        series[geo][1].append(float(row["value"]))

    # Plot
    fig, ax = plt.subplots(figsize=(8, 4))
    for label, (dates, values) in series.items():
        ax.plot(dates, values, label=label, color=_get_colour(label), linewidth=1.8)

    ax.set_title("Consumer Price Index (2002=100)")
    ax.set_ylabel("Index")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    fig.autofmt_xdate(rotation=45)
    ax.legend(loc="upper left")

    return _save_chart(fig, "cpi-trend")


def generate_labour_chart(months: int = 24) -> Path:
    """Generate a multi-series chart: unemployment rate by geography.

    Returns the path to the saved PNG.
    """
    rows = db.execute(
        """SELECT s.description, dp.ref_period, dp.value
           FROM data_points dp
           JOIN series s ON dp.series_id = s.series_id
           JOIN data_tables dt ON s.table_id = dt.table_id
           WHERE dt.source_pid = '14100287'
             AND s.description IN (
                 'Nova Scotia;Unemployment rate',
                 'Canada;Unemployment rate'
             )
             AND dp.ref_period >= (
                 SELECT MAX(ref_period) - interval '%s months'
                 FROM data_points dp2
                 JOIN series s2 ON dp2.series_id = s2.series_id
                 JOIN data_tables dt2 ON s2.table_id = dt2.table_id
                 WHERE dt2.source_pid = '14100287'
             )
           ORDER BY s.description, dp.ref_period""",
        (months,),
    )

    series: dict[str, tuple[list[date], list[float]]] = {}
    for row in rows:
        desc = row["description"]
        geo = desc.split(";")[0]
        if geo not in series:
            series[geo] = ([], [])
        series[geo][0].append(row["ref_period"])
        series[geo][1].append(float(row["value"]))

    fig, ax = plt.subplots(figsize=(8, 4))
    for label, (dates, values) in series.items():
        ax.plot(dates, values, label=label, color=_get_colour(label), linewidth=1.8)

    ax.set_title("Unemployment Rate, Seasonally Adjusted")
    ax.set_ylabel("Rate (%)")
    ax.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.1f%%"))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    fig.autofmt_xdate(rotation=45)
    ax.legend(loc="upper left")

    return _save_chart(fig, "unemployment-rate-trend")


def generate_generic_chart(
    source_pid: str,
    descriptions: list[str],
    title: str,
    ylabel: str,
    slug: str,
    months: int = 24,
    is_rate: bool = False,
) -> Path:
    """Generate a chart for any set of series from a given table.

    Args:
        source_pid: Table PID.
        descriptions: List of series descriptions to include.
        title: Chart title.
        ylabel: Y-axis label.
        slug: Filename slug for the saved PNG.
        months: Number of months of history.
        is_rate: If True, format y-axis as percentage.

    Returns:
        Path to the saved PNG.
    """
    placeholders = ", ".join(["%s"] * len(descriptions))
    params = tuple(descriptions) + (source_pid, months)

    rows = db.execute(
        f"""SELECT s.description, dp.ref_period, dp.value
           FROM data_points dp
           JOIN series s ON dp.series_id = s.series_id
           JOIN data_tables dt ON s.table_id = dt.table_id
           WHERE s.description IN ({placeholders})
             AND dt.source_pid = %s
             AND dp.ref_period >= (
                 SELECT MAX(ref_period) - interval '%s months'
                 FROM data_points dp2
                 JOIN series s2 ON dp2.series_id = s2.series_id
                 JOIN data_tables dt2 ON s2.table_id = dt2.table_id
                 WHERE dt2.source_pid = %s
             )
           ORDER BY s.description, dp.ref_period""",
        params + (source_pid,),
    )

    series: dict[str, tuple[list[date], list[float]]] = {}
    for row in rows:
        desc = row["description"]
        # Use last part of description as label (after last semicolon)
        label = desc.split(";")[-1].strip() if ";" in desc else desc
        if label not in series:
            series[label] = ([], [])
        series[label][0].append(row["ref_period"])
        series[label][1].append(float(row["value"]))

    fig, ax = plt.subplots(figsize=(8, 4))
    for label, (dates, values) in series.items():
        ax.plot(dates, values, label=label, color=_get_colour(label), linewidth=1.8)

    ax.set_title(title)
    ax.set_ylabel(ylabel)
    if is_rate:
        ax.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.1f%%"))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    fig.autofmt_xdate(rotation=45)
    if len(series) > 1:
        ax.legend(loc="best")

    return _save_chart(fig, slug)
