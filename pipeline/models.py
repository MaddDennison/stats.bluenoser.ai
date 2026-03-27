"""Data models for the Stats Bluenoser pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal


@dataclass
class Source:
    source_id: int
    name: str
    api_base_url: str | None = None
    api_type: str | None = None
    licence_url: str | None = None
    config_json: dict | None = None


@dataclass
class DataTable:
    table_id: int | None
    source_id: int
    source_pid: str
    title: str
    frequency: str | None = None
    subject_code: str | None = None
    survey_code: str | None = None
    last_source_update: datetime | None = None
    metadata_json: dict | None = None
    active: bool = True


@dataclass
class Series:
    series_id: int | None
    table_id: int
    vector_id: str
    coordinate: str | None = None
    geo_name: str | None = None
    geo_code: str | None = None
    unit_of_measure: str | None = None
    scalar_factor: int = 0
    description: str | None = None
    dimension_labels: dict | None = None


@dataclass
class DataPoint:
    series_id: int
    ref_period: date
    value: Decimal | None = None
    status_code: str | None = None
    symbol_code: str | None = None
    decimal_precision: int | None = None
    release_date: datetime | None = None


@dataclass
class Revision:
    revision_id: int | None
    series_id: int
    ref_period: date
    previous_value: Decimal | None = None
    new_value: Decimal | None = None
    detected_at: datetime | None = None


@dataclass
class Release:
    release_id: int | None
    title: str
    slug: str
    body_markdown: str
    topic_id: int | None = None
    ref_period: str | None = None
    geography_scope: str | None = None
    body_html: str | None = None
    charts_json: dict | None = None
    source_table_pids: list[str] = field(default_factory=list)
    ai_model: str | None = None
    ai_generated: bool = True
    reviewed: bool = False
    published: bool = False
    published_at: datetime | None = None
    significance_score: Decimal | None = None
