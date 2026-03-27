"""Tests for the analyzer — prompt building, context assembly, significance scoring."""

from __future__ import annotations

from datetime import date

import pytest

from pipeline.analyzer import (
    build_analysis_prompt,
    calculate_significance_score,
    load_template,
)


class TestLoadTemplate:
    def test_loads_cpi_template(self):
        template = load_template("cpi_release.md")
        assert "{data_json}" in template
        assert "{ref_month_upper}" in template
        assert "CONSUMER PRICE INDEX" in template

    def test_loads_labour_template(self):
        template = load_template("labour_release.md")
        assert "{data_json}" in template
        assert "LABOUR MARKET TRENDS" in template

    def test_loads_generic_template(self):
        template = load_template("generic_release.md")
        assert "{data_json}" in template

    def test_missing_template_raises(self):
        with pytest.raises(FileNotFoundError):
            load_template("nonexistent.md")


class TestBuildAnalysisPrompt:
    def test_injects_data(self):
        template = "Data: {data_json}\nMonth: {ref_month_upper} {ref_year}"
        context = {
            "ref_month": "February",
            "ref_year": 2026,
            "prior_month_name": "January 2026",
            "year_ago_name": "February 2025",
            "series": {"Nova Scotia;All-items": {"current": 169.2}},
        }
        prompt = build_analysis_prompt(template, context)

        assert "169.2" in prompt
        assert "FEBRUARY" in prompt
        assert "2026" in prompt
        assert "January 2026" in prompt

    def test_handles_empty_series(self):
        template = "Data: {data_json}"
        context = {
            "ref_month": "March",
            "ref_year": 2026,
            "prior_month_name": "February 2026",
            "year_ago_name": "March 2025",
            "series": {},
        }
        prompt = build_analysis_prompt(template, context)
        assert "{}" in prompt


class TestCalculateSignificanceScore:
    def test_routine_data(self):
        context = {
            "series": {
                "Nova Scotia;All-items": {
                    "current": 169.2,
                    "yoy_pct": 1.7,
                    "mom_pct": 0.2,
                },
                "Canada;All-items": {
                    "current": 165.9,
                    "yoy_pct": 1.8,
                },
            }
        }
        score = calculate_significance_score(context)
        assert score == 0.0  # routine, no flags

    def test_high_inflation(self):
        context = {
            "series": {
                "Nova Scotia;All-items": {
                    "current": 180.0,
                    "yoy_pct": 5.5,
                    "mom_pct": 0.3,
                },
                "Canada;All-items": {
                    "current": 170.0,
                    "yoy_pct": 5.0,
                },
            }
        }
        score = calculate_significance_score(context)
        assert score > 0.0  # high inflation flagged

    def test_ns_divergence(self):
        context = {
            "series": {
                "Nova Scotia;All-items": {
                    "current": 180.0,
                    "yoy_pct": 5.0,
                    "mom_pct": 0.1,
                },
                "Canada;All-items": {
                    "current": 170.0,
                    "yoy_pct": 2.0,
                },
            }
        }
        score = calculate_significance_score(context)
        assert score >= 0.3  # divergence > 1 pp flagged

    def test_deflation(self):
        context = {
            "series": {
                "Nova Scotia;All-items": {
                    "current": 160.0,
                    "yoy_pct": -0.5,
                    "mom_pct": -0.2,
                },
                "Canada;All-items": {
                    "current": 165.0,
                    "yoy_pct": 0.5,
                },
            }
        }
        score = calculate_significance_score(context)
        assert score >= 0.4  # deflation flagged

    def test_score_capped_at_1(self):
        context = {
            "series": {
                "Nova Scotia;All-items": {
                    "current": 160.0,
                    "yoy_pct": -2.0,
                    "mom_pct": -1.5,
                },
                "Canada;All-items": {
                    "current": 175.0,
                    "yoy_pct": 5.0,
                },
            }
        }
        score = calculate_significance_score(context)
        assert score <= 1.0

    def test_missing_data(self):
        context = {"series": {}}
        score = calculate_significance_score(context)
        assert score == 0.0
