"""
Tests for FirmsEnricher and BankEnricher.

Run with:
    pytest tests/ -v
"""

import json
import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.bank_enricher import BankEnricher
from src.firms_enricher import FirmsEnricher


# ──────────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def set_env(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-key")


@pytest.fixture
def firms_enricher():
    return FirmsEnricher(max_concurrent=2, requests_per_minute=60)


@pytest.fixture
def bank_enricher():
    return BankEnricher(max_concurrent=2, requests_per_minute=60)


# ──────────────────────────────────────────────────────────────────────────────
# FirmsEnricher
# ──────────────────────────────────────────────────────────────────────────────

class TestFirmsEnricher:
    def test_columns(self, firms_enricher):
        assert "Year of Issuer Establishment" in firms_enricher.columns

    def test_system_prompt_contains_key_instruction(self, firms_enricher):
        assert "establishment_year" in firms_enricher.system_prompt

    def test_create_user_prompt(self, firms_enricher):
        row = {
            "row_index": 0,
            "IssueDate": "2000-01-01",
            "Issuer": "Acme Corp",
            "TickerSymbol": "ACM",
            "BusinessDescription": "Widget manufacturer",
            "Nation": "USA",
            "State": "CA",
            "PrimaryExchangeWhereIssuers": "NYSE",
        }
        prompt = firms_enricher.create_user_prompt(row)
        assert "Acme Corp" in prompt
        assert "2000-01-01" in prompt
        assert "NYSE" in prompt

    def test_parse_response(self, firms_enricher):
        parsed = {"establishment_year": 1985}
        row_data = {"Issuer": "Acme Corp", "IssueDate": "2000-01-01", "row_index": 0}
        result = firms_enricher.parse_response(parsed, row_data)
        assert result["Year of Issuer Establishment"] == 1985
        assert result["company_name"] == "Acme Corp"

    def test_completion_mask_filters_correctly(self, firms_enricher):
        df = pd.DataFrame({
            "Issuer": ["A", "B", "C"],
            "Year of Issuer Establishment": [None, 1990, None],
        })
        mask = firms_enricher.completion_mask(df)
        assert mask.tolist() == [True, False, True]

    def test_prepare_batch_item(self, firms_enricher):
        row = pd.Series({
            "IssueDate": "2001-06-15",
            "Issuer": "TechCo",
            "TickerSymbol": "TCO",
            "BusinessDescription": "Software",
            "Nation": "UK",
            "State": "",
            "PrimaryExchangeWhereIssuers": "LSE",
        })
        item = firms_enricher.prepare_batch_item(42, row)
        assert item["row_index"] == 42
        assert item["Issuer"] == "TechCo"


# ──────────────────────────────────────────────────────────────────────────────
# BankEnricher
# ──────────────────────────────────────────────────────────────────────────────

class TestBankEnricher:
    def test_columns(self, bank_enricher):
        assert "Ultimate Parent (as of Year)" in bank_enricher.columns
        assert "Bank Type" in bank_enricher.columns
        assert "Reputation (High/Medium/Low)" in bank_enricher.columns

    def test_system_prompt_non_empty(self, bank_enricher):
        assert len(bank_enricher.system_prompt) > 50

    def test_create_user_prompt(self, bank_enricher):
        row = {"row_index": 0, "bank_name": "Goldman Sachs", "year": 1990}
        prompt = bank_enricher.create_user_prompt(row)
        assert "Goldman Sachs" in prompt
        assert "1990" in prompt
        assert "ultimate_parent" in prompt

    def test_parse_response(self, bank_enricher):
        parsed = {
            "ultimate_parent": "Goldman Sachs Group",
            "parent_country": "USA",
            "parent_established": 1869,
            "is_parent": "Yes",
            "manager_country": "USA",
            "manager_established": 1869,
            "government_owned": "No",
            "bank_type": "Investment Bank",
            "top_20_global": "Yes",
            "european_presence": "Yes",
            "privatisation_track": "Yes",
            "reputation": "High",
        }
        row_data = {"bank_name": "Goldman Sachs", "year": 1990, "row_index": 0}
        result = bank_enricher.parse_response(parsed, row_data)
        assert result["Ultimate Parent (as of Year)"] == "Goldman Sachs Group"
        assert result["Bank Type"] == "Investment Bank"
        assert result["Reputation (High/Medium/Low)"] == "High"

    def test_completion_mask(self, bank_enricher):
        df = pd.DataFrame({
            "Lead Managers": ["JPMorgan", "HSBC", None],
            "Year": [1995, 2000, 2005],
            "Ultimate Parent (as of Year)": [None, "HSBC Holdings", None],
        })
        mask = bank_enricher.completion_mask(df)
        # Row 0: needs enrichment (NaN parent, valid manager, valid year)
        # Row 1: already done
        # Row 2: Lead Managers is None → should be excluded
        assert mask.iloc[0] is True or mask[0]
        assert not mask.iloc[1]

    def test_prepare_batch_item(self, bank_enricher):
        row = pd.Series({"Lead Managers": "Barclays", "Year": 1992.0})
        item = bank_enricher.prepare_batch_item(7, row)
        assert item["row_index"] == 7
        assert item["bank_name"] == "Barclays"
        assert item["year"] == 1992


# ──────────────────────────────────────────────────────────────────────────────
# BaseEnricher shared behaviour
# ──────────────────────────────────────────────────────────────────────────────

class TestBaseEnricherShared:
    def test_extract_wait_time_ms(self, firms_enricher):
        text = "Rate limit exceeded, try again in 500ms"
        wait = firms_enricher._extract_wait_time(text)
        assert wait == pytest.approx(1.0, abs=0.1)

    def test_extract_wait_time_requests(self, firms_enricher):
        wait = firms_enricher._extract_wait_time("requests per min exceeded")
        assert wait == 2.0

    def test_extract_wait_time_tokens(self, firms_enricher):
        wait = firms_enricher._extract_wait_time("tokens per min exceeded")
        assert wait == 5.0

    def test_extract_wait_time_fallback(self, firms_enricher):
        wait = firms_enricher._extract_wait_time("something else happened")
        assert wait == 3.0

    def test_check_daily_limits_token_breach(self, firms_enricher):
        firms_enricher.tokens_used_today = firms_enricher.max_tokens_per_day - 10
        assert not firms_enricher._check_daily_limits(estimated_tokens=50)
        assert firms_enricher.daily_limit_reached

    def test_check_daily_limits_request_breach(self, firms_enricher):
        firms_enricher.requests_made_today = firms_enricher.max_requests_per_day
        assert not firms_enricher._check_daily_limits()
        assert firms_enricher.daily_limit_reached

    def test_check_daily_limits_ok(self, firms_enricher):
        firms_enricher.tokens_used_today = 0
        firms_enricher.requests_made_today = 0
        firms_enricher.daily_limit_reached = False
        assert firms_enricher._check_daily_limits(estimated_tokens=100)
