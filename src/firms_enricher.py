"""
Company IPO Data Enricher
=========================
Enriches a CSV of IPO records with each company's original founding year,
sourced from OpenAI's GPT-4o-mini with strict JSON output.

Expected input columns:
    IssueDate, Issuer, TickerSymbol (opt), BusinessDescription (opt),
    Nation (opt), State (opt), PrimaryExchangeWhereIssuers (opt),
    Year of Issuer Establishment  ← populated by this script

Usage:
    python scripts/run_firms_enrichment.py [--resume]
"""

from __future__ import annotations

import pandas as pd

from src.base_enricher import BaseEnricher


class FirmsEnricher(BaseEnricher):
    """
    Enriches IPO records with the founding year of each issuing company.

    Handles 65 k+ rows via async batching with OpenAI Tier-1 daily limits.
    """

    MODEL = "gpt-4o-mini"

    # ------------------------------------------------------------------ #
    # Construction                                                          #
    # ------------------------------------------------------------------ #

    def __init__(
        self,
        max_concurrent: int = 8,
        requests_per_minute: int = 200,
    ) -> None:
        super().__init__(
            model=self.MODEL,
            max_concurrent=max_concurrent,
            requests_per_minute=requests_per_minute,
            max_tokens_per_day=1_999_000,
            max_requests_per_day=9_950,
            max_tokens_per_call=50,   # Response is just {"establishment_year": YYYY}
            temperature=0.1,
            daily_usage_file="ipo_daily_usage.json",
            failed_file="ipo_failed_rows.json",
            progress_file="ipo_progress.json",
        )

    # ------------------------------------------------------------------ #
    # BaseEnricher interface                                               #
    # ------------------------------------------------------------------ #

    @property
    def system_prompt(self) -> str:
        return (
            "You are a financial data analyst specialising in company history.\n"
            "Your sole job is to identify the year a company was originally founded.\n\n"
            "Strict output rules:\n"
            "- Always respond with valid JSON only. No explanations, no extra text.\n"
            "- Required key:\n"
            '    "establishment_year" → the year (YYYY) when the company was first founded.\n\n'
            "Guidance for determining founding year:\n"
            "1. Prioritise official sources: regulator filings, annual reports, stock exchange "
            "disclosures, company 'About Us', Wikipedia, Bloomberg, Crunchbase.\n"
            "2. Distinguish:\n"
            "   • Founding year vs IPO year (IPO is later — not acceptable).\n"
            "   • Founding year vs year of incorporation/re-incorporation or restructuring "
            "(use the original founding year).\n"
            "   • Parent vs subsidiary (return the year of the IPO-issuing entity).\n"
            "3. If multiple dates are mentioned, choose the earliest credible founding year.\n"
            "4. Return a clean 4-digit year only (e.g., 1887). No text, ranges, or approximations.\n\n"
            'Output format:\n{ "establishment_year": 1887 }\n\nRespond only with this JSON object.'
        )

    @property
    def columns(self) -> list[str]:
        return ["company_name", "ipo_date", "Year of Issuer Establishment"]

    def completion_mask(self, df: pd.DataFrame) -> pd.Series:
        return df["Year of Issuer Establishment"].isna() & df["Issuer"].notna()

    def prepare_batch_item(self, idx: int, row: pd.Series) -> dict:
        return {
            "row_index": idx,
            "IssueDate": row.get("IssueDate", ""),
            "Issuer": row["Issuer"],
            "TickerSymbol": row.get("TickerSymbol", ""),
            "BusinessDescription": row.get("BusinessDescription", ""),
            "Nation": row.get("Nation", ""),
            "State": row.get("State", ""),
            "PrimaryExchangeWhereIssuers": row.get("PrimaryExchangeWhereIssuers", ""),
        }

    def create_user_prompt(self, row_data: dict) -> str:
        return (
            "Based on the IPO information below, provide only the founding year "
            "of the PRIMARY company in JSON format.\n\n"
            f"IPO Date: {row_data['IssueDate']}\n"
            f"Company Name: {row_data['Issuer']}\n"
            f"Ticker Symbol: {row_data['TickerSymbol']}\n"
            f"Business Description: {row_data['BusinessDescription']}\n"
            f"Country: {row_data['Nation']}\n"
            f"State/Region: {row_data['State']}\n"
            f"Primary Exchange: {row_data['PrimaryExchangeWhereIssuers']}\n\n"
            'Required output:\n'
            '{\n'
            '    "establishment_year": "Year the PRIMARY company was originally '
            'founded/established (YYYY, number only)"\n'
            '}\n\n'
            f"Company: {row_data['Issuer']}\n"
            f"IPO Date: {row_data['IssueDate']}\n\n"
            "Respond only with the JSON object."
        )

    def parse_response(self, parsed_json: dict, row_data: dict) -> dict:
        return {
            "company_name": row_data["Issuer"],
            "ipo_date": row_data["IssueDate"],
            "Year of Issuer Establishment": parsed_json.get("establishment_year"),
        }
