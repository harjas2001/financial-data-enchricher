"""
Bank / Lead Manager Data Enricher
===================================
Enriches a CSV of bond/equity deal records with structured metadata about
each lead manager: ultimate parent, country, establishment year, bank type,
global ranking, government ownership, European presence, privatisation track
record, and reputational tier.

Expected input columns:
    Lead Managers, Year  ← key input fields
    Ultimate Parent (as of Year)  ← used to detect already-processed rows

All other output columns are created/populated by this script.

Usage:
    python scripts/run_bank_enrichment.py [--resume]
"""

from __future__ import annotations

import pandas as pd

from src.base_enricher import BaseEnricher


# Output column name constants — avoids silent typos across the codebase
COL_ULTIMATE_PARENT = "Ultimate Parent (as of Year)"
COL_PARENT_COUNTRY = "Ultimate Parent Country of Incorporation"
COL_PARENT_ESTABLISHED = "Year of Establishment of the Ultimate Parent"
COL_IS_PARENT = "Was Lead Manager also Parent?"
COL_MANAGER_COUNTRY = "Lead Manager Country of Incorporation"
COL_MANAGER_ESTABLISHED = "Year of Establishment of Lead Manager"
COL_GOVT_OWNED = "Government Owned (>50%)"
COL_BANK_TYPE = "Bank Type"
COL_TOP20 = "Top 20 Global Bank in Year"
COL_EU_PRESENCE = "European Presence in Year"
COL_PRIVATISATION = "Privatisation Track Record"
COL_REPUTATION = "Reputation (High/Medium/Low)"


class BankEnricher(BaseEnricher):
    """
    Enriches bank/lead-manager records with institutional metadata.

    Handles large deal datasets via async batching with OpenAI Tier-1 daily
    limits, supporting full resume/crash-recovery semantics.
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
            max_tokens_per_day=1_999_900,
            max_requests_per_day=9_950,
            max_tokens_per_call=500,
            temperature=0.1,
            daily_usage_file="bank_daily_usage.json",
            failed_file="bank_failed_rows.json",
            progress_file="bank_progress.json",
        )

    # ------------------------------------------------------------------ #
    # BaseEnricher interface                                               #
    # ------------------------------------------------------------------ #

    @property
    def system_prompt(self) -> str:
        return (
            "You are a financial data analyst expert. Always respond with valid JSON only.\n\n"
            "Guidelines:\n"
            "- If unsure about any field, make your best educated guess based on "
            "typical patterns for similar institutions.\n"
            "- For establishment years, provide only the number (e.g., 1969, not '1969 (merger)').\n"
            "- Be specific about the year context — the financial industry changed significantly "
            "across decades.\n"
            "- If the institution name suggests it is the same as its parent, answer 'Yes' for "
            "is_parent.\n"
            "- Consider the historical context of the year when assessing global ranking and presence."
        )

    @property
    def columns(self) -> list[str]:
        return [
            "Lead Managers",
            "Year",
            COL_ULTIMATE_PARENT,
            COL_PARENT_COUNTRY,
            COL_PARENT_ESTABLISHED,
            COL_IS_PARENT,
            COL_MANAGER_COUNTRY,
            COL_MANAGER_ESTABLISHED,
            COL_GOVT_OWNED,
            COL_BANK_TYPE,
            COL_TOP20,
            COL_EU_PRESENCE,
            COL_PRIVATISATION,
            COL_REPUTATION,
        ]

    def completion_mask(self, df: pd.DataFrame) -> pd.Series:
        return (
            df[COL_ULTIMATE_PARENT].isna()
            & df["Lead Managers"].notna()
            & df["Year"].notna()
        )

    def prepare_batch_item(self, idx: int, row: pd.Series) -> dict:
        return {
            "row_index": idx,
            "bank_name": row["Lead Managers"],
            "year": int(row["Year"]),
        }

    def create_user_prompt(self, row_data: dict) -> str:
        bank = row_data["bank_name"]
        year = row_data["year"]
        return (
            f'Based on the lead manager(s) "{bank}" and the year {year}, '
            "provide the following information in JSON format.\n\n"
            "Note: If multiple lead managers are listed, focus on the FIRST/PRIMARY "
            "lead manager for all fields.\n\n"
            "{\n"
            f'    "ultimate_parent": "Name of the ultimate parent company of the PRIMARY '
            f'lead manager as of {year}",\n'
            f'    "parent_country": "Country where the ultimate parent is incorporated",\n'
            f'    "parent_established": "Year the ultimate parent was established (number only)",\n'
            f'    "is_parent": "Yes/No — Was the PRIMARY lead manager also the parent company?",\n'
            f'    "manager_country": "Country where the PRIMARY lead manager is incorporated",\n'
            f'    "manager_established": "Year the PRIMARY lead manager was established (number only)",\n'
            f'    "government_owned": "Yes/No — Was the PRIMARY lead manager government owned (>50%)?",\n'
            f'    "bank_type": "Investment Bank/Commercial Bank/Universal Bank/Central Bank",\n'
            f'    "top_20_global": "Yes/No — Was the PRIMARY lead manager a top-20 global bank in {year}?",\n'
            f'    "european_presence": "Yes/No — Did the PRIMARY lead manager have European operations in {year}?",\n'
            f'    "privatisation_track": "Yes/No — History of privatisation involvement?",\n'
            f'    "reputation": "High/Medium/Low — Market reputation/tier of the PRIMARY lead manager"\n'
            "}\n\n"
            f'Bank: "{bank}"\n'
            f"Year: {year}\n\n"
            "Respond only with the JSON object."
        )

    def parse_response(self, parsed_json: dict, row_data: dict) -> dict:
        return {
            "Lead Managers": row_data["bank_name"],
            "Year": row_data["year"],
            COL_ULTIMATE_PARENT: parsed_json.get("ultimate_parent"),
            COL_PARENT_COUNTRY: parsed_json.get("parent_country"),
            COL_PARENT_ESTABLISHED: parsed_json.get("parent_established"),
            COL_IS_PARENT: parsed_json.get("is_parent"),
            COL_MANAGER_COUNTRY: parsed_json.get("manager_country"),
            COL_MANAGER_ESTABLISHED: parsed_json.get("manager_established"),
            COL_GOVT_OWNED: parsed_json.get("government_owned"),
            COL_BANK_TYPE: parsed_json.get("bank_type"),
            COL_TOP20: parsed_json.get("top_20_global"),
            COL_EU_PRESENCE: parsed_json.get("european_presence"),
            COL_PRIVATISATION: parsed_json.get("privatisation_track"),
            COL_REPUTATION: parsed_json.get("reputation"),
        }
