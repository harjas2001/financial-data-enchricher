"""
Base class for all async OpenAI data enrichers.

Handles the shared infrastructure:
- Async/concurrent API calls with configurable concurrency
- Per-minute and per-day rate limiting
- Exponential backoff on retries
- Incremental CSV writes (crash-safe)
- Resume/merge from prior session
- Progress and daily usage persistence
"""

from __future__ import annotations

import asyncio
import csv
import json
import os
import re
import threading
import time
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any

import aiohttp
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


class BaseEnricher(ABC):
    """
    Abstract base class for rate-limited, resumable OpenAI enrichment jobs.

    Subclasses must implement:
        - system_prompt (property)
        - create_user_prompt(row_data) -> str
        - parse_response(parsed_json, row_data) -> dict
        - columns (property) — ordered list of output CSV column names
        - completion_mask(df) -> pd.Series — True for rows still needing enrichment
    """

    # ------------------------------------------------------------------ #
    # Construction                                                          #
    # ------------------------------------------------------------------ #

    def __init__(
        self,
        model: str,
        max_concurrent: int = 8,
        requests_per_minute: int = 200,
        max_tokens_per_day: int = 1_999_000,
        max_requests_per_day: int = 9_950,
        max_tokens_per_call: int = 500,
        temperature: float = 0.1,
        daily_usage_file: str = "daily_usage.json",
        failed_file: str = "failed_rows.json",
        progress_file: str = "progress.json",
    ) -> None:
        self.api_key = os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY not found. Add it to your .env file.")

        self.model = model
        self.max_concurrent = max_concurrent
        self.requests_per_minute = requests_per_minute
        self.request_delay = 60.0 / requests_per_minute

        # Daily quota
        self.max_tokens_per_day = max_tokens_per_day
        self.max_requests_per_day = max_requests_per_day
        self.max_tokens_per_call = max_tokens_per_call
        self.temperature = temperature

        # Runtime state
        self.tokens_used_today = 0
        self.requests_made_today = 0
        self.daily_limit_reached = False
        self.processed_count = 0
        self.failed_rows: list[dict] = []
        self.total_cost = 0.0
        self.start_time: float | None = None
        self.output_file: str | None = None

        # Thread safety
        self._lock = threading.Lock()

        # Rate-limit tracking (shared across coroutines via lock)
        self._last_request_time = 0.0
        self._request_count_this_minute = 0
        self._minute_start = time.time()

        # File paths
        self.daily_usage_file = daily_usage_file
        self.failed_file = failed_file
        self.progress_file = progress_file

        self._load_daily_usage()
        self._print_init_summary()

    # ------------------------------------------------------------------ #
    # Abstract interface                                                    #
    # ------------------------------------------------------------------ #

    @property
    @abstractmethod
    def system_prompt(self) -> str:
        """The system prompt sent to the model on every request."""

    @abstractmethod
    def create_user_prompt(self, row_data: dict) -> str:
        """Build the per-row user prompt from ``row_data``."""

    @abstractmethod
    def parse_response(self, parsed_json: dict, row_data: dict) -> dict:
        """
        Convert the model's parsed JSON into a flat dict suitable for
        one CSV row.  Must include all keys listed in ``self.columns``.
        """

    @property
    @abstractmethod
    def columns(self) -> list[str]:
        """Ordered list of output CSV column names (excluding ``row_index``)."""

    @abstractmethod
    def completion_mask(self, df: pd.DataFrame) -> pd.Series:
        """
        Return a boolean Series that is True for rows still needing
        enrichment (i.e., the primary enrichment column is NaN and the
        key input columns are present).
        """

    @abstractmethod
    def prepare_batch_item(self, idx: int, row: pd.Series) -> dict:
        """
        Extract the fields needed for ``create_user_prompt`` from a
        DataFrame row and return them as a dict (including ``row_index``).
        """

    # ------------------------------------------------------------------ #
    # Daily usage persistence                                              #
    # ------------------------------------------------------------------ #

    def _load_daily_usage(self) -> None:
        try:
            if Path(self.daily_usage_file).exists():
                with open(self.daily_usage_file) as f:
                    data = json.load(f)
                if data.get("date") == time.strftime("%Y-%m-%d"):
                    self.tokens_used_today = data.get("tokens_used", 0)
                    self.requests_made_today = data.get("requests_made", 0)
                    print(
                        f"📁 Loaded today's usage: {self.tokens_used_today:,} tokens, "
                        f"{self.requests_made_today:,} requests"
                    )
                    return
                print("🆕 New day detected — resetting daily counters.")
            else:
                print("🆕 No previous usage file found — starting fresh.")
        except Exception as exc:
            print(f"⚠️  Could not load daily usage: {exc}")

        self.tokens_used_today = 0
        self.requests_made_today = 0

    def _save_daily_usage(self) -> None:
        try:
            with open(self.daily_usage_file, "w") as f:
                json.dump(
                    {
                        "date": time.strftime("%Y-%m-%d"),
                        "tokens_used": self.tokens_used_today,
                        "requests_made": self.requests_made_today,
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    },
                    f,
                    indent=2,
                )
        except Exception as exc:
            print(f"⚠️  Could not save daily usage: {exc}")

    def _check_daily_limits(self, estimated_tokens: int = 500) -> bool:
        if self.tokens_used_today + estimated_tokens >= self.max_tokens_per_day:
            print(
                f"🛑 Daily token limit reached: "
                f"{self.tokens_used_today:,}/{self.max_tokens_per_day:,}"
            )
            self.daily_limit_reached = True
            return False
        if self.requests_made_today >= self.max_requests_per_day:
            print(
                f"🛑 Daily request limit reached: "
                f"{self.requests_made_today:,}/{self.max_requests_per_day:,}"
            )
            self.daily_limit_reached = True
            return False
        return True

    # ------------------------------------------------------------------ #
    # Rate limiting                                                         #
    # ------------------------------------------------------------------ #

    async def _enforce_rate_limit(self) -> None:
        now = time.time()

        # Roll over per-minute counter
        if now - self._minute_start >= 60:
            self._minute_start = now
            self._request_count_this_minute = 0

        # Back off when approaching the per-minute cap
        if self._request_count_this_minute >= self.requests_per_minute - 20:
            wait = 60.0 - (now - self._minute_start)
            if wait > 0:
                print(f"⏱️  Rate-limit buffer reached — waiting {wait:.1f}s…")
                await asyncio.sleep(wait)
                self._minute_start = time.time()
                self._request_count_this_minute = 0

        # Ensure minimum delay between successive requests
        elapsed = now - self._last_request_time
        if elapsed < self.request_delay:
            await asyncio.sleep(self.request_delay - elapsed)

        self._last_request_time = time.time()
        self._request_count_this_minute += 1

    @staticmethod
    def _extract_wait_time(error_text: str) -> float:
        """Parse retry-after milliseconds from OpenAI 429 body."""
        match = re.search(r"try again in (\d+)ms", error_text)
        if match:
            return float(match.group(1)) / 1000.0 + 0.5
        if "requests per min" in error_text:
            return 2.0
        if "tokens per min" in error_text:
            return 5.0
        return 3.0

    # ------------------------------------------------------------------ #
    # Core API call                                                         #
    # ------------------------------------------------------------------ #

    async def _make_api_call(
        self,
        session: aiohttp.ClientSession,
        row_data: dict,
        max_retries: int = 5,
    ) -> dict:
        row_index: int = row_data["row_index"]

        if not self._check_daily_limits():
            return self._failure(row_data, "Daily limit reached")

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": self.create_user_prompt(row_data)},
            ],
            "max_tokens": self.max_tokens_per_call,
            "temperature": self.temperature,
        }

        for attempt in range(max_retries):
            try:
                await self._enforce_rate_limit()

                async with session.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers=headers,
                    json=payload,
                ) as response:
                    if response.status == 200:
                        return await self._handle_success(response, row_data)

                    if response.status == 429:
                        body = await response.text()
                        wait = self._extract_wait_time(body)
                        print(
                            f"⏳ Rate limit (row {row_index}) — "
                            f"waiting {wait:.1f}s… (attempt {attempt + 1}/{max_retries})"
                        )
                        await asyncio.sleep(wait)
                        continue

                    body = await response.text()
                    if attempt < max_retries - 1:
                        print(
                            f"⚠️  HTTP {response.status} (row {row_index}) — "
                            f"retrying… ({attempt + 1}/{max_retries})"
                        )
                        await asyncio.sleep(2**attempt)
                        continue
                    return self._failure(row_data, f"HTTP {response.status}: {body}")

            except json.JSONDecodeError as exc:
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                return self._failure(row_data, f"JSON parse error: {exc}")

            except Exception as exc:
                if attempt < max_retries - 1:
                    await asyncio.sleep(2**attempt)
                    continue
                return self._failure(row_data, f"API error: {exc}")

        return self._failure(row_data, f"Failed after {max_retries} attempts")

    async def _handle_success(
        self, response: aiohttp.ClientResponse, row_data: dict
    ) -> dict:
        data = await response.json()
        raw = data["choices"][0]["message"]["content"].strip()

        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = re.sub(r"^```(?:json)?", "", raw).rstrip("`").strip()

        parsed = json.loads(raw)

        usage = data.get("usage", {})
        input_tokens: int = usage.get("prompt_tokens", 200)
        output_tokens: int = usage.get("completion_tokens", 100)
        total_tokens = input_tokens + output_tokens
        cost = (input_tokens / 1_000_000) * 0.50 + (output_tokens / 1_000_000) * 1.50

        with self._lock:
            self.total_cost += cost
            self.tokens_used_today += total_tokens
            self.requests_made_today += 1

            if self.requests_made_today % 50 == 0:
                self._save_daily_usage()
            if self.requests_made_today % 100 == 0:
                rem_t = self.max_tokens_per_day - self.tokens_used_today
                rem_r = self.max_requests_per_day - self.requests_made_today
                print(
                    f"📈 Daily: {self.tokens_used_today:,}/{self.max_tokens_per_day:,} tokens "
                    f"({rem_t:,} left) | "
                    f"{self.requests_made_today:,}/{self.max_requests_per_day:,} requests "
                    f"({rem_r:,} left)"
                )

        return {
            "success": True,
            "row_index": row_data["row_index"],
            "row_data": row_data,
            "parsed": parsed,
            "cost": cost,
            "total_tokens": total_tokens,
        }

    @staticmethod
    def _failure(row_data: dict, error: str) -> dict:
        return {
            "success": False,
            "row_index": row_data["row_index"],
            "row_data": row_data,
            "error": error,
        }

    # ------------------------------------------------------------------ #
    # Batch / result handling                                              #
    # ------------------------------------------------------------------ #

    async def _process_batch(
        self,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        batch: list[dict],
    ) -> None:
        async with semaphore:
            tasks = []
            for item in batch:
                if self.daily_limit_reached:
                    break
                tasks.append(self._make_api_call(session, item))

            if not tasks:
                return

            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    print(f"❌ Batch error: {result}")
                    continue
                await self._handle_result(result)

            await asyncio.sleep(self.request_delay)

    async def _handle_result(self, result: dict) -> None:
        if result["success"]:
            await self._write_to_csv(result)
            with self._lock:
                self.processed_count += 1
            if self.processed_count % 10 == 0:
                await self._update_progress()
                print(
                    f"✅ Processed {self.processed_count} rows | "
                    f"Cost: ${self.total_cost:.4f}"
                )
        else:
            error = result.get("error", "")
            if error == "Daily limit reached":
                self.daily_limit_reached = True
                return

            entry = {
                "row_index": result["row_index"],
                "row_data": result["row_data"],
                "error": error,
                "timestamp": datetime.now().isoformat(),
            }
            with self._lock:
                self.failed_rows.append(entry)
            print(f"❌ Failed row {result['row_index']}: {error}")

    # ------------------------------------------------------------------ #
    # CSV I/O                                                              #
    # ------------------------------------------------------------------ #

    @property
    def _enriched_tmp_file(self) -> str:
        assert self.output_file
        return self.output_file.replace(".csv", "_enriched_data.csv")

    async def _write_to_csv(self, result: dict) -> None:
        try:
            row = self.parse_response(result["parsed"], result["row_data"])
            row["row_index"] = result["row_index"]
            fieldnames = ["row_index"] + self.columns

            tmp = self._enriched_tmp_file
            file_exists = Path(tmp).exists()

            with self._lock:
                with open(tmp, "a", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    if not file_exists:
                        writer.writeheader()
                    writer.writerow(row)
        except Exception as exc:
            print(f"❌ CSV write error: {exc}")

    def _merge_final_csv(self, original_df: pd.DataFrame) -> None:
        print("🔄 Merging enriched data into output CSV…")
        tmp = self._enriched_tmp_file

        if not Path(tmp).exists():
            print("❌ No enriched data file found — skipping merge.")
            return

        enriched_df = pd.read_csv(tmp)
        for _, row in enriched_df.iterrows():
            idx = int(row["row_index"])
            if idx < len(original_df):
                for col in self.columns:
                    if col in row:
                        original_df.at[idx, col] = row[col]

        original_df.to_csv(self.output_file, index=False)
        print(f"💾 Final output saved: {self.output_file}")
        Path(tmp).unlink()

    def _merge_enriched_to_original(self, input_file: str) -> None:
        """Re-integrate a prior session's incremental file before resuming."""
        tmp = self._enriched_tmp_file
        if not Path(tmp).exists():
            print("📝 No prior enriched data found — starting fresh.")
            return

        print("🔄 Resuming: merging prior session's enriched data…")
        df_orig = pd.read_csv(input_file)
        df_orig.columns = df_orig.columns.str.strip()
        df_enr = pd.read_csv(tmp)

        merged = 0
        for _, row in df_enr.iterrows():
            idx = int(row["row_index"])
            if idx < len(df_orig):
                for col in self.columns:
                    if col in row and pd.notna(row[col]):
                        df_orig.at[idx, col] = row[col]
                merged += 1

        backup = input_file.replace(".csv", "_backup_before_resume.csv")
        df_orig.to_csv(backup, index=False)
        df_orig.to_csv(input_file, index=False)
        print(f"✅ Re-merged {merged} previously processed rows.")
        print(f"💾 Backup saved: {backup}")
        Path(tmp).unlink()

    def _save_failed_rows(self) -> None:
        if self.failed_rows:
            with open(self.failed_file, "w") as f:
                json.dump(self.failed_rows, f, indent=2)
            print(f"💾 {len(self.failed_rows)} failed rows → {self.failed_file}")

    async def _update_progress(self) -> None:
        data = {
            "processed_count": self.processed_count,
            "failed_count": len(self.failed_rows),
            "total_cost": self.total_cost,
            "tokens_used_today": self.tokens_used_today,
            "requests_made_today": self.requests_made_today,
            "elapsed_seconds": time.time() - (self.start_time or time.time()),
            "timestamp": datetime.now().isoformat(),
        }
        with open(self.progress_file, "w") as f:
            json.dump(data, f, indent=2)

    # ------------------------------------------------------------------ #
    # Main entry point                                                     #
    # ------------------------------------------------------------------ #

    async def process_csv(
        self,
        input_file: str,
        output_file: str,
        batch_size: int = 15,
        resume: bool = False,
    ) -> None:
        self.output_file = output_file
        self.start_time = time.time()

        print(f"📊 Loading {input_file}…")

        if resume:
            self._merge_enriched_to_original(input_file)

        df = pd.read_csv(input_file)
        df.columns = df.columns.str.strip()
        print(f"📈 Total rows: {len(df):,}")

        mask = self.completion_mask(df)
        rows_todo = df[mask].copy()
        completed = len(df) - len(rows_todo)
        print(f"🔄 To process: {len(rows_todo):,}  |  Already done: {completed:,}")

        if resume and completed > 0:
            print("🚀 Resuming from where we left off!")

        if rows_todo.empty:
            print("🎉 All rows already processed — nothing to do.")
            return

        batch_data = [
            self.prepare_batch_item(idx, row)
            for idx, row in rows_todo.iterrows()
        ]

        print(
            f"🚀 Starting async processing | "
            f"batch={batch_size} | concurrency={self.max_concurrent}"
        )
        semaphore = asyncio.Semaphore(self.max_concurrent)

        async with aiohttp.ClientSession() as session:
            tasks = []
            for i in range(0, len(batch_data), batch_size):
                if self.daily_limit_reached:
                    print("🛑 Daily limit reached — stopping batch creation.")
                    break
                tasks.append(
                    self._process_batch(session, semaphore, batch_data[i : i + batch_size])
                )
            await asyncio.gather(*tasks)

        if self.daily_limit_reached:
            print("\n🛑 DAILY LIMIT REACHED")
            print(
                f"📊 Today: {self.tokens_used_today:,} tokens | "
                f"{self.requests_made_today:,} requests"
            )
            print("💤 Run tomorrow with --resume to continue.")
            self._save_daily_usage()
        else:
            elapsed = (time.time() - self.start_time) / 60
            print(
                f"\n🎯 Done! Processed {self.processed_count:,} | "
                f"Failed {len(self.failed_rows):,} | "
                f"Cost ${self.total_cost:.4f} | "
                f"{elapsed:.1f} min"
            )
            self._merge_final_csv(df)

        self._save_failed_rows()
        await self._update_progress()

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #

    def _print_init_summary(self) -> None:
        print(f"\n{'=' * 60}")
        print(f"  {self.__class__.__name__}")
        print(f"{'=' * 60}")
        print(f"  Model           : {self.model}")
        print(f"  Concurrency     : {self.max_concurrent}")
        print(f"  Rate limit      : {self.requests_per_minute} req/min")
        print(
            f"  Daily limits    : {self.max_tokens_per_day:,} tokens | "
            f"{self.max_requests_per_day:,} requests"
        )
        print(
            f"  Today's usage   : {self.tokens_used_today:,} tokens | "
            f"{self.requests_made_today:,} requests"
        )
        print(
            f"  Remaining today : "
            f"{self.max_tokens_per_day - self.tokens_used_today:,} tokens | "
            f"{self.max_requests_per_day - self.requests_made_today:,} requests"
        )
        print(f"{'=' * 60}\n")
