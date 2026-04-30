"""
Entry point: Company IPO Firms Enrichment
==========================================
Enriches a CSV of IPO records with each company's original founding year.

Usage
-----
    # Fresh run
    python scripts/run_firms_enrichment.py

    # Resume a previous (interrupted or daily-limited) session
    python scripts/run_firms_enrichment.py --resume

Configuration
-------------
All tuneable parameters live in the CONFIG block below.
Set OPENAI_API_KEY in your .env file (see .env.example).
"""

import asyncio
import os
import sys

# Make sure the repo root is on the path when running from any directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.firms_enricher import FirmsEnricher

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG — edit these values as needed
# ──────────────────────────────────────────────────────────────────────────────
INPUT_FILE = "data/firms_for_enrichment.csv"
OUTPUT_FILE = "data/firms_for_enrichment_completed.csv"
BATCH_SIZE = 15         # rows per async batch
MAX_CONCURRENT = 8      # parallel API calls
REQUESTS_PER_MINUTE = 200
# ──────────────────────────────────────────────────────────────────────────────


async def main() -> None:
    print("🏢  Company IPO Data Enrichment Tool")
    print("=" * 60)

    resume = "--resume" in sys.argv or "-r" in sys.argv
    print("🔄 RESUME MODE" if resume else "🚀 FRESH START")

    if not os.path.exists(INPUT_FILE):
        print(f"❌  Input file not found: {INPUT_FILE}")
        sys.exit(1)

    if not os.getenv("OPENAI_API_KEY"):
        print("❌  OPENAI_API_KEY not set. Add it to your .env file.")
        sys.exit(1)

    enricher = FirmsEnricher(
        max_concurrent=MAX_CONCURRENT,
        requests_per_minute=REQUESTS_PER_MINUTE,
    )

    await enricher.process_csv(
        input_file=INPUT_FILE,
        output_file=OUTPUT_FILE,
        batch_size=BATCH_SIZE,
        resume=resume,
    )

    print("\n✅  Session complete!")
    print(f"   Output  : {OUTPUT_FILE}")
    print("   Failures: ipo_failed_rows.json")
    print("   Progress: ipo_progress.json")
    print("   Usage   : ipo_daily_usage.json")
    if resume:
        print("\n💡 Your original CSV now contains all enriched IPO data.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️  Interrupted. Re-run with --resume to continue.")
    except Exception as exc:
        import traceback
        print(f"\n❌  Unexpected error: {exc}")
        traceback.print_exc()
        sys.exit(1)
