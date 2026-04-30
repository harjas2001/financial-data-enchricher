# financial-data-enrichment

Async, resumable OpenAI enrichment pipelines for large financial datasets.
Built for OpenAI Tier-1 API limits (2 M tokens / 10 K requests per day).

## What's included

| Script | Enricher class | Input | What it adds |
|---|---|---|---|
| `scripts/run_firms_enrichment.py` | `FirmsEnricher` | IPO records CSV | `Year of Issuer Establishment` |
| `scripts/run_bank_enrichment.py` | `BankEnricher` | Deal/bank records CSV | 12 institutional metadata columns |

Both scripts share a common `BaseEnricher` that handles rate limiting, daily quotas, crash-safe incremental writes, and resume logic.

---

## Quick start

### 1. Clone & install

```bash
git clone https://github.com/your-org/financial-data-enrichment.git
cd financial-data-enrichment
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Set your API key

```bash
cp .env.example .env
# Edit .env and paste your OpenAI API key
```

### 3. Place your data

Put your CSV in `data/`:

```
data/firms_for_enrichment.csv      ← for the IPO enricher
data/banks_metadata.csv            ← for the bank enricher
```

Sample files showing the expected schema are in `data/samples/`.

### 4. Run

```bash
# IPO company founding years
python scripts/run_firms_enrichment.py

# Bank / lead manager metadata
python scripts/run_bank_enrichment.py
```

### 5. Resume after interruption or daily limit

Both scripts are fully resumable. If a run is interrupted (Ctrl-C, crash, or daily API limit), re-run with:

```bash
python scripts/run_firms_enrichment.py --resume
python scripts/run_bank_enrichment.py   --resume
```

The script will merge any rows processed so far back into the original CSV, then continue from where it left off.

---

## Input CSV schemas

### `firms_for_enrichment.csv`

| Column | Required | Notes |
|---|---|---|
| `Issuer` | ✅ | Company name |
| `IssueDate` | ✅ | IPO date |
| `TickerSymbol` | optional | Helps disambiguation |
| `BusinessDescription` | optional | Improves accuracy |
| `Nation` | optional | Country of listing |
| `State` | optional | US state / region |
| `PrimaryExchangeWhereIssuers` | optional | e.g. NYSE, LSE |
| `Year of Issuer Establishment` | ✅ (blank) | Populated by the script |

### `banks_metadata.csv`

| Column | Required | Notes |
|---|---|---|
| `Lead Managers` | ✅ | Bank / underwriter name(s) |
| `Year` | ✅ | Deal year (used as temporal context) |
| `Ultimate Parent (as of Year)` | ✅ (blank) | Primary completion signal |
| All other output columns | blank | Populated by the script |

---

## Output columns — Bank enricher

| Column | Description |
|---|---|
| `Ultimate Parent (as of Year)` | Ultimate parent company at time of deal |
| `Ultimate Parent Country of Incorporation` | Country |
| `Year of Establishment of the Ultimate Parent` | Founding year |
| `Was Lead Manager also Parent?` | Yes / No |
| `Lead Manager Country of Incorporation` | Country |
| `Year of Establishment of Lead Manager` | Founding year |
| `Government Owned (>50%)` | Yes / No |
| `Bank Type` | Investment / Commercial / Universal / Central |
| `Top 20 Global Bank in Year` | Yes / No |
| `European Presence in Year` | Yes / No |
| `Privatisation Track Record` | Yes / No |
| `Reputation (High/Medium/Low)` | Tier |

---

## How it works

```
scripts/run_*.py
    └── src/FirmsEnricher  or  src/BankEnricher   (domain logic)
            └── src/BaseEnricher                   (shared infrastructure)
                    ├── Async aiohttp session
                    ├── Per-minute rate limiter
                    ├── Daily token + request quota tracker
                    ├── Exponential backoff (5 retries)
                    ├── Incremental CSV writes  ← crash-safe
                    ├── Progress JSON           ← human-readable status
                    └── Resume / merge logic    ← pick up where you left off
```

### Crash safety

Results are written to `*_enriched_data.csv` after **each successful API call**. If the script crashes:

1. Re-run with `--resume`
2. The script merges the partial results back into the original CSV
3. Processing continues only for rows still missing data

### Daily limit handling

When the Tier-1 daily limit is approaching, the script stops cleanly and prints the resume command. Run with `--resume` the next day.

---

## Configuration

Edit the `CONFIG` block at the top of each `scripts/run_*.py` file:

```python
INPUT_FILE           = "data/firms_for_enrichment.csv"
OUTPUT_FILE          = "data/firms_for_enrichment_completed.csv"
BATCH_SIZE           = 15     # rows per coroutine batch
MAX_CONCURRENT       = 8      # parallel API calls
REQUESTS_PER_MINUTE  = 200    # stay within your tier's RPM limit
```

To switch models or tune daily quotas, edit the enricher's `__init__` in `src/firms_enricher.py` or `src/bank_enricher.py`.

---

## Extending to a new dataset

1. Create `src/my_enricher.py` subclassing `BaseEnricher`
2. Implement the six abstract members:
   - `system_prompt` — the model instruction
   - `create_user_prompt(row_data)` — per-row prompt
   - `parse_response(parsed_json, row_data)` — JSON → flat dict
   - `columns` — list of output column names
   - `completion_mask(df)` — which rows still need enrichment
   - `prepare_batch_item(idx, row)` — DataFrame row → dict
3. Copy a `scripts/run_*.py` and swap in your class

---

## Running tests

```bash
pytest tests/ -v
```

---

## Project structure

```
financial-data-enrichment/
├── src/
│   ├── __init__.py
│   ├── base_enricher.py        # Shared async infrastructure
│   ├── firms_enricher.py       # IPO founding-year enricher
│   └── bank_enricher.py        # Bank metadata enricher
├── scripts/
│   ├── run_firms_enrichment.py
│   └── run_bank_enrichment.py
├── tests/
│   └── test_enrichers.py
├── data/
│   └── samples/
│       ├── firms_sample.csv
│       └── banks_sample.csv
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

---

## License

MIT
