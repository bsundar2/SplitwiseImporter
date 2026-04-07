# SplitwiseImporter

A Python project to import Splitwise expenses, process credit card statements, categorize expenses, and sync to Google Sheets for budget tracking.

## 🎯 Project Overview

A robust, automated pipeline for managing personal finances by importing credit card statements, categorizing expenses, and syncing them with Splitwise and Google Sheets.

**Architecture:**
- **Splitwise** = Source of Truth (Manual edits & split management)
- **Local SQLite Database** = Synced Mirror (Fast queries & historical archiving)
- **Google Sheets** = Viewing Layer (Formatted exports & budget analysis)

## Setup
1. Create a virtual environment: `python -m venv .venv`
2. Activate the environment: `source .venv/bin/activate`
3. Install dependencies: `pip install -r requirements.txt`
4. Configure environment variables:
   - Copy the template: `cp config/.env.example config/.env`
   - Open `config/.env` and add your API keys and configuration (see Environment Variables section below)
5. Set up Google Sheets access:
   - Place your service account JSON file at `config/gsheets_authentication.json`
   - Share your spreadsheet with the service account email address
6. Set PYTHONPATH: `export PYTHONPATH=/home/balaji94/PycharmProjects/SplitwiseImporter`

## Quick Start

### Recommended: Automated Monthly Pipeline

The easiest way to process a new statement is the automated pipeline:

```bash
# Activate venv and set PYTHONPATH
source .venv/bin/activate
export PYTHONPATH=/home/balaji94/PycharmProjects/SplitwiseImporter

# Full pipeline: Import statement → Sync DB → Export to sheets
python src/export/monthly_export_pipeline.py \
  --statement data/raw/jan2026.csv \
  --year 2026 \
  --start-date 2026-01-01 \
  --end-date 2026-01-31

# Sync and export only (no new statement)
python src/export/monthly_export_pipeline.py --year 2026 --sync-only

# Append-only mode (only export unwritten transactions)
python src/export/monthly_export_pipeline.py --year 2026 --sync-only --append-only

# Dry run to preview all changes
python src/export/monthly_export_pipeline.py \
  --statement data/raw/jan2026.csv \
  --year 2026 \
  --dry-run
```

This single command:
1. Imports your CSV statement to Splitwise
2. Syncs database with Splitwise (updates payment info)
3. Exports to Google Sheets (filters payments, shows only 12 columns)

### Generate Budget Summaries

After exporting transactions, generate budget analysis and spending patterns:

```bash
# Generate all summary sheets for 2026
python src/export/generate_summaries.py --year 2026

# Preview summaries without writing
python src/export/generate_summaries.py --year 2026 --dry-run

# With custom budget file
python src/export/generate_summaries.py --year 2026 --budget config/budget_2026.json
```

**Summary sheets generated:**
- **Monthly Summary** - Total spending by month with MoM changes and cumulative totals
- **Category Breakdown** - Spending by category with percentages and transaction counts
- **Budget vs Actual** - Compare actual spending against budget targets with variance %
- **Monthly Trends** - 3-month rolling averages and YTD trends
- **Category by Month** - Pivot table showing category spending by month

### Manual DB Sync
For precise control or historical adjustments:

### Process a Credit Card Statement (Manual Steps)
```bash
# Parse and categorize transactions
python src/import_statement/pipeline.py --statement data/raw/your_statement.csv

If you need to troubleshoot or run individual steps:

```bash
# 1. Import statement to Splitwise
python src/import_statement/pipeline.py \
  --statement data/raw/your_statement.csv \
  --start-date 2026-01-01 \
  --end-date 2026-01-31

# Dry run to preview without saving
python src/import_statement/pipeline.py \
  --statement data/raw/your_statement.csv \
  --dry-run

# 2. Sync database with Splitwise
python src/db_sync/sync_from_splitwise.py --year 2026 --live

# Dry run first to preview
python src/db_sync/sync_from_splitwise.py --year 2026 --dry-run

# 3. Export to Google Sheets
python src/export/splitwise_export.py \
  --source database \
  --year 2026 \
  --worksheet "Expenses 2026" \
  --overwrite

# Dry run export
python src/export/splitwise_export.py \
  --source database \
  --year 2026 \
  --dry-run
```

### Review & Improve Merchant Extraction

The pipeline automatically generates a review file for extracted merchant names. Review and correct them to improve future processing:

```bash
# Unified review workflow (recommended - runs all steps)
python src/merchant_review/run_review_workflow.py --processed-csv data/processed/your_statement.csv.processed.csv --batch 20

# Or run steps individually:
# 1. Generate review file
python src/merchant_review/generate_review_file.py --processed-csv data/processed/your_statement.csv.processed.csv --output data/processed/merchant_names_for_review.csv

# 2. Start interactive review (batch of 20)
python src/merchant_review/review_merchants.py --batch 20

# 3. Check progress
python src/merchant_review/review_merchants.py --stats

# 4. Apply your corrections to update the configuration
python src/merchant_review/apply_review_feedback.py

# Re-run pipeline to see improvements
python src/import_statement/pipeline.py --statement data/raw/your_statement.csv
```

### Export Options

Export from Splitwise API or database to Google Sheets:

```bash
# Export from database (recommended - faster, filtered)
python src/export/splitwise_export.py \
  --source database \
  --year 2026 \
  --worksheet "Expenses 2026" \
  --overwrite

# Export from Splitwise API (live data)
python src/export/splitwise_export.py \
  --source splitwise \
  --start-date 2026-01-01 \
  --end-date 2026-12-31 \
  --worksheet "Expenses 2026" \
  --overwrite

# Dry run to preview
python src/export/splitwise_export.py \
  --source database \
  --year 2026 \
  --dry-run
```

**Export features:**
- 12 columns: Date, Amount, Category, Description, Details (cc_ref), Split Type, Participant Names, My Paid, My Owed, My Net, Splitwise ID, Fingerprint
- Filters Payment transactions (excluded from sheets but tracked in DB)
- Overwrite mode for full refresh or default append mode

### Update Existing Splitwise Expenses

```bash
# Fix self-expenses with incorrect 50/50 splits (make them 100% owed)
python src/update/update_self_expenses.py --start-date 2025-01-01 --end-date 2025-12-31

# Dry run to preview changes
python src/update/update_self_expenses.py --start-date 2025-01-01 --end-date 2025-12-31 --dry-run

# Update a specific expense by ID
python src/update/update_self_expenses.py --expense-id 1234567890

# Limit number of updates (for testing)
python src/update/update_self_expenses.py --start-date 2025-01-01 --limit 10
```

### Bulk Category Updates

Update categories for existing Splitwise expenses in bulk by merchant name or current category:

```bash
# Update all SpotHero expenses to Transportation > Parking
python src/update/bulk_update_categories.py --merchant "SpotHero" --subcategory parking

# Update Amazon (excluding AWS) to Household supplies
python src/update/bulk_update_categories.py --merchant "Amazon" --exclude "AWS" --subcategory household_supplies

# Update Costco expenses currently in "Home - Other" to Household supplies
python src/update/bulk_update_categories.py --merchant "Costco" --current-category "Home - Other" --subcategory-id 14

# Dry run to preview changes
python src/update/bulk_update_categories.py --merchant "SpotHero" --subcategory parking --dry-run

# Skip confirmation prompt
python src/update/bulk_update_categories.py --merchant "Costco" --subcategory household_supplies --yes
```

**Common Subcategory Options:**
- `parking` (ID: 9) - Transportation > Parking
- `household_supplies` (ID: 14) - Home > Household supplies
- `home_other` (ID: 28) - Home > Other
- `medical` (ID: 38) - Life > Medical expenses
- `groceries` (ID: 1) - Food and drink > Groceries
- `dining_out` (ID: 2) - Food and drink > Dining out

See `src/constants/splitwise.py` (SUBCATEGORY_IDS) for the full list of available subcategories.

Or use `--subcategory-id` with any Splitwise subcategory ID.

### Process Refunds and Credits

Refunds are automatically processed during statement import, but you can also process pending refunds separately:

```bash
# Automatic - refunds processed after importing statements
python src/import_statement/pipeline.py --statement data/raw/jan2026.csv

# Manual - process all pending refunds
python -m src.import_statement.process_refunds --verbose

# Dry run - preview refund matching
python -m src.import_statement.process_refunds --dry-run --verbose
```

**How it works:**
- Detects refunds (negative amounts in CSV)
- Matches to original transaction by cc_reference_id or merchant+amount+date
- Creates negative Splitwise expense with same category and split
- Links refund to original in database for audit trail
- Idempotent - safe to re-run, won't create duplicates

See [docs/refund_handling_guide.md](docs/refund_handling_guide.md) for complete documentation.

## Project Structure

```
SplitwiseImporter/
├── src/
│   ├── database/               # Local SQLite database layer
│   │   ├── schema.py           # Table definitions (transactions, monthly_summaries, etc.)
│   │   ├── models.py           # Transaction & ImportLog dataclasses
│   │   └── db_manager.py       # DatabaseManager with CRUD + summary methods
│   ├── db_sync/                # Database sync utilities
│   │   └── sync_from_splitwise.py # Sync DB with Splitwise (insert/update/delete)
│   ├── import_statement/       # CSV statement parsing and import pipeline
│   │   ├── pipeline.py         # Main ETL pipeline orchestrator
│   │   ├── parse_statement.py  # CSV parsing and normalization
│   │   └── categorization.py   # Transaction categorization logic
│   ├── export/                 # Splitwise data export and summaries
│   │   ├── splitwise_export.py # Fetch and export Splitwise expenses
│   │   ├── monthly_export_pipeline.py # Automated 4-step pipeline
│   │   └── generate_summaries.py # Budget analysis and spending summaries
│   ├── update/                 # Bulk update utilities
│   │   ├── update_self_expenses.py # Fix self-expense splits
│   │   └── bulk_update_categories.py # Bulk category updates
│   ├── merchant_review/        # Interactive merchant review workflow
│   │   ├── run_review_workflow.py   # Unified workflow orchestrator (NEW)
│   │   ├── generate_review_file.py  # Generate review CSV from processed data
│   │   ├── review_merchants.py      # Interactive review tool
│   │   └── apply_review_feedback.py # Apply corrections to config
│   ├── common/                 # Shared utilities
│   │   ├── splitwise_client.py # Splitwise API wrapper
│   │   ├── sheets_sync.py      # Google Sheets integration
│   │   └── utils.py            # Common helper functions (simplified merchant extraction)
│   └── constants/              # Configuration constants
├── config/                     # Credentials and mappings
│   ├── .env                    # API keys (not in git)
│   ├── merchant_category_lookup.json  # 219+ merchant→category mappings
│   ├── amex_category_mapping.json     # Amex category mappings
│   └── gsheets_authentication.json    # Google Sheets credentials
├── data/
│   ├── raw/                    # Raw credit card statements
│   ├── processed/              # Processed outputs and review files
│   └── transactions.db         # SQLite database (4,889 transactions: 2013-2026)
├── docs/                       # Documentation
└── notebooks/                  # Jupyter analysis notebooks
```

## 🚀 Core Features

- **Automated Pipeline**: Single-command ETL (Extract, Transform, Load) for monthly statements.
- **Smart Categorization**: 200+ merchant mappings with interactive review and auto-correction.
- **Refund Handling**: Automatic detection and matching of refunds/credits.
- **Budget Analysis**: Detailed monthly summaries, category breakdowns, and year-over-year trends in Google Sheets.
- **Historical Archiving**: Complete transaction history (2013-2026) stored locally in SQLite.
- **Year-Based Exporting**: Clean, structured spreadsheets with separate tabs for each year.
- **Idempotent Updates**: Smart syncing ensures no duplicate entries and minimal API/Sheet writes.

## Common Workflows

### Monthly Expense Processing Pipeline

**IMPORTANT:** Follow this order to ensure proper data flow and chronological sorting:

1. **Import credit card statements to Splitwise**
   ```bash
   # Parse and add new transactions to Splitwise
   python src/import_statement/pipeline.py --statement data/raw/amex_jan2026.csv
   ```

2. **Export Splitwise to Google Sheets** (use overwrite mode)
   ```bash
   # Export with overwrite to maintain chronological sorting
   python src/export/splitwise_export.py --start-date 2026-01-01 --end-date 2026-12-31 --overwrite
   ```

**Why this order matters:**
- Credit card statements may contain retroactive/backdated transactions (e.g., processing delays)
- Splitwise must be updated first with all transactions for the period
- Overwrite mode re-sorts all expenses chronologically, placing backdated entries in correct position
- Append mode would place retroactive expenses at the bottom, breaking chronological order

**Note:** Always use `--overwrite` when exporting after importing statements to maintain proper sorting.

### First-Time Statement Import
1. Place your CSV statement in `data/raw/`
2. Run dry-run to preview: `python src/import_statement/pipeline.py --statement data/raw/statement.csv --dry-run`
3. Review merchant extractions in `data/processed/merchant_names_for_review.csv`
4. Correct any issues: `python src/merchant_review/review_merchants.py`
5. Run actual import: `python src/import_statement/pipeline.py --statement data/raw/statement.csv`

### Large Statement Processing (Batch Mode)
```bash
# Process in batches of 50 transactions
python src/import_statement/pipeline.py --statement data/raw/big_statement.csv --limit 50 --offset 0
python src/import_statement/pipeline.py --statement data/raw/big_statement.csv --limit 50 --offset 50 --append
python src/import_statement/pipeline.py --statement data/raw/big_statement.csv --limit 50 --offset 100 --append
# ... continue until done
```

### Monthly Budget Sync
```bash
# Export current month's Splitwise expenses
python src/export/splitwise_export.py --start-date 2025-01-01 --end-date 2025-01-31 --sheet-name "Jan 2025"

# Full year export with categories
python src/export/splitwise_export.py --start-date 2025-01-01 --end-date 2025-12-31 --overwrite --export-categories
```

## Configuration Files

### merchant_category_lookup.json
Maps merchant names to Splitwise categories. Auto-updated through merchant review workflow.

```json
{
  "spothero": {
    "canonical_name": "SpotHero",
    "category": "Transportation",
    "subcategory": "Parking",
    "confidence": 0.95
  }
}
```

### Environment Variables (.env)
Required API credentials and default settings:

```env
# Splitwise API
SPLITWISE_CONSUMER_KEY=your_key_here
SPLITWISE_CONSUMER_SECRET=your_secret_here
SPLITWISE_API_KEY=your_api_key_here

# Splitwise User IDs (Get these from your Splitwise profile or API)
SPLITWISE_SELF_ID=your_self_user_id_here
SPLITWISE_PARTNER_ID=your_partner_user_id_here

# Google Sheets
SPREADSHEET_KEY=your_google_sheets_key

# Default date range and worksheet (change for new year)
START_DATE=2026-01-01
END_DATE=2026-12-31
EXPENSES_WORKSHEET_NAME=Expenses 2026
DRY_RUN_WORKSHEET_NAME=Statement Imports
```

**Note:** Update `START_DATE`, `END_DATE`, and `EXPENSES_WORKSHEET_NAME` at the start of each year to automatically target the new year's data.

## Tips & Best Practices

- **Always set PYTHONPATH** before running commands: `export PYTHONPATH=/path/to/SplitwiseImporter`
- **Use dry-run first** to preview changes before committing to Splitwise
- **Review merchants regularly** to improve auto-categorization accuracy
- **Process large statements in batches** to handle API rate limits gracefully
- **Use --overwrite for exports** to get a clean dataset with deleted transactions filtered out
- **Check logs** in terminal output for detailed processing information
- **Follow the processing pipeline order**: Import statements to Splitwise first, then export to sheets with `--overwrite`
- **Update config/.env dates** at the start of each year (START_DATE, END_DATE, EXPENSES_WORKSHEET_NAME)

## Automation Considerations

The expense processing workflow can be automated with these steps:

1. **Statement Download**: Automate CSV download from credit card provider (or manual upload to `data/raw/`)
2. **Import to Splitwise**: Run `pipeline.py` with new statement
3. **Export to Sheets**: Run `splitwise_export.py --overwrite` after import completes
4. **Verification**: Check logs for import/export counts and any errors

**Recommended schedule:**
- Run pipeline monthly after credit card statement is available
- Use `--overwrite` mode to handle any retroactive transactions
- Monitor merchant review file for new merchants needing categorization

**Future enhancements:**
- Cron job or GitHub Actions for scheduled execution
- Email/Slack notifications on completion or errors
- Automatic merchant review aggregation and reporting

## Troubleshooting

**Import fails with "ModuleNotFoundError"**: Set PYTHONPATH to project root  
**Duplicate expenses created**: Check cache in `data/splitwise_expense_details_*.json`  
**Wrong categories**: Review and correct in `config/merchant_category_lookup.json`  
**Deleted expenses appearing**: Use `--overwrite` flag when exporting to filter them out  
**Date mismatch (one day off)**: Fixed in export - dates no longer use UTC conversion  
**Category updates not reflected**: Run export with `--overwrite` after bulk updates  
**Wrong year data**: Update `START_DATE`, `END_DATE`, and `EXPENSES_WORKSHEET_NAME` in `config/.env`


