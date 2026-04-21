# WIAA School Sync System - Setup & Operations Guide

## System Overview

This system scrapes Wisconsin school data from the WIAA website daily and syncs it to NetSuite CRM. It handles:
- Scraping school info, administrators, and coaches from WIAA pages
- Creating/updating Customer records in NetSuite
- Creating/updating Contact records in NetSuite
- Auto-inactivating contacts who leave a school
- Storing all data in a Google Sheet for visibility
- Running automatically via GitHub Actions (daily at 5 AM Central)

---

## Accounts & Services

### 1. GitHub Repository (Code & Automation)
- **Repo**: https://github.com/andybsgsports/netsuite-school-sync (private)
- **Account**: `andybsgsports` (logged in via `gh` CLI)
- **GitHub Actions**: Runs daily at 5:00 AM Central (10:00 UTC)
- **Secrets configured**: NS_ACCOUNT, NS_CONSUMER_KEY, NS_CONSUMER_SEC, NS_TOKEN_KEY, NS_TOKEN_SEC, GOOGLE_SHEET_ID, GOOGLE_CREDENTIALS_JSON

### 2. Google Service Account (Sheet Access)
- **Email**: `badger-sync@badger-school-sync.iam.gserviceaccount.com`
- **Project**: `badger-school-sync` (Google Cloud Console)
- **Credentials file**: `credentials.json` (local only, never committed)
- **Purpose**: Reads/writes the Google Sheet programmatically

### 3. Google Sheet (Data Store)
- **Name**: School Sync Master
- **URL**: https://docs.google.com/spreadsheets/d/1iWhtasin-gmk3jllDvls7G1eI_pgzMm4yfQUP_qZHEM/edit
- **Sheet ID**: `1iWhtasin-gmk3jllDvls7G1eI_pgzMm4yfQUP_qZHEM`
- **Owner**: andybsgsports@gmail.com (Andrew Murray)
- **Shared with**: badger-sync service account (Editor)
- **Tabs**:
  - `Schools` — 43 schools with URLs, NS Customer IDs, sync timestamps
  - `Contacts` — ~2077 contacts with names, emails, roles, NS Contact IDs

### 4. NetSuite API (CRM)
- **Account ID**: 11319665
- **Auth method**: OAuth 1.0 (HMAC-SHA256)
- **Credentials**: Stored in `.env` file locally, GitHub Secrets for CI
- **API Base**: `https://11319665.suitetalk.api.netsuite.com/services/rest/record/v1`

---

## File Structure

```
Netsuite Contacts Sync/
  netsuite_sync.py          # Core engine: OAuth auth, WIAA scraper, NS API
  school_netsuite_sync.py   # Daily runner (Google Sheets + GitHub Actions)
  rep_digests.py            # Daily per-rep WIAA digest emails (Gmail SMTP)
  cleanup_duplicate_addresses.py  # Manual duplicate-address cleanup
  Andy-WIAA Script.py       # Local script: scrape + diff + Outlook email
  Andy-School Script.py     # Original version of Andy script (reference)
  run_sync.py               # Legacy local runner (backward compat)
  run_sync.bat              # Task Scheduler entry point
  build_master_sheet.py     # One-time: builds master Excel from WIAA
  ihsa_batch_runner.py      # IHSA batch scraper for Illinois schools
  requirements.txt          # Python dependencies
  .env                      # Local credentials (NEVER committed)
  credentials.json          # Google service account key (NEVER committed)
  snapshots/                # Per-rep diff snapshots (committed by workflow)
  .gitignore                # Excludes sensitive/data files
  .github/workflows/
    daily-sync.yml          # Daily WIAA → Google Sheets → NetSuite sync
    rep-digests.yml         # Daily per-rep digest emails
    manual-cleanup.yml      # On-demand duplicate-address cleanup
```

---

## How It Was Set Up (Step by Step)

### Step 0: Security Fix
- Removed hardcoded NetSuite API credentials from `netsuite_sync.py`
- Added `_load_dotenv()` function to load credentials from `.env` file
- Created `.env` file with all credentials (excluded from git via `.gitignore`)

### Step 1: Created Google Sheet
- Navigated to `sheets.new` in browser to create a blank Google Sheet
- Named it "School Sync Master"
- Sheet ID from URL: `1iWhtasin-gmk3jllDvls7G1eI_pgzMm4yfQUP_qZHEM`

### Step 2: Shared Sheet with Service Account
- Clicked Share button in Google Sheets
- Added `badger-sync@badger-school-sync.iam.gserviceaccount.com` as Editor
- Ran Python script to populate Schools (43 rows) and Contacts (2077 rows) from `School Sync Master.xlsx`
- Normalized column names (First Name -> First, Last Name -> Last, Sync (Y/N) -> Sync)
- Removed duplicate columns

### Step 3: Local Testing
- Added `GOOGLE_SHEET_ID` to `.env` file
- Ran `python school_netsuite_sync.py` with `SCHOOL_FILTER = "Barneveld"` (single school test)
- Verified: WIAA scrape worked, NetSuite customer updated, Google Sheet saved correctly
- Fixed bug: SCHOOL_FILTER was overwriting all schools data on save (now uses separate `schools_to_sync` variable)

### Step 4: GitHub Setup
- Installed GitHub CLI: `winget install --id GitHub.cli`
- Authenticated: `gh auth login --web` (device code flow via browser)
- Created private repo: `gh repo create netsuite-school-sync --private`
- Added remote: `git remote add origin https://github.com/andybsgsports/netsuite-school-sync.git`
- Cleaned up 22 unnecessary debug/utility scripts
- Committed and pushed all code

### Step 5: GitHub Secrets & Workflow
- Set 7 secrets via `gh secret set`:
  - `NS_ACCOUNT` = 11319665
  - `NS_CONSUMER_KEY` = (64-char hex key)
  - `NS_CONSUMER_SEC` = (64-char hex key)
  - `NS_TOKEN_KEY` = (64-char hex key)
  - `NS_TOKEN_SEC` = (64-char hex key)
  - `GOOGLE_SHEET_ID` = 1iWhtasin-gmk3jllDvls7G1eI_pgzMm4yfQUP_qZHEM
  - `GOOGLE_CREDENTIALS_JSON` = (full credentials.json content)
- Enabled GitHub Actions on the repo
- Triggered workflow manually: `gh workflow run "Daily School Sync"`
- Workflow runs daily at 5:00 AM Central (10:00 UTC)

---

## Daily Operations

### Automatic (GitHub Actions)
The sync runs automatically every day at 5:00 AM Central. It:
1. Reads the Schools tab from Google Sheets
2. Scrapes each school's WIAA page for current admins and coaches
3. Updates the NetSuite Customer record with school info
4. Creates new contacts found on WIAA (auto-sync = Y)
5. Inactivates contacts no longer on WIAA (with safeguard: skips if scrape returned 0)
6. Saves all changes back to Google Sheets

### Manual Trigger
To run the sync on-demand:
```bash
gh workflow run "Daily School Sync" --repo andybsgsports/netsuite-school-sync
```
Or go to: https://github.com/andybsgsports/netsuite-school-sync/actions
Click "Daily School Sync" -> "Run workflow"

### Check Workflow Status
```bash
gh run list --repo andybsgsports/netsuite-school-sync
gh run view <run-id> --repo andybsgsports/netsuite-school-sync --log
```

### Local Testing
```bash
# Edit school_netsuite_sync.py: set SCHOOL_FILTER = "Barneveld"
python school_netsuite_sync.py
# Reset: set SCHOOL_FILTER = ""
```

### Andy-WIAA Script (Local Only)
Runs via Windows Task Scheduler. Scrapes WIAA + IHSA, builds sport-tab Excel workbook, diffs against previous version, and emails changes via Outlook.
```bash
python "Andy-WIAA Script.py"
```

---

## Adding a New School

1. Open the Google Sheet: https://docs.google.com/spreadsheets/d/1iWhtasin-gmk3jllDvls7G1eI_pgzMm4yfQUP_qZHEM/edit
2. In the `Schools` tab, add a new row with:
   - `School Name` — e.g., "Madison West"
   - `School URL` — WIAA directory URL
   - `State` — "WI"
   - Leave `NS Customer ID` blank (will auto-create in NetSuite)
3. The next sync run will scrape the school, create the customer in NetSuite, and populate the contacts

---

## Troubleshooting

### Workflow fails
- Check logs: GitHub Actions tab -> click failed run -> view logs
- Common issues: WIAA website down, NetSuite API rate limit, Google Sheet quota

### Contacts not syncing
- Check the `Sync` column in the Contacts tab — must be "Y"
- Contacts with `Sync = "N"` are skipped

### Missing credentials
- Local: check `.env` file exists with all 6 values (NS_ACCOUNT through GOOGLE_SHEET_ID)
- CI: check GitHub Secrets at https://github.com/andybsgsports/netsuite-school-sync/settings/secrets/actions

### Google Sheet access denied
- Verify the service account email has Editor access to the sheet
- Service account: `badger-sync@badger-school-sync.iam.gserviceaccount.com`

---

## Credential Locations

| Credential | Local Location | CI Location |
|---|---|---|
| NetSuite API keys | `.env` file | GitHub Secrets |
| Google service account | `credentials.json` file | `GOOGLE_CREDENTIALS_JSON` secret |
| Google Sheet ID (sync) | `.env` file | `GOOGLE_SHEET_ID` secret |
| Google Sheet ID (reps)  | `.env` file | `GOOGLE_SHEET_ID_REPS` secret (WI School List- Master) |
| Gmail sender | `.env` file | `GMAIL_USER` secret |
| Gmail app password | `.env` file | `GMAIL_APP_PASSWORD` secret |

**Important**: `.env` and `credentials.json` are in `.gitignore` and must NEVER be committed to git.

---

## Rep Digests (per-rep WIAA emails)

Consolidates the six old Desktop/Task-Scheduler scripts (`KyleLrun_wiaa_scraper.py`, etc.)
into a single config-driven script. Reads the "WI School List- Master" sheet, groups schools
by `Sales Rep`, scrapes WIAA for each rep's territory, diffs vs. yesterday, and emails a digest
via Gmail SMTP.

### One-time setup

1. **Create a Gmail app password** (requires 2FA on andy@bsgsports.com or whichever account sends):
   - https://myaccount.google.com/security → 2-Step Verification → App passwords
   - Generate one for "Mail" / "Windows Computer"
   - Copy the 16-char password

2. **Add GitHub Secrets** (repo → Settings → Secrets and variables → Actions):
   ```
   gh secret set GMAIL_USER           --body "andy@bsgsports.com"
   gh secret set GMAIL_APP_PASSWORD   --body "<16-char app password>"
   gh secret set GOOGLE_SHEET_ID_REPS --body "1SlZHbGRvPiO8Qtq7kY2aI0Y9oUsKZ2CxXNXcuw211N0"
   ```

3. **Confirm rep email addresses** in `rep_digests.py` (the `REPS` list near the top).
   Only Paul and KyleL were explicit in the old scripts — the others are best-guesses.

### Testing before going live

The workflow defaults to DRY-RUN on both scheduled and manual triggers. In dry-run mode all
emails are redirected to `GMAIL_USER` so you can confirm formatting and rep splits.

```bash
# Trigger a dry run against just one rep:
gh workflow run "Rep Digests" -f rep_filter="Kyle Loughrin" -f dry_run=true

# Trigger a live run against one rep (DO THIS FIRST when flipping live):
gh workflow run "Rep Digests" -f rep_filter="Kyle Loughrin" -f dry_run=false
```

### Going fully live

Once dry-run output looks right for every rep, change the default `DRY_RUN` env in
`.github/workflows/rep-digests.yml` — the expression

```yaml
DRY_RUN: ${{ (inputs.dry_run == false && github.event_name == 'workflow_dispatch') && '0' || '1' }}
```

should become

```yaml
DRY_RUN: ${{ inputs.dry_run && '1' || '0' }}
```

That flips scheduled (cron) runs to live while letting manual dispatches still opt into dry-run.

### Snapshots

Each rep's contact set is snapshotted to `snapshots/{Rep_Name}.json` after each run. The
workflow commits these back to `master` with `[skip ci]`. Diffs on the next run compare
against the committed snapshot. First run for any rep has no prior snapshot → sends a
"Initial snapshot" digest.
