# NetSuite School Sync

Automated pipeline that scrapes Wisconsin (WIAA) and Illinois (IHSA) high school
athletics directories and keeps the corresponding Customer and Contact records
in NetSuite in sync — school info, addresses, admins, and coaches — with zero
manual data entry.

This is a private internal tool for BSG Sports. It is not an open-source
project and is not accepting outside contributions, but the standard
community-health files are kept up to date for maintainability.

## What it does

- **Scrapes** school directory pages (WIAA/IHSA) for address, enrollment,
  colors, nickname, phone, and staff (administrators + coaches).
- **Syncs to NetSuite** — creates/updates Customer records per school and
  Contact records per staff member, with a Ship-To address line per contact.
- **Heals drift** — when the source site changes (address, school rename,
  staff turnover), the next sync run corrects the sheet and NetSuite to
  match, including healing the customer's Bill-To line.
- **Shares co-op coaches** — a coach serving multiple schools gets one
  NetSuite contact card attached to every school via a RESTlet, instead of
  duplicate records per school.
- **Runs nightly** via GitHub Actions (see `.github/workflows/nightly.yml`),
  one job per sales rep, plus a set of on-demand manual workflows for
  one-off fixes and diagnostics.
- **Google Sheets** (`School Sync Master`) is the human-editable source of
  truth between the scrape and NetSuite — see `Schools` and `Contacts` tabs.

## Getting started

See `SETUP_GUIDE.md` for the full list of accounts, credentials, and file
layout. `RESTLET_SETUP.md` and `OUTLOOK_SETUP.md` cover the two optional
integrations (shared contact cards, Outlook digest email).

## Key entry points

| Script | Purpose |
|---|---|
| `push_only.py` | Nightly push: sheet → NetSuite (no scraping) |
| `school_netsuite_sync.py` | Full daily run: scrape WIAA → sheet → NetSuite |
| `ihsa_sync.py` | Illinois (IHSA) equivalent |
| `rep_digests.py` | Per-rep email digest of what changed |
| `netsuite_sync.py` | Core engine — NetSuite API, scraping, address healing |

Manual one-off scripts (duplicate cleanup, ID fixes, etc.) each have a
matching `.github/workflows/manual-*.yml` for a dry-run-then-live workflow.

## Security

Report a suspected credential leak or vulnerability per `SECURITY.md`.
Never commit `.env`, `credentials.json`, or any NetSuite/Google secret —
`secret-scan.yml` runs gitleaks on every push to catch this.
