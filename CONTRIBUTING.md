# Contributing

This is a private, single-maintainer repository for BSG Sports' internal
NetSuite/WIAA/IHSA sync automation. It's not open for outside
contributions, but this doc captures how changes are made here so the
project stays maintainable over time.

## Workflow

1. **Branch from `master`.** Use a descriptive branch name
   (`fix-monroe-address`, `heal-school-renames`, etc.).
2. **One concern per PR.** Keep changes scoped and reviewable — a bug fix
   shouldn't also refactor unrelated code.
3. **Test before pushing.** Most sync logic can be verified offline with
   stubbed NetSuite/Sheets calls (see recent PR history for examples) since
   `gspread`/`cryptography` and live NetSuite credentials aren't always
   available in every environment. Run `python -m py_compile <file>.py` at
   minimum before every commit.
4. **Open a PR against `master`.** Describe *why* the change is needed
   (what broke, what data proved it), not just what the diff does.
5. **CI must pass.** `secret-scan.yml` (gitleaks) runs on every push and
   PR — a failure here blocks merge and must be fixed, not bypassed.
6. **Squash-merge** once approved.

## Where things live

- **Core sync logic**: `netsuite_sync.py` (NetSuite API + scraping),
  `push_only.py` (nightly sheet→NetSuite push), `school_netsuite_sync.py`
  (full daily WIAA run), `ihsa_sync.py` (Illinois equivalent).
- **One-off/manual fixes**: a standalone script (e.g. `fix_*.py`,
  `cleanup_*.py`) plus a matching `.github/workflows/manual-*.yml` that
  defaults to a dry run and requires an explicit `live` flag to apply
  changes to NetSuite or the sheet.
- **Scheduled automation**: `.github/workflows/nightly.yml`,
  `rep-digests.yml`, `ihsa-sync.yml`.

## Conventions worth preserving

- **Dry-run by default.** Any script that writes to NetSuite or the sheet
  should default to printing its plan and require `LIVE=1` / `--live` (or
  a workflow checkbox) to actually apply changes.
- **Idempotent heals, not one-time patches.** When fixing stale data (an
  address, a renamed school, a departed contact), prefer fixing the
  underlying nightly sync logic so it self-heals going forward, over a
  one-off script that only fixes today's snapshot.
- **Never commit secrets.** Credentials come from GitHub Actions secrets
  or a local `.env`/`credentials.json` (gitignored). See `SECURITY.md`.

## Questions

Contact the repo owner (andybsgsports) directly.
