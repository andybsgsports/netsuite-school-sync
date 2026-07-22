# Security Policy

This repository automates writes to a live NetSuite account and a Google
Sheet using OAuth credentials stored as GitHub Actions secrets and, for
local runs, a `.env` file / `credentials.json`. The main risks are
credential leakage and unintended live writes.

## Reporting a vulnerability or leaked credential

If you find a security issue — a committed secret, a workflow that exposes
a credential in logs, an auth bypass, or anything else — **do not open a
public issue**. Contact the repo owner (andybsgsports) directly and
privately so credentials can be rotated before the report is public.

If you discover that a NetSuite, Google service-account, or Outlook
credential has been committed to history (even in an old commit), treat it
as compromised:

1. Rotate the credential in NetSuite / Google Cloud Console / Outlook
   immediately — do not wait for the git history to be cleaned up.
2. Remove it from git history (not just the current tree) or purge the
   affected commits.
3. Update the corresponding GitHub Actions secret.

## What's already in place

- `secret-scan.yml` runs [gitleaks](https://github.com/gitleaks/gitleaks)
  (config: `.gitleaks.toml`) on every push and pull request. A finding
  fails CI — do not bypass this with `--no-verify` or by disabling the
  check.
- `.gitignore` excludes `.env`, `credentials.json`, and other local
  secret files from being committed in the first place.
- Every script that writes to NetSuite or the Google Sheet defaults to a
  **dry run** and requires an explicit `LIVE=1` / `--live` flag (or a
  workflow `live` checkbox) to make real changes. Prefer this pattern for
  any new script that touches live data.

## Supported scope

This is a single-environment internal tool — there's no versioned release
line to patch, so "supported version" doesn't apply. Fixes land on
`master` and take effect on the next scheduled or manual run.
