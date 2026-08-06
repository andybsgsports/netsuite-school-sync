# Outlook Contacts Sync — Setup Guide

This adds a daily one-way sync from the **Contacts** tab of the Google master
sheet into Andy's Outlook contacts (`andy@bsgsports.com`) via Microsoft Graph.

## How it works

- One Outlook contact folder per `Role` (Football, Basketball, Athletic Director, ...).
- `Department` field = `Sales Rep` (from the Schools tab, joined by School Name).
- `Categories` = `[WIAA-Sync, Sales Rep, Type]` (filter by salesman or coach level in Outlook).
- Match key is **email**. Adds new, updates changed, deletes departed.
- Only contacts tagged with the `WIAA-Sync` category get deleted — your hand-made contacts in the same folder are safe.
- Runs as its own workflow `Outlook Contacts Sync`, triggered automatically after `Rep Digests` finishes each morning (with a 7:30 AM Central backstop).

## Azure setup (already done)

App registration: `WIAA Contacts Sync`
- Client ID: `15c37b48-585f-437b-8da6-7301d993399e`
- Tenant ID: `72bd9a57-7017-4871-88c0-2ea274e11fd9`
- Permission: `Microsoft Graph -> Contacts.ReadWrite` (Delegated)
- Redirect URI: `http://localhost`
- Allow public client flows: enabled

## One-time local auth

Run on your laptop (Windows, the same machine that runs Task Scheduler):

```bash
pip install msal
python outlook_auth_setup.py
```

A browser opens. Sign in as `andy@bsgsports.com` and approve.

The script writes `outlook_token_cache.json` next to itself.

## Add GitHub Secrets

Go to https://github.com/andybsgsports/netsuite-school-sync/settings/secrets/actions
and add three new repository secrets:

| Name | Value |
|---|---|
| `OUTLOOK_CLIENT_ID` | `15c37b48-585f-437b-8da6-7301d993399e` |
| `OUTLOOK_TENANT_ID` | `72bd9a57-7017-4871-88c0-2ea274e11fd9` |
| `OUTLOOK_TOKEN_CACHE` | the full contents of `outlook_token_cache.json` |

Then **delete `outlook_token_cache.json` from your laptop** — it contains your refresh token. Do not commit it.

(`.gitignore` already excludes it via the catch-all for `*token_cache*` if you use that pattern, otherwise add a line: `outlook_token_cache.json`.)

## Verify

Manually trigger the workflow:
1. Go to https://github.com/andybsgsports/netsuite-school-sync/actions
2. Click **Outlook Contacts Sync** -> **Run workflow** -> **Run workflow**
3. Watch the **Run Outlook contacts sync** step.

Then check Outlook (web or desktop) — you should see new contact folders by sport with the contacts populated.

## Token auto-rotation (added Aug 2026)

Refresh tokens for public clients have a ~90-day lifetime, and a static
GitHub Secret goes stale (this bit us on 2026-08-01). The workflow now
self-rotates: after each run, the freshest MSAL token cache is encrypted
with the `OUTLOOK_CACHE_KEY` secret (Fernet) and committed to the repo as
`outlook_token_cache.enc`. Load priority is `.enc` file first, then the
`OUTLOOK_TOKEN_CACHE` secret (bootstrap/fallback), then a local
`outlook_token_cache.json`. No scheduled re-auth is needed anymore.

## Re-auth (only if rotation breaks)

If the token is revoked by tenant policy, or the sync is down long enough
that the stored token dies, it fails with `Silent token refresh failed`. To fix:

1. Run `python outlook_auth_setup.py` again on your laptop
2. Update the `OUTLOOK_TOKEN_CACHE` GitHub Secret with the new cache JSON
3. Delete the local file again
4. Delete `outlook_token_cache.enc` from the repo (so the fresh secret wins),
   then trigger the workflow — it re-bootstraps the encrypted file

## Files added

- `outlook_auth_setup.py` — one-time local auth, writes token cache
- `outlook_contacts_sync.py` — daily sync engine
- `.github/workflows/outlook-sync.yml` — new standalone workflow (chains off Rep Digests)

## Notes

- Sync is **one-way**: Sheet -> Outlook. Edits made directly in Outlook are overwritten.
- A row is synced only if `Sync = Y` AND it has both an email and a role.
- Folder names are sanitized (slashes replaced, capped at 64 chars).
