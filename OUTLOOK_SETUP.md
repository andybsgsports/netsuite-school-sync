# Outlook Contacts Sync — Setup Guide

This adds a daily one-way sync from the **Contacts** tab of the Google master
sheet into Andy's Outlook contacts (`andy@bsgsports.com`) via Microsoft Graph.

## How it works

- One Outlook contact folder per `Role` (Football, Basketball, Athletic Director, ...).
- `Department` field = `Sales Rep` (from the Schools tab, joined by School Name).
- `Categories` = `[WIAA-Sync, Sales Rep, Type]` (filter by salesman or coach level in Outlook).
- Match key is **email**. Adds new, updates changed, deletes departed.
- Only contacts tagged with the `WIAA-Sync` category get deleted — your hand-made contacts in the same folder are safe.
- Runs in the existing daily GitHub Actions workflow, right after the NetSuite sync.

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
2. Click **Daily School Sync** -> **Run workflow** -> **Run workflow**
3. Watch the **Run Outlook contacts sync** step.

Then check Outlook (web or desktop) — you should see new contact folders by sport with the contacts populated.

## Re-auth (if it ever stops working)

Refresh tokens for public clients have a ~90-day rolling lifetime. If the daily sync runs continuously, the refresh token stays valid. If you skip 90+ days, or your tenant policy revokes it, the sync will fail with `Silent token refresh failed`. To fix:

1. Run `python outlook_auth_setup.py` again on your laptop
2. Update the `OUTLOOK_TOKEN_CACHE` GitHub Secret with the new cache JSON
3. Delete the local file again

## Files added

- `outlook_auth_setup.py` — one-time local auth, writes token cache
- `outlook_contacts_sync.py` — daily sync engine
- `.github/workflows/daily-sync.yml` — added step + new secrets, added `msal` to pip install

## Notes

- Sync is **one-way**: Sheet -> Outlook. Edits made directly in Outlook are overwritten.
- A row is synced only if `Sync = Y` AND it has both an email and a role.
- Folder names are sanitized (slashes replaced, capped at 64 chars).
