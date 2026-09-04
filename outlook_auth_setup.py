"""
outlook_auth_setup.py
---------------------
ONE-TIME local auth for the Outlook Contacts sync. Run this on your laptop
to interactively sign in to your bsgsports M365 account. It opens a browser,
you log in, and it saves a token cache JSON that the daily sync can use.

After running:
  1. Copy the entire contents of  outlook_token_cache.json
  2. Add it to GitHub repo  Settings -> Secrets and variables -> Actions
     as a new secret named  OUTLOOK_TOKEN_CACHE
  3. Delete the local file (do NOT commit it).

Usage:
    pip install msal
    python outlook_auth_setup.py
"""

import json
import sys
from pathlib import Path

try:
    from msal import PublicClientApplication, SerializableTokenCache
except ImportError:
    print("ERROR: msal package not installed. Run:  pip install msal")
    sys.exit(1)

# -- Azure app config (from your registered app) ----------------------------
CLIENT_ID  = "15c37b48-585f-437b-8da6-7301d993399e"
TENANT_ID  = "72bd9a57-7017-4871-88c0-2ea274e11fd9"
AUTHORITY  = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES     = ["Contacts.ReadWrite", "User.Read"]
CACHE_FILE = Path(__file__).parent / "outlook_token_cache.json"


def main():
    print("=" * 60)
    print("  Outlook Contacts Sync -- One-Time Auth Setup")
    print("=" * 60)
    print()
    print("This will open a browser. Sign in as andy@bsgsports.com and")
    print("approve the requested permissions.\n")

    cache = SerializableTokenCache()
    if CACHE_FILE.exists():
        cache.deserialize(CACHE_FILE.read_text())

    app = PublicClientApplication(
        CLIENT_ID,
        authority=AUTHORITY,
        token_cache=cache,
    )

    # Try silent first (in case cache already valid)
    accounts = app.get_accounts()
    result = None
    if accounts:
        result = app.acquire_token_silent(SCOPES, account=accounts[0])

    if not result:
        # Interactive sign-in -- opens default browser
        result = app.acquire_token_interactive(
            scopes=SCOPES,
            prompt="select_account",
        )

    if "access_token" not in result:
        print("ERROR: auth failed.")
        print(json.dumps(result, indent=2))
        sys.exit(1)

    # Save the cache (contains the refresh token)
    if cache.has_state_changed:
        CACHE_FILE.write_text(cache.serialize())

    print("\n" + "=" * 60)
    print("  AUTH SUCCESSFUL")
    print("=" * 60)
    print(f"\n  Signed in as: {result.get('id_token_claims', {}).get('preferred_username', '?')}")
    print(f"  Token cache saved to: {CACHE_FILE}")
    print()
    print("NEXT STEPS:")
    print("  1. Open outlook_token_cache.json and copy ALL of its contents.")
    print("  2. Go to GitHub repo -> Settings -> Secrets and variables -> Actions")
    print("  3. Click 'New repository secret', name it: OUTLOOK_TOKEN_CACHE")
    print("  4. Paste the cache JSON as the value, click Add secret.")
    print("  5. Also add these secrets if not already set:")
    print(f"       OUTLOOK_CLIENT_ID = {CLIENT_ID}")
    print(f"       OUTLOOK_TENANT_ID = {TENANT_ID}")
    print("  6. Delete outlook_token_cache.json from your laptop.")
    print()


if __name__ == "__main__":
    main()
