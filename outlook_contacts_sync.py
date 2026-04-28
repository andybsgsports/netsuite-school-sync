"""
outlook_contacts_sync.py
------------------------
Daily sync: reads the Contacts tab from the Google master sheet and
mirrors it into Andy's Outlook contacts (andy@bsgsports.com) via Microsoft
Graph.

Folder structure: one Outlook contact folder per Role
  (Football, Basketball, Athletic Director, Principal, ...).
Department field = Sales Rep (looked up from Schools tab via School Name).
Categories = [Sales Rep, Type] for filtering.

Match key: email. Adds new, updates changed, deletes departed.

Auth: MSAL public-client refresh-token flow. Token cache lives in the
OUTLOOK_TOKEN_CACHE env var (whole JSON pasted in as a GitHub Secret).
"""

import os
import sys
import json
import time
import re
from datetime import datetime
from typing import Dict, List, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials
from msal import PublicClientApplication, SerializableTokenCache

# -- Config ------------------------------------------------------------------
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "")
GOOGLE_SCOPES   = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

CLIENT_ID  = os.environ.get("OUTLOOK_CLIENT_ID", "15c37b48-585f-437b-8da6-7301d993399e")
TENANT_ID  = os.environ.get("OUTLOOK_TENANT_ID", "72bd9a57-7017-4871-88c0-2ea274e11fd9")
AUTHORITY  = f"https://login.microsoftonline.com/{TENANT_ID}"
GRAPH_SCOPES = ["Contacts.ReadWrite", "User.Read"]
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# Sheet column names (must match school_netsuite_sync.py)
C_SCHOOL = "School Name"
C_FIRST  = "First"
C_LAST   = "Last"
C_EMAIL  = "Email"
C_ROLE   = "Role"
C_TYPE   = "Type"
C_SYNC   = "Sync"

S_NAME  = "School Name"
S_SALES = "Sales Rep"

# Tag we put on Categories so we only touch contacts WE created
SYNC_TAG = "WIAA-Sync"


# -- Google Sheets helpers ---------------------------------------------------
def get_gspread_client():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if creds_json:
        info = json.loads(creds_json)
        creds = Credentials.from_service_account_info(info, scopes=GOOGLE_SCOPES)
    else:
        creds_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "credentials.json")
        creds = Credentials.from_service_account_file(creds_file, scopes=GOOGLE_SCOPES)
    return gspread.authorize(creds)


def load_contacts_and_schools(gc):
    wb = gc.open_by_key(GOOGLE_SHEET_ID)
    contacts_ws = wb.worksheet("Contacts")
    schools_ws  = wb.worksheet("Schools")
    contacts = contacts_ws.get_all_records()
    schools  = schools_ws.get_all_records()

    # School Name -> Sales Rep map
    rep_by_school: Dict[str, str] = {}
    for s in schools:
        name = str(s.get(S_NAME, "")).strip()
        rep  = str(s.get(S_SALES, "")).strip()
        if name:
            rep_by_school[name.lower()] = rep

    return contacts, rep_by_school


# -- Auth --------------------------------------------------------------------
def get_access_token() -> str:
    """Load token cache from env, refresh, return access token."""
    cache_json = os.environ.get("OUTLOOK_TOKEN_CACHE", "")
    if not cache_json:
        # Fallback: read from local file (for local debugging)
        local = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             "outlook_token_cache.json")
        if os.path.exists(local):
            cache_json = open(local).read()
        else:
            raise RuntimeError(
                "No OUTLOOK_TOKEN_CACHE env var set and no local "
                "outlook_token_cache.json found. Run outlook_auth_setup.py first."
            )

    cache = SerializableTokenCache()
    cache.deserialize(cache_json)

    app = PublicClientApplication(CLIENT_ID, authority=AUTHORITY, token_cache=cache)
    accounts = app.get_accounts()
    if not accounts:
        raise RuntimeError("No accounts in token cache. Re-run outlook_auth_setup.py.")

    result = app.acquire_token_silent(GRAPH_SCOPES, account=accounts[0])
    if not result or "access_token" not in result:
        raise RuntimeError(
            "Silent token refresh failed. Re-run outlook_auth_setup.py to "
            "re-auth and update the OUTLOOK_TOKEN_CACHE secret."
        )
    return result["access_token"]


# -- Graph API helpers -------------------------------------------------------
class Graph:
    def __init__(self, token: str):
        self.s = requests.Session()
        self.s.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
            "Accept":        "application/json",
        })

    def _req(self, method: str, url: str, **kw):
        if not url.startswith("http"):
            url = GRAPH_BASE + url
        for attempt in range(3):
            r = self.s.request(method, url, timeout=30, **kw)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", "5"))
                print(f"  [GRAPH] 429 throttled, sleeping {wait}s")
                time.sleep(wait)
                continue
            if r.status_code >= 500 and attempt < 2:
                time.sleep(2 ** attempt)
                continue
            return r
        return r

    def get_all(self, url: str) -> List[dict]:
        """GET with @odata.nextLink pagination."""
        out = []
        next_url = url
        while next_url:
            r = self._req("GET", next_url)
            r.raise_for_status()
            data = r.json()
            out.extend(data.get("value", []))
            next_url = data.get("@odata.nextLink")
        return out

    def post(self, url: str, body: dict) -> dict:
        r = self._req("POST", url, data=json.dumps(body))
        if r.status_code not in (200, 201):
            raise RuntimeError(f"POST {url} -> {r.status_code}: {r.text[:300]}")
        return r.json()

    def patch(self, url: str, body: dict) -> dict:
        r = self._req("PATCH", url, data=json.dumps(body))
        if r.status_code not in (200, 204):
            raise RuntimeError(f"PATCH {url} -> {r.status_code}: {r.text[:300]}")
        return r.json() if r.text else {}

    def delete(self, url: str):
        r = self._req("DELETE", url)
        if r.status_code not in (200, 204):
            raise RuntimeError(f"DELETE {url} -> {r.status_code}: {r.text[:300]}")


# -- Folder management -------------------------------------------------------
def safe_folder_name(role: str) -> str:
    """Clean a Role string for use as an Outlook folder display name."""
    name = re.sub(r"\s+", " ", role).strip()
    # Outlook folder names can't contain certain chars in some clients
    name = name.replace("/", "-").replace("\\", "-")
    return name[:64] or "Other"


def ensure_folders(g: Graph, role_names: List[str]) -> Dict[str, str]:
    """Return {role -> folder_id}, creating any missing folders."""
    existing = g.get_all("/me/contactFolders")
    by_name = {f["displayName"]: f["id"] for f in existing}

    folder_ids: Dict[str, str] = {}
    for role in role_names:
        fname = safe_folder_name(role)
        if fname in by_name:
            folder_ids[role] = by_name[fname]
        else:
            print(f"  [FOLDER] creating: {fname}")
            r = g.post("/me/contactFolders", {"displayName": fname})
            folder_ids[role] = r["id"]
            by_name[fname] = r["id"]
    return folder_ids


# -- Contact mapping ---------------------------------------------------------
def build_contact_payload(row: dict, sales_rep: str) -> dict:
    first = str(row.get(C_FIRST, "")).strip()
    last  = str(row.get(C_LAST, "")).strip()
    email = str(row.get(C_EMAIL, "")).strip().lower()
    role  = str(row.get(C_ROLE, "")).strip()
    ctype = str(row.get(C_TYPE, "")).strip()
    school = str(row.get(C_SCHOOL, "")).strip()

    display = f"{first} {last}".strip() or email

    payload = {
        "givenName":   first,
        "surname":     last,
        "displayName": display,
        "companyName": school,
        "jobTitle":    f"{ctype} - {role}".strip(" -") if ctype or role else "",
        "department":  sales_rep,
        "emailAddresses": [{
            "address": email,
            "name":    display,
        }] if email else [],
        "categories":  [c for c in (SYNC_TAG, sales_rep, ctype) if c],
    }
    return payload


def contact_email(c: dict) -> str:
    addrs = c.get("emailAddresses") or []
    if not addrs:
        return ""
    return (addrs[0].get("address") or "").strip().lower()


def contact_needs_update(existing: dict, desired: dict) -> bool:
    """Compare the fields we care about."""
    fields = ["givenName", "surname", "displayName", "companyName",
              "jobTitle", "department"]
    for f in fields:
        if (existing.get(f) or "") != (desired.get(f) or ""):
            return True
    # Email
    if contact_email(existing) != contact_email(desired):
        return True
    # Categories (compare as sets)
    if set(existing.get("categories") or []) != set(desired.get("categories") or []):
        return True
    return False


# -- Main sync ---------------------------------------------------------------
def main():
    print("=" * 60)
    print(f"  Outlook Contacts Sync  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    if not GOOGLE_SHEET_ID:
        print("ERROR: GOOGLE_SHEET_ID env var not set.")
        sys.exit(1)

    # 1. Load source data
    print("\n[1/5] Loading Google Sheet...")
    gc = get_gspread_client()
    contacts_data, rep_by_school = load_contacts_and_schools(gc)

    # Filter to syncable rows with email
    syncable = []
    for row in contacts_data:
        if str(row.get(C_SYNC, "N")).strip().upper() != "Y":
            continue
        if not str(row.get(C_EMAIL, "")).strip():
            continue
        if not str(row.get(C_ROLE, "")).strip():
            continue
        syncable.append(row)
    print(f"  {len(syncable)} syncable contacts (have email + role + Sync=Y)")

    # 2. Auth + Graph client
    print("\n[2/5] Authenticating to Microsoft Graph...")
    token = get_access_token()
    g = Graph(token)

    # 3. Build desired state: {role -> {email -> payload}}
    print("\n[3/5] Building desired contact state...")
    desired: Dict[str, Dict[str, dict]] = {}
    for row in syncable:
        role  = str(row[C_ROLE]).strip()
        email = str(row[C_EMAIL]).strip().lower()
        school = str(row.get(C_SCHOOL, "")).strip()
        rep    = rep_by_school.get(school.lower(), "")
        payload = build_contact_payload(row, rep)
        desired.setdefault(role, {})[email] = payload
    print(f"  {len(desired)} role-folders, "
          f"{sum(len(v) for v in desired.values())} contacts")

    # 4. Ensure folders exist
    print("\n[4/5] Ensuring Outlook contact folders exist...")
    folder_ids = ensure_folders(g, list(desired.keys()))

    # 5. Sync each folder
    print("\n[5/5] Syncing contacts per folder...")
    totals = {"added": 0, "updated": 0, "deleted": 0, "unchanged": 0, "skipped": 0}

    for role, desired_by_email in desired.items():
        fid = folder_ids[role]
        fname = safe_folder_name(role)
        print(f"\n  [{fname}] desired={len(desired_by_email)}")

        existing = g.get_all(
            f"/me/contactFolders/{fid}/contacts"
            "?$select=id,givenName,surname,displayName,companyName,jobTitle,"
            "department,emailAddresses,categories"
        )
        existing_by_email = {}
        for c in existing:
            em = contact_email(c)
            if em:
                existing_by_email[em] = c

        # Track which existing emails we've handled (to compute deletions)
        handled = set()

        for email, payload in desired_by_email.items():
            ex = existing_by_email.get(email)
            if not ex:
                try:
                    g.post(f"/me/contactFolders/{fid}/contacts", payload)
                    totals["added"] += 1
                except Exception as e:
                    print(f"    ERROR add {email}: {e}")
                    totals["skipped"] += 1
            else:
                handled.add(email)
                if contact_needs_update(ex, payload):
                    try:
                        g.patch(f"/me/contacts/{ex['id']}", payload)
                        totals["updated"] += 1
                    except Exception as e:
                        print(f"    ERROR update {email}: {e}")
                        totals["skipped"] += 1
                else:
                    totals["unchanged"] += 1

        # Delete contacts in folder that we created but are no longer in source
        # Only delete if SYNC_TAG is in categories (so we don't nuke contacts
        # that happen to share a folder name but were created by hand)
        for em, ex in existing_by_email.items():
            if em in handled or em in desired_by_email:
                continue
            cats = ex.get("categories") or []
            if SYNC_TAG not in cats:
                continue
            try:
                g.delete(f"/me/contacts/{ex['id']}")
                totals["deleted"] += 1
            except Exception as e:
                print(f"    ERROR delete {em}: {e}")
                totals["skipped"] += 1

    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    print(f"  Added:     {totals['added']}")
    print(f"  Updated:   {totals['updated']}")
    print(f"  Deleted:   {totals['deleted']}")
    print(f"  Unchanged: {totals['unchanged']}")
    print(f"  Skipped:   {totals['skipped']}")
    print()


if __name__ == "__main__":
    main()
