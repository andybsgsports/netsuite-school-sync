"""
ihsa_sync.py
------------
Daily Illinois sync. Uses IHSA's public REST API directly (no Selenium, no
Chrome). For each school on the IL_Schools tab:
  1. GET /v1/schools/{id}/staff2 -> full roster (admins + coaches + medical
     + activities) with PersonID, Name, DefaultTitle, RoleID, HasEmail
  2. For each person with HasEmail=true:
     GET /v1/schools/{id}/staff/{PersonID}/email -> {"email": "..."}
  3. Add to Contacts tab (auto-sync = Y), push Sync=Y contacts to NetSuite,
     inactivate departed contacts.

Does NOT touch the NetSuite Customer record — IHSA does not expose the rich
fields we populate for WI schools, and blanking them would be lossy.

Env:
  GOOGLE_SHEET_ID, GOOGLE_CREDENTIALS_JSON, NS_*
  SCHOOL_FILTER (optional) — only sync this school name
"""

import json
import os
import re
import sys
import time
from datetime import datetime

import gspread
import requests
from google.oauth2.service_account import Credentials

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from netsuite_sync import sync_contact, inactivate_contact, compute_school_domain, smart_title

# -- Config ------------------------------------------------------------------
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "")
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
DELAY_BETWEEN_SCHOOLS = 1.0
DELAY_BETWEEN_EMAILS = 0.2  # between email-reveal calls

# IHSA API headers — minimum set the public site uses.
IHSA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Sec-Fetch-Site": "same-site",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "Referer": "https://www.ihsa.org/",
    "Origin": "https://www.ihsa.org",
}
IHSA_API = "https://api.ihsa.org/v1"

SCHOOL_FILTER = os.environ.get("SCHOOL_FILTER", "").strip()

# IL_Schools columns
S_NAME   = "Schools"
S_URL    = "School Website"
S_STATE  = "State"
S_NS_ID  = "NS Customer ID"
S_SALES  = "Sales Rep"
S_SYNCED = "Last Synced"
S_NOTES  = "Notes"

# Contacts tab columns (shared with WI)
C_SCHOOL = "School Name"
C_FIRST  = "First"
C_LAST   = "Last"
C_EMAIL  = "Email"
C_ROLE   = "Role"
C_TYPE   = "Type"
C_SYNC   = "Sync"
C_NS_CID = "NS Contact ID"
C_NS_CUS = "NS Customer ID"
C_SYNCED = "Last Synced"

# IHSA sections that are administrative (non-sport). Everything else is a
# coach/activity entry where DefaultTitle ends in "... Head Coach" / "Coach".
ADMIN_SECTIONS = {
    "Administration",
    "Athletic Medical Staff",
}


def parse_title_for_sheet(default_title, role_id, section):
    """
    Map an IHSA staff entry onto the WI Contacts-tab shape:
      Coaches: role = sport ("Boys Baseball"), type = "Head Coach" / "Coach"
      Admins:  role = full title,              type = "Admin"
    """
    title = (default_title or "").strip()
    low = title.lower()

    if section in ADMIN_SECTIONS:
        return title, "Admin"

    # Coach variants — strip the coach suffix to isolate the sport.
    if "head coach" in low:
        sport = re.sub(r"(?i)\s*head\s*coach\s*$", "", title).strip()
        return sport or title, "Head Coach"
    if "assistant coach" in low:
        sport = re.sub(r"(?i)\s*assistant\s*coach\s*$", "", title).strip()
        return sport or title, "Assistant Coach"
    if re.search(r"(?i)\bcoach\b", title):
        sport = re.sub(r"(?i)\s*coach\s*$", "", title).strip()
        return sport or title, "Coach"

    # Adviser / Director / Band Director etc. — not a sport coach, treat as Admin
    return title, "Admin"


# -- Google Sheets -----------------------------------------------------------
def get_gspread_client():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if creds_json:
        creds = Credentials.from_service_account_info(
            json.loads(creds_json), scopes=GOOGLE_SCOPES
        )
    else:
        creds_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "credentials.json")
        creds = Credentials.from_service_account_file(creds_file, scopes=GOOGLE_SCOPES)
    return gspread.authorize(creds)


def load_sheet(gc):
    wb = gc.open_by_key(GOOGLE_SHEET_ID)
    il_ws = wb.worksheet("IL_Schools")
    contacts_ws = wb.worksheet("Contacts")
    return il_ws.get_all_records(), contacts_ws.get_all_records(), il_ws, contacts_ws


def save_il_schools(ws, rows):
    if not rows:
        return
    headers = list(rows[0].keys())
    vals = [headers] + [[str(r.get(h, "") or "") for h in headers] for r in rows]
    ws.clear()
    ws.update(range_name="A1", values=vals)
    print(f"  [SHEETS] IL_Schools saved ({len(rows)} rows)")


def save_contacts(ws, rows):
    if not rows:
        return
    headers = [C_SCHOOL, C_FIRST, C_LAST, C_EMAIL, C_ROLE, C_TYPE, C_SYNC,
               C_NS_CID, C_NS_CUS, C_SYNCED]
    clean = [r for r in rows if str(r.get(C_SCHOOL, "")).strip()]
    vals = [headers] + [[str(r.get(h, "") or "") for h in headers] for r in clean]
    ws.clear()
    ws.update(range_name="A1", values=vals)
    print(f"  [SHEETS] Contacts saved ({len(clean)} rows)")


# -- IHSA API ----------------------------------------------------------------
def extract_school_id(url_or_id):
    """Accept either the full URL or bare ID; return zero-padded ID string."""
    s = str(url_or_id).strip()
    m = re.search(r"/details/(\d+)", s)
    if m:
        return m.group(1).zfill(4)
    if s.isdigit():
        return s.zfill(4)
    return None


def strip_honorific(name):
    """'Mr. John Smith' -> 'John Smith'."""
    return re.sub(r"^(Mr\.?|Mrs\.?|Ms\.?|Dr\.?|Coach)\s+", "", name or "", flags=re.IGNORECASE).strip()


def split_first_last(name, fallback_last=""):
    """
    Split 'First Last' into (first, last). Handles IHSA's preferred-name
    convention where the name they go by is in parentheses:
      'Trey (Michael) Hickey' -> ('Michael', 'Hickey')
      'Mr. Adam McDonald'     -> ('Adam', 'McDonald')
    """
    clean = strip_honorific(name)
    # Preferred name in parens: take the bracketed name, drop the casual one.
    m = re.match(r"^(\S+)\s+\(([^)]+)\)\s+(.+)$", clean)
    if m:
        preferred = m.group(2).strip()
        rest = m.group(3).strip()
        return preferred, rest
    # Strip any stray parens that might still be in the string.
    clean = re.sub(r"\([^)]*\)", "", clean).strip()
    clean = re.sub(r"\s+", " ", clean)
    parts = clean.split()
    if len(parts) >= 2:
        return parts[0], " ".join(parts[1:])
    if parts:
        return parts[0], fallback_last or ""
    return "", fallback_last or ""


def fetch_school_staff(school_id):
    """Fetch full roster from IHSA. Returns list of normalized contact dicts."""
    r = requests.get(f"{IHSA_API}/schools/{school_id}/staff2", headers=IHSA_HEADERS, timeout=15)
    if r.status_code != 200:
        print(f"    [IHSA] staff2 failed: {r.status_code}")
        return []
    data = r.json().get("data", {})
    people = []
    for section, members in data.items():
        for m in members:
            pid = m.get("PersonID")
            has_email = bool(m.get("HasEmail"))
            default_title = m.get("DefaultTitle") or ""
            role_id = m.get("RoleID") or ""
            if not default_title:
                # IHSA placeholder row with no title — skip (they appear alongside
                # real entries for the same coach).
                continue
            # API sometimes returns explicit null for these — use `or ""` not default.
            first, last = split_first_last(m.get("Name") or "", fallback_last=m.get("LastName") or "")
            sheet_role, sheet_type = parse_title_for_sheet(default_title, role_id, section)
            people.append({
                "person_id": pid,
                "first": smart_title(first),
                "last":  smart_title(last),
                "role": sheet_role,    # sport name for coaches, title for admins
                "type": sheet_type,    # "Head Coach" / "Coach" / "Admin"
                "default_title": default_title,  # original IHSA title kept for digest xlsx
                "role_id": role_id,
                "has_email": has_email,
                "phone": m.get("Phone") or "",
                "email": "",  # filled later
            })
    return people


def fetch_email(school_id, person_id):
    """Resolve an email via the gated reveal endpoint."""
    r = requests.get(
        f"{IHSA_API}/schools/{school_id}/staff/{person_id}/email",
        headers=IHSA_HEADERS, timeout=15,
    )
    if r.status_code != 200:
        return ""
    try:
        return str(r.json().get("email", "")).strip()
    except ValueError:
        return ""


def scrape_school(school_id):
    """Scrape one school: roster + emails. Returns list of contact dicts."""
    people = fetch_school_staff(school_id)
    for p in people:
        if p["has_email"] and p["person_id"]:
            p["email"] = fetch_email(school_id, p["person_id"])
            time.sleep(DELAY_BETWEEN_EMAILS)
    # Drop anyone we couldn't get an email for — the sheet's key is (school, email, role)
    return [p for p in people if p["email"]]


# -- Main --------------------------------------------------------------------
def main():
    print(f"{'='*60}")
    print(f"  IL Sync (API-based)  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}\n")

    if not GOOGLE_SHEET_ID:
        print("ERROR: GOOGLE_SHEET_ID not set")
        sys.exit(1)

    gc = get_gspread_client()
    il_schools, contacts, il_ws, contacts_ws = load_sheet(gc)

    if SCHOOL_FILTER:
        schools_to_sync = [s for s in il_schools if s.get(S_NAME, "").strip() == SCHOOL_FILTER]
        print(f"  TEST MODE: only '{SCHOOL_FILTER}'")
    else:
        schools_to_sync = il_schools

    print(f"  IL schools: {len(schools_to_sync)}  |  Existing contacts: {len(contacts)}\n")

    synced = 0
    skipped_no_ns_id = 0
    contact_creates = 0
    contact_updates = 0
    contact_inactivates = 0

    for school_row in schools_to_sync:
        school_name = str(school_row.get(S_NAME, "")).strip()
        url         = str(school_row.get(S_URL, "")).strip()
        ns_id       = str(school_row.get(S_NS_ID, "")).strip()
        state       = str(school_row.get(S_STATE, "IL")).strip() or "IL"

        if not (school_name and url):
            continue
        school_id = extract_school_id(url)
        if not school_id:
            print(f"\n[IL] {school_name} — can't parse IHSA ID from {url!r}, skipping")
            continue

        print(f"\n{'-'*60}\n[IL] {school_name}  (ihsa id {school_id})")

        site_contacts = scrape_school(school_id)
        print(f"  Scraped: {len(site_contacts)} contacts (with email)")

        if ns_id in ("", "nan", "None", "0"):
            # Record found contacts as Sync=N so Andy sees them; skip NS sync.
            school_row[S_NOTES] = (str(school_row.get(S_NOTES, "")) + " [needs NS Customer ID]").strip()
            skipped_no_ns_id += 1
            print(f"  SKIP NS sync — no NS Customer ID")
            existing_keys = {
                (c.get(C_EMAIL, "").strip().lower(), c.get(C_ROLE, "").strip().lower())
                for c in contacts
                if c.get(C_SCHOOL, "").strip() == school_name
            }
            for sc in site_contacts:
                key = (sc["email"].lower(), sc["role"].lower())
                if key in existing_keys:
                    continue
                contacts.append({
                    C_SCHOOL: school_name, C_FIRST: sc["first"], C_LAST: sc["last"],
                    C_EMAIL:  sc["email"], C_ROLE:  sc["role"], C_TYPE:  sc["type"],
                    C_SYNC:   "N", C_NS_CID: "", C_NS_CUS: "", C_SYNCED: "",
                })
                existing_keys.add(key)
            continue

        existing_keys = {
            (c.get(C_EMAIL, "").strip().lower(), c.get(C_ROLE, "").strip().lower())
            for c in contacts
            if c.get(C_SCHOOL, "").strip() == school_name
        }
        site_emails = {sc["email"].lower() for sc in site_contacts}

        for sc in site_contacts:
            key = (sc["email"].lower(), sc["role"].lower())
            if key in existing_keys:
                continue
            contacts.append({
                C_SCHOOL: school_name, C_FIRST: sc["first"], C_LAST: sc["last"],
                C_EMAIL:  sc["email"], C_ROLE:  sc["role"], C_TYPE:  sc["type"],
                C_SYNC:   "Y", C_NS_CID: "", C_NS_CUS: ns_id, C_SYNCED: "",
            })
            existing_keys.add(key)
            print(f"  + New: {sc['first']} {sc['last']} — {sc['role']} [{sc['type']}]")
            contact_creates += 1

        # Pass the school's institutional email domain so sync_contact can
        # claim home-school primary for matching contacts.
        school_info = {
            "state":  state,
            "domain": compute_school_domain([{"email": sc["email"]} for sc in site_contacts]),
        }
        if school_info["domain"]:
            print(f"  School domain: {school_info['domain']}")
        for c in contacts:
            if c.get(C_SCHOOL, "").strip() != school_name:
                continue
            sync_flag  = str(c.get(C_SYNC, "N")).strip().upper()
            email      = str(c.get(C_EMAIL, "")).strip()
            first      = str(c.get(C_FIRST, "")).strip()
            last       = str(c.get(C_LAST, "")).strip()
            role       = str(c.get(C_ROLE, "")).strip()
            contact_ns = str(c.get(C_NS_CID, "")).strip()
            if not email:
                continue
            c[C_NS_CUS] = ns_id
            departed = site_contacts and (email.lower() not in site_emails)

            if sync_flag == "Y" and not departed:
                if contact_ns == "UNLINKED":
                    continue
                new_id = sync_contact(ns_id, school_name, {
                    "first": first, "last": last,
                    "email": email, "role": role,
                    "ns_id": contact_ns if contact_ns not in ("", "nan", "None") else "",
                }, school_info)
                if new_id:
                    c[C_NS_CID] = str(new_id)
                    c[C_SYNCED] = datetime.now().strftime("%Y-%m-%d %H:%M")
                    contact_updates += 1
            elif departed and contact_ns not in ("", "nan", "None", "UNLINKED") and site_contacts:
                inactivate_contact(contact_ns, f"{first} {last}")
                c[C_SYNC] = "N"
                c[C_NS_CID] = ""
                contact_inactivates += 1
                print(f"  - Departed: {first} {last}")
            elif sync_flag == "N" and contact_ns not in ("", "nan", "None", "UNLINKED"):
                inactivate_contact(contact_ns, f"{first} {last}")
                c[C_NS_CID] = ""
                contact_inactivates += 1
            time.sleep(0.15)

        school_row[S_SYNCED] = datetime.now().strftime("%Y-%m-%d %H:%M")
        synced += 1
        time.sleep(DELAY_BETWEEN_SCHOOLS)

    print(f"\n{'='*60}\n  Saving to Google Sheets...")
    save_il_schools(il_ws, il_schools)
    save_contacts(contacts_ws, contacts)

    print(f"\n{'='*60}")
    print(f"  IL SYNC COMPLETE")
    print(f"  Schools synced:       {synced}")
    print(f"  Missing NS ID:        {skipped_no_ns_id}")
    print(f"  Contacts created:     {contact_creates}")
    print(f"  Contacts updated:     {contact_updates}")
    print(f"  Contacts inactivated: {contact_inactivates}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
