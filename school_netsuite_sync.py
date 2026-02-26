"""
school_netsuite_sync.py
-----------------------
Daily sync: reads school list from Google Sheets, scrapes WIAA pages,
syncs Customer and Contact records to NetSuite, writes IDs back to Sheets.

Google Sheet (set GOOGLE_SHEET_ID env var):
  Tab "Schools":  School Name | Full Name | State | School URL | NS Ext ID |
                  Sales Rep | ... | NS Customer ID | Last Synced | Notes
  Tab "Contacts": School Name | First | Last | Email | Role | Type |
                  Sync | NS Contact ID | NS Customer ID | Last Synced
"""

import os
import sys
import json
import time
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from netsuite_sync import (
    scrape_wiaa_school_detail,
    sync_school,
    sync_contact,
    inactivate_contact,
    remove_contact_ship_to,
)

# -- Config -------------------------------------------------------------------
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "")
GOOGLE_SCOPES   = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
DELAY = 1.5  # seconds between schools

# Set to a school name to test a single school, or "" for all.
SCHOOL_FILTER = ""

# -- Column names -------------------------------------------------------------
S_NAME   = "School Name"
S_FULL   = "Full Name"
S_STATE  = "State"
S_URL    = "School URL"
S_NS_ID  = "NS Customer ID"
S_SALES  = "Sales Rep"
S_SYNCED = "Last Synced"

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

CONTACTS_COLUMNS = [C_SCHOOL, C_FIRST, C_LAST, C_EMAIL, C_ROLE, C_TYPE,
                    C_SYNC, C_NS_CID, C_NS_CUS, C_SYNCED]


# -- Google Sheets helpers ----------------------------------------------------
def get_gspread_client():
    """Authenticate with Google. Supports env var JSON (CI) or local file."""
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if creds_json:
        info = json.loads(creds_json)
        creds = Credentials.from_service_account_info(info, scopes=GOOGLE_SCOPES)
    else:
        creds_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "credentials.json")
        creds = Credentials.from_service_account_file(creds_file, scopes=GOOGLE_SCOPES)
    return gspread.authorize(creds)


def load_sheet(gc):
    """Load Schools and Contacts tabs from Google Sheets as list-of-dicts."""
    wb = gc.open_by_key(GOOGLE_SHEET_ID)

    # Schools tab
    schools_ws = wb.worksheet("Schools")
    schools_data = schools_ws.get_all_records()

    # Contacts tab (create if missing)
    try:
        contacts_ws = wb.worksheet("Contacts")
        contacts_data = contacts_ws.get_all_records()
    except gspread.exceptions.WorksheetNotFound:
        contacts_ws = wb.add_worksheet(title="Contacts", rows=1, cols=len(CONTACTS_COLUMNS))
        contacts_ws.append_row(CONTACTS_COLUMNS)
        contacts_data = []

    # Normalize legacy column names
    rename_map = {
        "First Name": "First", "Last Name": "Last",
        "Sync (Y/N)": "Sync", "Full School Name": "School Name",
    }
    for row in contacts_data:
        for old_key, new_key in rename_map.items():
            if old_key in row and new_key not in row:
                row[new_key] = row.pop(old_key)
            elif old_key in row and new_key in row:
                del row[old_key]

    return schools_data, contacts_data, schools_ws, contacts_ws


def save_schools_tab(ws, schools_data):
    """Overwrite the Schools tab with updated data."""
    if not schools_data:
        return
    headers = list(schools_data[0].keys())
    rows = [headers] + [[str(row.get(h, "") or "") for h in headers] for row in schools_data]
    ws.clear()
    ws.update(range_name="A1", values=rows)
    print(f"  [SHEETS] Schools tab saved ({len(schools_data)} rows)")


def save_contacts_tab(ws, contacts_data):
    """Overwrite the Contacts tab with updated data."""
    if not contacts_data:
        return
    headers = CONTACTS_COLUMNS
    rows = [headers]
    for row in contacts_data:
        rows.append([str(row.get(h, "") or "") for h in headers])
    ws.clear()
    ws.update(range_name="A1", values=rows)
    print(f"  [SHEETS] Contacts tab saved ({len(contacts_data)} rows)")


# -- Main sync ---------------------------------------------------------------
def main():
    print(f"{'='*60}")
    print(f"  School -> NetSuite Sync  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}\n")

    if not GOOGLE_SHEET_ID:
        print("ERROR: GOOGLE_SHEET_ID env var not set.")
        print("Set it to your Google Sheet ID, e.g.:")
        print("  export GOOGLE_SHEET_ID='1abc...'")
        sys.exit(1)

    gc = get_gspread_client()
    all_schools_data, contacts_data, schools_ws, contacts_ws = load_sheet(gc)

    if SCHOOL_FILTER:
        schools_to_sync = [s for s in all_schools_data
                           if s.get(S_NAME, "").strip() == SCHOOL_FILTER]
        print(f"  TEST MODE: Only syncing '{SCHOOL_FILTER}'")
    else:
        schools_to_sync = all_schools_data

    print(f"  Schools: {len(schools_to_sync)}/{len(all_schools_data)}  |  Contacts: {len(contacts_data)}\n")

    synced = 0
    skipped = 0
    errors = 0

    for school_row in schools_to_sync:
        school_name = str(school_row.get(S_NAME, "")).strip()
        full_name   = str(school_row.get(S_FULL, "")).strip()
        url         = str(school_row.get(S_URL, "")).strip()
        state       = str(school_row.get(S_STATE, "WI")).strip() or "WI"
        ns_id       = str(school_row.get(S_NS_ID, "")).strip()
        sales_rep   = str(school_row.get(S_SALES, "")).strip()

        if not url:
            skipped += 1
            continue

        if ns_id in ("", "nan", "None", "0"):
            ns_id = None

        display_name = full_name if full_name else school_name

        print(f"\n{'='*60}")
        print(f"[SCHOOL] {display_name}")

        # -- 1. Scrape WIAA page -----------------------------------------
        school_info, scraped_admins, scraped_coaches = scrape_wiaa_school_detail(url)
        all_site_contacts = scraped_admins + scraped_coaches
        print(f"  Scraped: {len(scraped_admins)} admins, {len(scraped_coaches)} coaches")

        # -- 2. Get existing contacts for this school --------------------
        school_contacts = [c for c in contacts_data
                           if c.get(C_SCHOOL, "").strip() == school_name]

        contacts_for_sync = [
            {
                "first": c.get(C_FIRST, ""),
                "last":  c.get(C_LAST, ""),
                "email": c.get(C_EMAIL, ""),
                "role":  c.get(C_ROLE, ""),
                "ns_id": c.get(C_NS_CID, ""),
                "sync":  True,
            }
            for c in school_contacts
            if str(c.get(C_SYNC, "N")).strip().upper() == "Y"
        ]

        # -- 3. Sync Customer to NetSuite --------------------------------
        # Pass empty contacts to sync_school so it only syncs the Customer
        # record (with address items). Contact creation is handled in step 6.
        try:
            result_id, school_info_out, all_found, created = sync_school(
                school_name=display_name,
                school_url=url,
                state=state,
                sync_contacts=[],
                sales_rep=sales_rep or None,
                ns_customer_id=ns_id or None,
            )
        except Exception as e:
            print(f"  ERROR syncing customer: {e}")
            errors += 1
            time.sleep(DELAY)
            continue

        if not result_id:
            print(f"  Could not sync Customer -- skipping contacts")
            errors += 1
            time.sleep(DELAY)
            continue

        if created:
            print(f"  NEW Customer created -- ID {result_id}")

        school_row[S_NS_ID]  = str(result_id)
        school_row[S_SYNCED] = datetime.now().strftime("%Y-%m-%d %H:%M")
        synced += 1

        # -- 4. Build set of emails currently on WIAA site ---------------
        site_emails = {
            p.get("email", "").strip().lower()
            for p in all_site_contacts
            if p.get("email", "").strip()
        }

        # -- 5. Add NEW contacts from WIAA (auto-sync = Y) --------------
        existing_emails = {
            c.get(C_EMAIL, "").strip().lower()
            for c in school_contacts
            if c.get(C_EMAIL, "").strip()
        }

        for person in all_site_contacts:
            em = person.get("email", "").strip().lower()
            if not em or em in existing_emails:
                continue
            new_row = {
                C_SCHOOL: school_name,
                C_FIRST:  person.get("first", ""),
                C_LAST:   person.get("last", ""),
                C_EMAIL:  person.get("email", ""),
                C_ROLE:   person.get("role", ""),
                C_TYPE:   person.get("type", ""),
                C_SYNC:   "Y",
                C_NS_CID: "",
                C_NS_CUS: str(result_id),
                C_SYNCED: "",
            }
            contacts_data.append(new_row)
            existing_emails.add(em)
            print(f"  + New: {person.get('first','')} {person.get('last','')} "
                  f"-- {person.get('role','')} [{person.get('type','')}]")

        # -- 6. Sync/inactivate contacts in NetSuite --------------------
        for c in contacts_data:
            if c.get(C_SCHOOL, "").strip() != school_name:
                continue

            sync_flag  = str(c.get(C_SYNC, "N")).strip().upper()
            first      = str(c.get(C_FIRST, "")).strip()
            last       = str(c.get(C_LAST, "")).strip()
            email      = str(c.get(C_EMAIL, "")).strip()
            role       = str(c.get(C_ROLE, "")).strip()
            contact_ns = str(c.get(C_NS_CID, "")).strip()

            if not email:
                continue

            c[C_NS_CUS] = str(result_id)
            email_lower = email.lower()
            departed = email_lower not in site_emails

            if sync_flag == "Y" and not departed:
                # Active contact still on site -- create or update
                contact_row = {
                    "first": first, "last": last,
                    "email": email, "role": role,
                    "ns_id": contact_ns if contact_ns not in ("", "nan", "None") else "",
                }
                new_id = sync_contact(result_id, display_name, contact_row, school_info_out)
                if new_id:
                    c[C_NS_CID] = str(new_id)
                    c[C_SYNCED] = datetime.now().strftime("%Y-%m-%d %H:%M")

            elif departed and contact_ns not in ("", "nan", "None") and all_site_contacts:
                # Contact gone from WIAA -- inactivate (only if scrape succeeded)
                inactivate_contact(contact_ns, f"{first} {last}")
                remove_contact_ship_to(result_id, f"{first} {last}")
                c[C_SYNC]   = "N"
                c[C_NS_CID] = ""
                print(f"  - Departed: {first} {last} -- inactivated")

            elif sync_flag == "N" and contact_ns not in ("", "nan", "None"):
                # Manually turned off -- inactivate
                inactivate_contact(contact_ns, f"{first} {last}")
                c[C_NS_CID] = ""

            time.sleep(0.2)

        time.sleep(DELAY)

    # -- 7. Save to Google Sheets ----------------------------------------
    print(f"\n{'='*60}")
    print(f"  Saving to Google Sheets...")
    save_schools_tab(schools_ws, all_schools_data)
    save_contacts_tab(contacts_ws, contacts_data)

    print(f"\n{'='*60}")
    print(f"  SYNC COMPLETE")
    print(f"  Synced: {synced}  |  Skipped: {skipped}  |  Errors: {errors}")
    print(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
