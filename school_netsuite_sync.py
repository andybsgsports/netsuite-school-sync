"""
school_netsuite_sync.py
-----------------------
Daily WI sync. Reads the `Schools` tab (filtered to state == 'WI'),
scrapes each school's WIAA page, and syncs Customer + Contact records to
NetSuite.

The master tab is the single source of truth and is essentially read-only
from this script's perspective:
  - Reads: School Name, State, School URL, Sales Rep, NS Customer ID, Locked
  - Writes: only `Last Synced` on rows we actually processed
  - NEVER touches NS Customer ID, Sales Rep, School Name, Notes, or any other
    cell. Manual edits on the sheet are sticky.

Rows are skipped (not errored) when:
  - state != 'WI'
  - NS Customer ID is blank      -> use create_missing_ns_customers.py to link
  - Locked == 'Y'
  - School URL is blank

The Contacts tab is still populated with new scraped contacts and trimmed
as contacts depart.

Env vars:
  GOOGLE_SHEET_ID, GOOGLE_CREDENTIALS_JSON, NS_*
  SCHOOL_FILTER  -- optional, exact-match school name for single-row testing
"""

import json
import os
import sys
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
    sync_address_book,
    compute_school_domain,
    smart_title,
    restlet_available,
    ns_restlet_attach,
)

# -- Config -------------------------------------------------------------------
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "")
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
DELAY = 1.5
SCHOOL_FILTER = os.environ.get("SCHOOL_FILTER", "").strip()
SALES_REP_FILTER = os.environ.get("SALES_REP_FILTER", "").strip()
MASTER_TAB = "Schools"
CONTACTS_TAB = "Contacts"
STATE_FILTER = "WI"

# -- Schools tab columns -----------------------------------------------------
M_NAME   = "School Name"
M_STATE  = "State"
M_URL    = "School URL"
M_SALES  = "Sales Rep"
M_NS_ID  = "NS Customer ID"
M_LOCKED = "Locked"
M_SYNCED = "Last Synced"

# -- Contacts tab columns ----------------------------------------------------
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
C_HASH   = "Content Hash"  # sha1 of (first,last,email,role) — used by push_only to skip unchanged rows
CONTACTS_COLUMNS = [C_SCHOOL, C_FIRST, C_LAST, C_EMAIL, C_ROLE, C_TYPE,
                    C_SYNC, C_NS_CID, C_NS_CUS, C_SYNCED, C_HASH]


# -- Sheets helpers ----------------------------------------------------------
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


def load_master_wi(gc):
    """
    Returns (rows, worksheet, last_synced_col_1based).
    rows is a list of (sheet_row_1based, record_dict) for WI rows only.
    last_synced_col_1based is the column number to update Last Synced cells.
    """
    wb = gc.open_by_key(GOOGLE_SHEET_ID)
    ws = wb.worksheet(MASTER_TAB)
    values = ws.get_all_values()
    if not values:
        return [], ws, None
    headers = values[0]
    last_synced_col = headers.index(M_SYNCED) + 1 if M_SYNCED in headers else None
    ns_id_col       = headers.index(M_NS_ID) + 1 if M_NS_ID in headers else None
    out = []
    for i, raw in enumerate(values[1:], start=2):  # sheet rows are 1-indexed; row 1 is header
        rec = dict(zip(headers, raw))
        if str(rec.get(M_STATE, "")).strip().upper() != STATE_FILTER:
            continue
        out.append((i, rec))
    return out, ws, last_synced_col, ns_id_col


def load_contacts(gc):
    wb = gc.open_by_key(GOOGLE_SHEET_ID)
    try:
        ws = wb.worksheet(CONTACTS_TAB)
        rows = ws.get_all_records()
    except gspread.exceptions.WorksheetNotFound:
        ws = wb.add_worksheet(title=CONTACTS_TAB, rows=1, cols=len(CONTACTS_COLUMNS))
        ws.append_row(CONTACTS_COLUMNS)
        rows = []

    # Normalize legacy column names
    rename_map = {"First Name": "First", "Last Name": "Last",
                  "Sync (Y/N)": "Sync", "Full School Name": "School Name"}
    for row in rows:
        for old, new in rename_map.items():
            if old in row and new not in row:
                row[new] = row.pop(old)
            elif old in row and new in row:
                del row[old]

    rows = [r for r in rows if str(r.get(C_SCHOOL, "")).strip()]

    # Dedupe by (school, email, role)
    seen = set()
    dedup = []
    for r in rows:
        key = (str(r.get(C_SCHOOL, "")).strip().lower(),
               str(r.get(C_EMAIL, "")).strip().lower(),
               str(r.get(C_ROLE, "")).strip().lower())
        if key[1] and key in seen:
            continue
        seen.add(key)
        dedup.append(r)
    if len(dedup) < len(rows):
        print(f"  [SHEETS] Deduped {len(rows) - len(dedup)} duplicate contact rows")
    return dedup, ws


def save_contacts(ws, rows):
    headers = CONTACTS_COLUMNS
    clean = [r for r in rows if str(r.get(C_SCHOOL, "")).strip()]
    if len(clean) < len(rows):
        print(f"  [SHEETS] Removed {len(rows) - len(clean)} rows with empty School Name")
    # Sort: School Name (alphabetical), then Role/Sport, then Last name
    clean.sort(key=lambda r: (
        str(r.get(C_SCHOOL, "")).strip().lower(),
        str(r.get(C_ROLE, "")).strip().lower(),
        str(r.get(C_LAST, "")).strip().lower(),
        str(r.get(C_FIRST, "")).strip().lower(),
    ))
    vals = [headers] + [[str(r.get(h, "") or "") for h in headers] for r in clean]
    ws.clear()
    ws.update(range_name="A1", values=vals)
    print(f"  [SHEETS] Contacts tab saved ({len(clean)} rows, sorted by School + Role)")


# -- Main sync ---------------------------------------------------------------
def main():
    print("=" * 60)
    print(f"  WI School Sync  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    if not GOOGLE_SHEET_ID:
        print("ERROR: GOOGLE_SHEET_ID env var not set.")
        sys.exit(1)

    gc = get_gspread_client()
    rows, master_ws, last_synced_col, ns_id_col = load_master_wi(gc)
    contacts_data, contacts_ws = load_contacts(gc)

    if SCHOOL_FILTER:
        rows = [(i, r) for i, r in rows if str(r.get(M_NAME, "")).strip() == SCHOOL_FILTER]
        print(f"  TEST MODE: '{SCHOOL_FILTER}' ({len(rows)} matching row(s))")

    if SALES_REP_FILTER:
        rows = [(i, r) for i, r in rows
                if str(r.get(M_SALES, "")).strip().lower() == SALES_REP_FILTER.lower()]
        print(f"  REP FILTER: '{SALES_REP_FILTER}' ({len(rows)} school(s))")

    print(f"  WI rows: {len(rows)}  |  Contacts: {len(contacts_data)}\n")

    synced = 0
    skipped_no_ns = 0
    skipped_locked = 0
    skipped_no_url = 0
    errors = 0
    created_count = 0
    last_synced_updates = []  # (sheet_row_1based, timestamp_str)
    ns_id_updates = []        # (sheet_row_1based, new_ns_id) for created customers

    for sheet_row, school_row in rows:
        school_name = str(school_row.get(M_NAME, "")).strip()
        url         = str(school_row.get(M_URL, "")).strip()
        ns_id       = str(school_row.get(M_NS_ID, "")).strip()
        sales_rep   = str(school_row.get(M_SALES, "")).strip()
        locked      = str(school_row.get(M_LOCKED, "")).strip().upper() == "Y"

        if locked:
            print(f"  [SKIP locked] {school_name}")
            skipped_locked += 1
            continue
        if not url:
            print(f"  [SKIP no-url] {school_name}")
            skipped_no_url += 1
            continue
        # Blank / sentinel NS Customer ID means no NS customer exists yet.
        # Normalize to '' so sync_school CREATES the customer (and we write
        # the new ID back below) instead of skipping the row.
        if ns_id in ("", "nan", "None", "0"):
            ns_id = ""

        print(f"\n{'=' * 60}")
        print(f"[SCHOOL] {school_name}  (NS {ns_id or 'NEW'})")

        # 1. Scrape WIAA
        school_info, scraped_admins, scraped_coaches = scrape_wiaa_school_detail(url)
        all_site_contacts = scraped_admins + scraped_coaches
        print(f"  Scraped: {len(scraped_admins)} admins, {len(scraped_coaches)} coaches")

        # 2. Existing contacts for this school
        school_contacts = [c for c in contacts_data
                           if c.get(C_SCHOOL, "").strip() == school_name]

        # 3. Sync Customer (update only — never create, ns_id is always set here)
        try:
            result_id, school_info_out, _, created = sync_school(
                school_name=school_name,
                school_url=url,
                state=STATE_FILTER,
                sync_contacts=[],
                sales_rep=sales_rep or None,
                ns_customer_id=ns_id,
            )
        except Exception as e:
            print(f"  ERROR syncing customer: {e}")
            errors += 1
            time.sleep(DELAY)
            continue

        if not result_id:
            print(f"  Could not sync Customer — skipping contacts")
            errors += 1
            time.sleep(DELAY)
            continue

        synced += 1
        last_synced_updates.append((sheet_row, datetime.now().strftime("%Y-%m-%d %H:%M")))

        # New customer created this run — write the ID back so the next run
        # takes the direct-PATCH path instead of creating a duplicate.
        if created and not ns_id:
            ns_id = str(result_id)
            ns_id_updates.append((sheet_row, str(result_id)))
            created_count += 1
            print(f"  Created NS Customer ID {result_id} — writing back to sheet")

        # 4. Build site-emails set for departure detection
        site_emails = {p.get("email", "").strip().lower()
                       for p in all_site_contacts if p.get("email", "").strip()}

        # 5. Add new contacts (auto sync = Y)
        existing_keys = {
            (c.get(C_EMAIL, "").strip().lower(), c.get(C_ROLE, "").strip().lower())
            for c in school_contacts
            if c.get(C_EMAIL, "").strip()
        }
        for person in all_site_contacts:
            em = person.get("email", "").strip().lower()
            role_key = person.get("role", "").strip().lower()
            if not em or (em, role_key) in existing_keys:
                continue
            contacts_data.append({
                C_SCHOOL: school_name,
                C_FIRST:  smart_title(person.get("first", "")),
                C_LAST:   smart_title(person.get("last", "")),
                C_EMAIL:  person.get("email", ""),
                C_ROLE:   person.get("role", ""),
                C_TYPE:   person.get("type", ""),
                C_SYNC:   "Y",
                C_NS_CID: "",
                C_NS_CUS: str(result_id),
                C_SYNCED: "",
            })
            existing_keys.add((em, role_key))
            print(f"  + New: {person.get('first','')} {person.get('last','')} "
                  f"— {person.get('role','')} [{person.get('type','')}]")

        # 6. Compute school domain, sync/inactivate contacts
        _school_sync_y = [
            {"email": str(c.get(C_EMAIL, "")).strip()}
            for c in contacts_data
            if c.get(C_SCHOOL, "").strip() == school_name
            and str(c.get(C_SYNC, "N")).strip().upper() == "Y"
            and str(c.get(C_EMAIL, "")).strip()
        ]
        school_info_out["domain"] = compute_school_domain(_school_sync_y)
        if school_info_out["domain"]:
            print(f"  School domain: {school_info_out['domain']}")

        # Co-op (shared) people: still actively serving another school on the
        # Contacts tab. Their card is SHARED (attached via RESTlet), so a
        # departure from this school detaches rather than inactivates, and
        # updates avoid moving the card's primary company. See sync_contact.
        def _other_active_schools(em_lower, cid):
            others = set()
            for oc in contacts_data:
                if str(oc.get(C_SYNC, "N")).strip().upper() != "Y":
                    continue
                osch = str(oc.get(C_SCHOOL, "")).strip()
                if not osch or osch == school_name:
                    continue
                if em_lower and str(oc.get(C_EMAIL, "")).strip().lower() == em_lower:
                    others.add(osch)
                elif cid and str(oc.get(C_NS_CID, "")).strip() == cid:
                    others.add(osch)
            return others

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
            departed = email.lower() not in site_emails
            still_at = _other_active_schools(email.lower(), contact_ns)

            if sync_flag == "Y" and not departed:
                if contact_ns == "UNLINKED":
                    continue
                new_id = sync_contact(result_id, school_name, {
                    "first": first, "last": last,
                    "email": email, "role": role,
                    "ns_id": contact_ns if contact_ns not in ("", "nan", "None") else "",
                }, school_info_out, shared=bool(still_at))
                if new_id:
                    c[C_NS_CID] = str(new_id)
                    c[C_SYNCED] = datetime.now().strftime("%Y-%m-%d %H:%M")
                elif new_id is None and not contact_ns:
                    c[C_NS_CID] = "UNLINKED"
            elif departed and contact_ns not in ("", "nan", "None", "UNLINKED") and all_site_contacts:
                if still_at and restlet_available():
                    ns_restlet_attach(contact_ns, result_id, "detach")
                    remove_contact_ship_to(result_id, f"{first} {last}")
                    print(f"  - Departed (co-op): {first} {last} — detached from "
                          f"{school_name}; still at {sorted(still_at)[:3]}")
                else:
                    inactivate_contact(contact_ns, f"{first} {last}")
                    remove_contact_ship_to(result_id, f"{first} {last}")
                    print(f"  - Departed: {first} {last} — inactivated")
                c[C_SYNC]   = "N"
                c[C_NS_CID] = ""
            elif sync_flag == "N" and contact_ns not in ("", "nan", "None", "UNLINKED"):
                if still_at and restlet_available():
                    ns_restlet_attach(contact_ns, result_id, "detach")
                    remove_contact_ship_to(result_id, f"{first} {last}")
                else:
                    inactivate_contact(contact_ns, f"{first} {last}")
                c[C_NS_CID] = ""
            time.sleep(0.2)

        # 6b. Ship-To addresses
        active_contacts = [
            {
                "first": str(c.get(C_FIRST, "")).strip(),
                "last":  str(c.get(C_LAST, "")).strip(),
                "email": str(c.get(C_EMAIL, "")).strip(),
                "role":  str(c.get(C_ROLE, "")).strip(),
            }
            for c in contacts_data
            if c.get(C_SCHOOL, "").strip() == school_name
            and str(c.get(C_SYNC, "N")).strip().upper() == "Y"
        ]
        if active_contacts and school_info_out:
            sync_address_book(result_id, school_info_out, active_contacts,
                              school_name=school_name)

        time.sleep(DELAY)

    # -- Save only Last Synced back to master (never touches other columns) --
    if last_synced_col and last_synced_updates:
        print(f"\n  Writing Last Synced on {len(last_synced_updates)} row(s) of {MASTER_TAB}...")
        # Batch-update to reduce API calls
        batch = [{
            "range": gspread.utils.rowcol_to_a1(row, last_synced_col),
            "values": [[ts]],
        } for row, ts in last_synced_updates]
        master_ws.batch_update(batch)

    # -- Write newly-created NS Customer IDs back to master --
    if ns_id_col and ns_id_updates:
        print(f"  Writing NS Customer ID on {len(ns_id_updates)} newly-created row(s)...")
        batch = [{
            "range": gspread.utils.rowcol_to_a1(row, ns_id_col),
            "values": [[new_id]],
        } for row, new_id in ns_id_updates]
        master_ws.batch_update(batch)

    save_contacts(contacts_ws, contacts_data)

    print(f"\n{'=' * 60}")
    print(f"  WI SYNC COMPLETE")
    print(f"  Synced: {synced}")
    print(f"  Created (new NS customers): {created_count}")
    print(f"  Skipped (no NS ID):  {skipped_no_ns}")
    print(f"  Skipped (locked):    {skipped_locked}")
    print(f"  Skipped (no URL):    {skipped_no_url}")
    print(f"  Errors:              {errors}")
    print(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)


if __name__ == "__main__":
    main()
