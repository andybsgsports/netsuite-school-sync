"""
ihsa_sync.py
------------
Daily Illinois sync: scrapes each IL school's IHSA detail page with Selenium,
adds discovered admins/coaches to the Contacts tab, and pushes Sync=Y contacts
to NetSuite. Mirrors school_netsuite_sync.py for the WI side.

Unlike the WI path, this does NOT touch the Customer record because IHSA
does not expose the richer WIAA-style fields (level, conference, enrollment,
etc.) and overwriting existing good data would be lossy.

Reads:
  - IL_Schools tab  (Schools | School Website | State | NS Customer ID | Sales Rep | ...)
  - Contacts tab    (shared with WI sync)

Env:
  GOOGLE_SHEET_ID, GOOGLE_CREDENTIALS_JSON, NS_*
"""

import json
import os
import re
import sys
import time
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from netsuite_sync import (
    sync_contact,
    inactivate_contact,
)
from ihsa_batch_runner import (
    load_domain_rules,
    load_exceptions,
    make_driver,
    extract_people,
    ihsa_url_from_id_or_url,
    split_first_space,
    infer_from_email,
    is_valid_email,
    clean_role,
    norm,
    BUILTIN_DOMAIN_RULES,
)

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# -- Config ------------------------------------------------------------------
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "")
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
DELAY_BETWEEN_SCHOOLS = 1.0

SCHOOL_FILTER = os.environ.get("SCHOOL_FILTER", "").strip()

# IL_Schools columns
S_NAME   = "Schools"
S_URL    = "School Website"
S_STATE  = "State"
S_NS_ID  = "NS Customer ID"
S_SALES  = "Sales Rep"
S_SYNCED = "Last Synced"
S_NOTES  = "Notes"

# Shared Contacts tab columns (same as WI)
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


# -- Google Sheets ------------------------------------------------------------
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
    il_schools = il_ws.get_all_records()
    contacts_ws = wb.worksheet("Contacts")
    contacts = contacts_ws.get_all_records()
    return il_schools, contacts, il_ws, contacts_ws


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


# -- IHSA scrape → contact rows ----------------------------------------------
def parse_role_type(job_title):
    """Classify an IHSA Job Title into (Role, Type). Type = Coach | Admin."""
    jt = norm(job_title)
    lower = jt.lower()
    if any(k in lower for k in ("athletic director", "athletic supervisor",
                                "principal", "superintendent", "activities director",
                                "dean of students", "athletic trainer")):
        return (jt.title(), "Admin")
    if "head coach" in lower or "coach" in lower:
        # Keep the job title as-is so it retains the sport (e.g. "Boys Basketball Head Coach")
        return (jt.title(), "Coach")
    if any(k in lower for k in ("advisor", "adviser", "director")):
        return (jt.title(), "Admin")
    return (jt.title(), "Admin")


def scrape_il_school(driver, url, school_name, state, domain_rules, exceptions):
    """Returns list of contact dicts matching Contacts-tab shape."""
    driver.get(ihsa_url_from_id_or_url(url))
    try:
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    except Exception:
        pass
    try:
        people = extract_people(driver)
    except Exception as exc:
        print(f"    ERROR extracting: {exc}")
        return []

    out = []
    for p in people:
        email = norm(p.get("Email", ""))
        if not is_valid_email(email):
            continue
        name = norm(p.get("Name", ""))
        role_raw = clean_role(p.get("Role", ""))
        fn, ln = split_first_space(name) if name else ("", "")
        if not (fn and ln):
            efn, eln = infer_from_email(email, domain_rules, exceptions)
            fn = fn or efn
            ln = ln or eln
        role, ctype = parse_role_type(role_raw)
        out.append({
            "first": fn.title() if fn else "",
            "last":  ln.title() if ln else "",
            "email": email,
            "role":  role,
            "type":  ctype,
        })
    return out


# -- Main --------------------------------------------------------------------
def main():
    print(f"{'='*60}")
    print(f"  IL Sync  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}\n")

    if not GOOGLE_SHEET_ID:
        print("ERROR: GOOGLE_SHEET_ID not set")
        sys.exit(1)

    gc = get_gspread_client()
    il_schools, contacts, il_ws, contacts_ws = load_sheet(gc)

    if SCHOOL_FILTER:
        schools_to_sync = [s for s in il_schools if s.get(S_NAME, "").strip() == SCHOOL_FILTER]
        print(f"  TEST MODE: Only '{SCHOOL_FILTER}'")
    else:
        schools_to_sync = il_schools

    print(f"  IL schools: {len(schools_to_sync)}  |  Existing contacts: {len(contacts)}\n")

    exceptions = load_exceptions  # it's a callable on path; see ihsa_batch_runner
    # The loader helpers expect Path inputs; we just use empty dicts in CI.
    from pathlib import Path
    exc_csv = Path("name_exceptions.csv")
    dom_csv = Path("domain_rules.csv")
    exceptions = load_exceptions(exc_csv) if exc_csv.exists() else {}
    domain_rules = load_domain_rules(dom_csv) if dom_csv.exists() else dict(BUILTIN_DOMAIN_RULES)

    driver = make_driver()
    synced = 0
    skipped_no_ns_id = 0
    contact_creates = 0
    contact_updates = 0
    contact_inactivates = 0

    try:
        for school_row in schools_to_sync:
            school_name = str(school_row.get(S_NAME, "")).strip()
            url         = str(school_row.get(S_URL, "")).strip()
            ns_id       = str(school_row.get(S_NS_ID, "")).strip()
            state       = str(school_row.get(S_STATE, "IL")).strip() or "IL"

            if not (school_name and url):
                continue

            print(f"\n{'-'*60}\n[IL] {school_name}")

            # Scrape site
            site_contacts = scrape_il_school(driver, url, school_name, state,
                                             domain_rules, exceptions)
            print(f"  Scraped: {len(site_contacts)} contacts")

            if ns_id in ("", "nan", "None", "0"):
                # No NetSuite customer linked — can't sync contacts; just
                # record the scraped contacts for manual linkage later.
                school_row[S_NOTES] = (school_row.get(S_NOTES, "") + " [needs NS Customer ID]").strip()
                skipped_no_ns_id += 1
                print(f"  SKIP — no NS Customer ID in sheet")
                # Still add discovered contacts so Andy can see them (Sync=N)
                existing_emails = {
                    (c.get(C_EMAIL, "").strip().lower(), c.get(C_ROLE, "").strip().lower())
                    for c in contacts
                    if c.get(C_SCHOOL, "").strip() == school_name
                }
                for sc in site_contacts:
                    key = (sc["email"].lower(), sc["role"].lower())
                    if key in existing_emails:
                        continue
                    contacts.append({
                        C_SCHOOL: school_name, C_FIRST: sc["first"], C_LAST: sc["last"],
                        C_EMAIL:  sc["email"], C_ROLE:  sc["role"], C_TYPE:  sc["type"],
                        C_SYNC:   "N", C_NS_CID: "", C_NS_CUS: "", C_SYNCED: "",
                    })
                    existing_emails.add(key)
                continue

            # Add new contacts (auto-sync = Y for IL, matching WI behavior)
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

            # Sync contacts + inactivate departed
            school_info = {"state": state}  # minimal; sync_contact needs only state
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
                email_lower = email.lower()
                departed = site_contacts and (email_lower not in site_emails)

                if sync_flag == "Y" and not departed:
                    if contact_ns == "UNLINKED":
                        continue
                    contact_row = {
                        "first": first, "last": last,
                        "email": email, "role": role,
                        "ns_id": contact_ns if contact_ns not in ("", "nan", "None") else "",
                    }
                    new_id = sync_contact(ns_id, school_name, contact_row, school_info)
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
                time.sleep(0.2)

            school_row[S_SYNCED] = datetime.now().strftime("%Y-%m-%d %H:%M")
            synced += 1
            time.sleep(DELAY_BETWEEN_SCHOOLS)
    finally:
        try:
            driver.quit()
        except Exception:
            pass

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
