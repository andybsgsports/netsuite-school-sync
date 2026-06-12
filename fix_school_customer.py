"""
fix_school_customer.py — repoint a school's NS Customer ID in the sheet.

Used when the Schools tab points a school at the wrong NetSuite customer
record (e.g. the district parent instead of the high-school subcustomer
that transactions are written against — Mount Horeb). Contacts' primary
Company follows the sheet's NS Customer ID, and the Loyalty Contact
dropdown on transactions only matches a contact's PRIMARY company, so
pointing at the wrong record hides every contact from the dropdown.

What it does (idempotent — safe to re-run):
  1. Verifies the target customer exists in NS and its companyName
     matches FIX_EXPECT_NAME (abort otherwise — nothing is changed).
  2. Schools tab: sets NS Customer ID to the new internal id, updates
     Full Name (so sync_customer doesn't rename the record), and records
     the old/parent record in an "NS Parent ID" column (push_only keeps
     the parent's Ship-To address book current too).
  3. Contacts tab: clears Content Hash for the school's rows so the next
     push re-PATCHes every contact, moving their primary Company to the
     new customer. Existing attachments to the parent are NOT removed by
     a PATCH, so contacts stay visible on the parent's Contacts tab.

Env:
  FIX_SCHOOL_NAME    - Schools tab "School Name" (exact match), required
  FIX_NEW_NS_ID      - new NS Customer internal id, required
  FIX_NEW_FULL_NAME  - new "Full Name" (optional; blank = leave as-is)
  FIX_PARENT_NS_ID   - parent customer internal id (optional)
  FIX_EXPECT_NAME    - substring the target's companyName must contain
  FIX_FORCE_REHASH   - "1" = clear hashes even if the NS ID didn't change
                       (re-run after partial contact failures)
  plus the usual GOOGLE_SHEET_ID / GOOGLE_CREDENTIALS_JSON / NS_* tokens
"""
from __future__ import annotations

import os
import sys

import gspread

from netsuite_sync import ns_get
from school_netsuite_sync import (
    get_gspread_client, GOOGLE_SHEET_ID, MASTER_TAB, CONTACTS_TAB,
    M_NAME, M_NS_ID, C_SCHOOL, C_HASH,
)

M_FULL   = "Full Name"
M_PARENT = "NS Parent ID"

SCHOOL  = os.environ.get("FIX_SCHOOL_NAME", "").strip()
NEW_ID  = os.environ.get("FIX_NEW_NS_ID", "").strip()
FULL    = os.environ.get("FIX_NEW_FULL_NAME", "").strip()
PARENT  = os.environ.get("FIX_PARENT_NS_ID", "").strip()
EXPECT  = os.environ.get("FIX_EXPECT_NAME", "").strip().lower()
FORCE   = os.environ.get("FIX_FORCE_REHASH", "").strip() in ("1", "true", "Y", "y")


def verify_customer(ns_id, label):
    r = ns_get(f"customer/{ns_id}")
    if r.status_code != 200:
        print(f"ABORT: GET customer/{ns_id} -> {r.status_code} {r.text[:160]}")
        sys.exit(1)
    data = r.json()
    name = data.get("companyName", "") or ""
    print(f"  [NS] {label}: customer {ns_id}  entityId={data.get('entityId')}  "
          f"companyName={name!r}  isInactive={data.get('isInactive')}")
    if EXPECT and EXPECT not in name.lower():
        print(f"ABORT: companyName does not contain {EXPECT!r} — wrong record?")
        sys.exit(1)
    return name


def main():
    if not (SCHOOL and NEW_ID.isdigit()):
        print("ERROR: FIX_SCHOOL_NAME and numeric FIX_NEW_NS_ID are required.")
        sys.exit(1)

    print("=" * 60)
    print(f"  FIX SCHOOL CUSTOMER ID")
    print(f"  School: {SCHOOL}")
    print(f"  New NS Customer ID: {NEW_ID}   Parent: {PARENT or '(none)'}")
    print("=" * 60)

    verify_customer(NEW_ID, "target (child)")
    if PARENT.isdigit():
        verify_customer(PARENT, "parent")

    gc = get_gspread_client()
    wb = gc.open_by_key(GOOGLE_SHEET_ID)

    # --- Schools tab -------------------------------------------------
    ws = wb.worksheet(MASTER_TAB)
    values = ws.get_all_values()
    headers = values[0]
    try:
        name_c = headers.index(M_NAME)
        nsid_c = headers.index(M_NS_ID)
    except ValueError as e:
        print(f"ERROR: missing Schools column: {e}")
        sys.exit(1)
    full_c = headers.index(M_FULL) if M_FULL in headers else None

    if M_PARENT in headers:
        par_c = headers.index(M_PARENT)
    else:
        ws.add_cols(1)
        par_c = len(headers)
        ws.update_cell(1, par_c + 1, M_PARENT)
        print(f"  [SHEETS] Added column '{M_PARENT}' to {MASTER_TAB}")

    row_i = next((i for i, r in enumerate(values[1:], start=2)
                  if len(r) > name_c and r[name_c].strip() == SCHOOL), None)
    if row_i is None:
        print(f"ERROR: school {SCHOOL!r} not found on {MASTER_TAB} tab.")
        sys.exit(1)

    old_id = values[row_i - 1][nsid_c].strip() if len(values[row_i - 1]) > nsid_c else ""
    id_changed = old_id != NEW_ID

    updates = []
    if id_changed:
        updates.append({"range": gspread.utils.rowcol_to_a1(row_i, nsid_c + 1),
                        "values": [[NEW_ID]]})
    if FULL and full_c is not None:
        updates.append({"range": gspread.utils.rowcol_to_a1(row_i, full_c + 1),
                        "values": [[FULL]]})
    if PARENT.isdigit():
        updates.append({"range": gspread.utils.rowcol_to_a1(row_i, par_c + 1),
                        "values": [[PARENT]]})
    if updates:
        ws.batch_update(updates)
    print(f"  [SHEETS] {SCHOOL}: NS Customer ID {old_id or '(blank)'} -> {NEW_ID}"
          f"{' (unchanged)' if not id_changed else ''}"
          f"{f' | Full Name -> {FULL}' if FULL and full_c is not None else ''}"
          f"{f' | {M_PARENT} -> {PARENT}' if PARENT.isdigit() else ''}")

    # --- Contacts tab: force re-push only when the target moved ------
    if not id_changed and not FORCE:
        print("  NS Customer ID already correct — hashes left alone.")
        return

    cws = wb.worksheet(CONTACTS_TAB)
    cvals = cws.get_all_values()
    cheads = cvals[0]
    try:
        sc = cheads.index(C_SCHOOL)
        hc = cheads.index(C_HASH)
    except ValueError as e:
        print(f"ERROR: missing Contacts column: {e}")
        sys.exit(1)

    clear = [{"range": gspread.utils.rowcol_to_a1(i, hc + 1), "values": [[""]]}
             for i, r in enumerate(cvals[1:], start=2)
             if len(r) > sc and r[sc].strip() == SCHOOL
             and len(r) > hc and r[hc].strip()]
    if clear:
        cws.batch_update(clear)
    print(f"  [SHEETS] Cleared Content Hash on {len(clear)} contact rows — "
          f"next push re-PATCHes them with company {NEW_ID}")


if __name__ == "__main__":
    main()
