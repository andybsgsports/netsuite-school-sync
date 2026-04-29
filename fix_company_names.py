"""
fix_company_names.py — fast targeted fix for the NetSuite Customer
companyName field on every school.

Reads the Schools tab from the master sheet and, for every row that has
both an NS Customer ID and a Full Name, PATCHes ONLY the companyName
field on that customer record. No scraping, no Ship-To, no contact
sync, no custom fields, no Sales Team. Just companyName.

Designed to be much faster than push_only.py — about 1 NS call per
school instead of ~30. ~5 minutes for 700 schools instead of 5 hours.

Used after a Full Name column edit on the sheet, or after a one-off
correction (e.g. "Oregon" -> "Oregon School District") to push the
display name to NetSuite without waiting for the full nightly.
"""
from __future__ import annotations

import os
import sys
import time

from netsuite_sync import ns_patch
from school_netsuite_sync import (
    get_gspread_client,
    GOOGLE_SHEET_ID, MASTER_TAB,
    M_NAME, M_NS_ID, M_LOCKED,
)

M_FULL = "Full Name"
SALES_REP_FILTER = os.environ.get("SALES_REP_FILTER", "").strip()
STATE_FILTER     = os.environ.get("STATE_FILTER", "").strip().upper()


def main():
    if not GOOGLE_SHEET_ID:
        print("ERROR: GOOGLE_SHEET_ID env var not set.")
        sys.exit(1)

    gc = get_gspread_client()
    wb = gc.open_by_key(GOOGLE_SHEET_ID)
    ws = wb.worksheet(MASTER_TAB)
    rows = ws.get_all_records()

    print(f"Schools tab rows: {len(rows)}")
    if SALES_REP_FILTER: print(f"  SALES_REP_FILTER: {SALES_REP_FILTER}")
    if STATE_FILTER:     print(f"  STATE_FILTER: {STATE_FILTER}")

    updated = 0
    skipped_no_full = 0
    skipped_no_ns   = 0
    skipped_filtered = 0
    locked = 0
    errors = 0

    for r in rows:
        name      = str(r.get(M_NAME, "")).strip()
        full_name = str(r.get(M_FULL, "")).strip()
        ns_id     = str(r.get(M_NS_ID, "")).strip()
        rep       = str(r.get("Sales Rep", "")).strip()
        state     = str(r.get("State", "")).strip().upper()
        is_locked = str(r.get(M_LOCKED, "")).strip().upper() == "Y"

        if not name:
            continue
        if is_locked:
            locked += 1
            continue
        if SALES_REP_FILTER and rep.lower() != SALES_REP_FILTER.lower():
            skipped_filtered += 1
            continue
        if STATE_FILTER and state != STATE_FILTER:
            skipped_filtered += 1
            continue
        if not full_name:
            skipped_no_full += 1
            continue
        if ns_id in ("", "nan", "None", "0"):
            skipped_no_ns += 1
            continue

        r2 = ns_patch(f"customer/{ns_id}", {"companyName": full_name})
        if r2.status_code in (200, 204):
            updated += 1
            print(f"  ✓ {ns_id}  {full_name}")
        else:
            errors += 1
            print(f"  ✗ {ns_id}  {full_name}  -> {r2.status_code} {r2.text[:120]}")
        time.sleep(0.1)

    print(f"\nUpdated:        {updated}")
    print(f"Errors:         {errors}")
    print(f"Skipped (no Full Name):     {skipped_no_full}")
    print(f"Skipped (no NS Customer):   {skipped_no_ns}")
    print(f"Skipped (locked):           {locked}")
    if SALES_REP_FILTER or STATE_FILTER:
        print(f"Skipped (filter mismatch):  {skipped_filtered}")


if __name__ == "__main__":
    main()
