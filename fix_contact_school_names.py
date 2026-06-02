"""
fix_contact_school_names.py — one-time cleanup that aligns the
School Name column on the Contacts tab with the canonical School Name
column on the Schools tab.

Pivots on NS Customer ID — that's the stable column shared between
the two tabs. For each Contacts-tab row whose NS Customer ID matches
a Schools-tab row, the School Name cell is overwritten with whatever
the Schools tab currently shows.

Rows whose NS Customer ID doesn't match any Schools-tab row are left
alone (and a count is printed at the end so Andy can spot orphans).
"""
from __future__ import annotations

import os
import sys

import gspread

from school_netsuite_sync import (
    get_gspread_client,
    GOOGLE_SHEET_ID, MASTER_TAB,
    M_NAME, M_NS_ID,
    C_SCHOOL, C_NS_CUS,
)


def main():
    if not GOOGLE_SHEET_ID:
        print("ERROR: GOOGLE_SHEET_ID env var not set.")
        sys.exit(1)

    gc = get_gspread_client()
    wb = gc.open_by_key(GOOGLE_SHEET_ID)

    # --- Build NS Customer ID -> canonical School Name map from Schools tab ---
    schools_ws = wb.worksheet(MASTER_TAB)
    schools_rows = schools_ws.get_all_records()
    ns_to_name = {}
    duplicate_ids = set()
    for r in schools_rows:
        ns_id = str(r.get(M_NS_ID, "")).strip()
        name  = str(r.get(M_NAME, "")).strip()
        if ns_id in ("", "nan", "None", "0") or not name:
            continue
        if ns_id in ns_to_name and ns_to_name[ns_id] != name:
            duplicate_ids.add(ns_id)
        ns_to_name[ns_id] = name
    print(f"Schools tab: {len(ns_to_name)} distinct NS Customer IDs mapped")
    if duplicate_ids:
        print(f"  WARN: {len(duplicate_ids)} NS IDs appear more than once with different names: {sorted(duplicate_ids)}")

    # --- Walk Contacts tab and stage school-name corrections ---
    contacts_ws = wb.worksheet("Contacts")
    values = contacts_ws.get_all_values()
    if not values:
        print("Contacts tab is empty.")
        return
    headers = values[0]
    if C_SCHOOL not in headers or C_NS_CUS not in headers:
        print(f"ERROR: Contacts tab missing '{C_SCHOOL}' or '{C_NS_CUS}' column.")
        sys.exit(1)
    name_col_1 = headers.index(C_SCHOOL) + 1   # gspread cells are 1-indexed
    ns_col_idx = headers.index(C_NS_CUS)

    updates = []          # list[gspread.Cell-style dict] for batch_update
    changed = 0
    unchanged = 0
    orphan = 0
    blank_ns = 0

    for row_idx, raw in enumerate(values[1:], start=2):  # row 1 = header
        cur_name = (raw[name_col_1 - 1] if len(raw) >= name_col_1 else "").strip()
        ns_id    = (raw[ns_col_idx]      if len(raw) > ns_col_idx else "").strip()
        if ns_id in ("", "nan", "None", "0"):
            blank_ns += 1
            continue
        canonical = ns_to_name.get(ns_id)
        if not canonical:
            orphan += 1
            continue
        if canonical == cur_name:
            unchanged += 1
            continue
        updates.append({
            "range": gspread.utils.rowcol_to_a1(row_idx, name_col_1),
            "values": [[canonical]],
        })
        changed += 1
        if changed <= 25 or changed % 100 == 0:
            print(f"  row {row_idx}: '{cur_name}' -> '{canonical}'")

    print(f"\nSummary:")
    print(f"  Rows to update:           {changed}")
    print(f"  Rows already correct:     {unchanged}")
    print(f"  Rows with blank NS ID:    {blank_ns}")
    print(f"  Rows with unknown NS ID:  {orphan}  (no match on Schools tab)")

    if not updates:
        print("\nNothing to write. Done.")
        return

    # gspread batch_update accepts up to 100 ranges per call; chunk to be safe
    CHUNK = 100
    for i in range(0, len(updates), CHUNK):
        contacts_ws.batch_update(updates[i:i + CHUNK])
    print(f"\nWrote {len(updates)} cell update(s) to the Contacts tab.")


if __name__ == "__main__":
    main()
