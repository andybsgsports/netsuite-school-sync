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

    # Per-current-name detection: if a single NS Customer ID maps to
    # multiple distinct School Names already on the Contacts tab (e.g.
    # West Bend East AND West Bend West share NS 3701), overwriting
    # would merge them into one name. Skip those rows unless the env
    # var ALLOW_NAME_MERGE=1 is set, and surface the collisions.
    from collections import defaultdict
    by_ns_current = defaultdict(set)
    for raw in values[1:]:
        ns = (raw[ns_col_idx] if len(raw) > ns_col_idx else "").strip()
        nm = (raw[name_col_1 - 1] if len(raw) >= name_col_1 else "").strip()
        if ns and nm:
            by_ns_current[ns].add(nm)
    collisions = {ns: sorted(names) for ns, names in by_ns_current.items()
                  if len(names) > 1}
    if collisions:
        print(f"\n  COLLISION: {len(collisions)} NS Customer ID(s) on the Contacts "
              f"tab map to MULTIPLE current School Names. First 10:")
        for ns, names in list(collisions.items())[:10]:
            print(f"    NS {ns}  Schools tab says '{ns_to_name.get(ns, '?')}', "
                  f"contacts have: {names}")

    allow_merge = os.environ.get("ALLOW_NAME_MERGE", "").strip().lower() in ("1", "true", "yes", "y")
    updates = []
    changed = 0
    unchanged = 0
    orphan = 0
    blank_ns = 0
    skipped_collision = 0

    for row_idx, raw in enumerate(values[1:], start=2):
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
        if ns_id in collisions and not allow_merge:
            skipped_collision += 1
            continue
        updates.append({
            "range": gspread.utils.rowcol_to_a1(row_idx, name_col_1),
            "values": [[canonical]],
        })
        changed += 1

    print(f"\nSummary:")
    print(f"  Rows to update:                       {changed}")
    print(f"  Rows already correct:                 {unchanged}")
    print(f"  Rows with blank NS ID:                {blank_ns}")
    print(f"  Rows with unknown NS ID:              {orphan}  (no Schools-tab match)")
    print(f"  Rows skipped (NS-ID name collision):  {skipped_collision}"
          f"{'  (set ALLOW_NAME_MERGE=1 to overwrite)' if skipped_collision else ''}")

    if not updates:
        print("\nNothing to write. Done.")
        return

    # Sheets API quota is 60 write requests per minute per user. The
    # previous 100-cells-per-call loop fired ~92 requests in 30s and
    # hit 429. gspread.batch_update accepts unlimited ranges in ONE
    # API call (bounded only by ~10 MB body) so send everything at once.
    contacts_ws.batch_update(updates, value_input_option="RAW")
    print(f"\nWrote {len(updates)} cell update(s) in one batch.")


if __name__ == "__main__":
    main()
