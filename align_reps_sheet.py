"""
align_reps_sheet.py — overwrite the school-name column on the legacy
"WI School List- Master" sheet so it matches the canonical "School
Name" column on the main master sheet's Schools tab.

Pivot key is the URL: main Schools tab "School URL" column vs the
legacy sheet's "School Website" column. URLs are stable across
renames, so we can map old shorthand rows to their canonical name.

If a legacy row's URL doesn't appear on the main Schools tab, the
row is left alone and reported in the summary.

Note: rep_digests.py no longer reads from this legacy sheet (since
PR #55) — it now reads the main Schools tab directly. This script
is just keeping the legacy sheet in sync for human/visual
consistency.

Env:
  GOOGLE_SHEET_ID            - main master sheet (Schools tab)
  GOOGLE_SHEET_ID_REPS       - legacy WI School List- Master
                               (default: 1SlZHbGRvPiO8Qtq7kY2aI0Y9oUsKZ2CxXNXcuw211N0)
  GOOGLE_CREDENTIALS_JSON
"""
from __future__ import annotations

import os
import sys

import gspread

from school_netsuite_sync import (
    get_gspread_client,
    GOOGLE_SHEET_ID, MASTER_TAB,
    M_NAME, M_URL,
)


LEGACY_SHEET_ID = os.environ.get(
    "GOOGLE_SHEET_ID_REPS",
    "1SlZHbGRvPiO8Qtq7kY2aI0Y9oUsKZ2CxXNXcuw211N0",
).strip()

# Column headers on the legacy sheet.
LEGACY_NAME_COL = "Schools"
LEGACY_URL_COL  = "School Website"


def normalize_url(u):
    """Strip whitespace and trailing slash so 'http://x.com/' matches
    'http://x.com'. Lower-case the scheme+host so HTTP/HTTPS quirks
    don't break matching."""
    s = str(u or "").strip().rstrip("/")
    return s.lower()


def main():
    if not GOOGLE_SHEET_ID:
        print("ERROR: GOOGLE_SHEET_ID env var not set.")
        sys.exit(1)
    if not LEGACY_SHEET_ID:
        print("ERROR: GOOGLE_SHEET_ID_REPS env var not set.")
        sys.exit(1)

    gc = get_gspread_client()

    # --- Build URL -> canonical name map from the main Schools tab ---
    main_wb = gc.open_by_key(GOOGLE_SHEET_ID)
    main_ws = main_wb.worksheet(MASTER_TAB)
    url_to_name = {}
    for r in main_ws.get_all_records():
        url  = normalize_url(r.get(M_URL, ""))
        name = str(r.get(M_NAME, "")).strip()
        if url and name:
            url_to_name[url] = name
    print(f"Main Schools tab: {len(url_to_name)} URL -> name mappings")

    # --- Walk the legacy sheet and stage updates ---
    legacy_wb = gc.open_by_key(LEGACY_SHEET_ID)
    legacy_ws = legacy_wb.sheet1
    values = legacy_ws.get_all_values()
    if not values:
        print("Legacy sheet is empty.")
        return
    headers = values[0]
    if LEGACY_NAME_COL not in headers or LEGACY_URL_COL not in headers:
        print(f"ERROR: legacy sheet missing '{LEGACY_NAME_COL}' or '{LEGACY_URL_COL}' column.")
        sys.exit(1)
    name_col_1 = headers.index(LEGACY_NAME_COL) + 1
    url_col_i  = headers.index(LEGACY_URL_COL)

    updates  = []
    changed  = 0
    same     = 0
    no_match = 0
    blank    = 0

    for row_idx, raw in enumerate(values[1:], start=2):
        cur_name = (raw[name_col_1 - 1] if len(raw) >= name_col_1 else "").strip()
        url      = normalize_url(raw[url_col_i] if len(raw) > url_col_i else "")
        if not url:
            blank += 1
            continue
        canonical = url_to_name.get(url)
        if not canonical:
            no_match += 1
            continue
        if canonical == cur_name:
            same += 1
            continue
        updates.append({
            "range": gspread.utils.rowcol_to_a1(row_idx, name_col_1),
            "values": [[canonical]],
        })
        changed += 1

    print(f"\nSummary:")
    print(f"  Rows to update:           {changed}")
    print(f"  Rows already matching:    {same}")
    print(f"  Rows with blank URL:      {blank}")
    print(f"  Rows with unknown URL:    {no_match}  (no match on main Schools tab)")

    if not updates:
        print("\nNothing to write.")
        return

    legacy_ws.batch_update(updates, value_input_option="RAW")
    print(f"\nWrote {len(updates)} cell update(s) to the legacy sheet.")


if __name__ == "__main__":
    main()
