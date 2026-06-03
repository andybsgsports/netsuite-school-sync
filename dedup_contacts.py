"""
dedup_contacts.py — one-time cleanup that removes exact-duplicate
rows from the Contacts tab.

A row is considered a duplicate of an earlier row when its
(School Name, Email, Role) tuple matches case-insensitively. The
first occurrence wins (it usually carries the more-populated
NS Contact ID / Last Synced / Content Hash columns); later
duplicates are dropped.

Used after fix_contact_school_names.py collapses shorthand variants
('Antioch' -> 'Antioch Community High School'), which can leave two
rows for the same person under what is now the same canonical key.
"""
from __future__ import annotations

import os
import sys

import gspread

from school_netsuite_sync import (
    get_gspread_client,
    GOOGLE_SHEET_ID,
    CONTACTS_COLUMNS,
    C_SCHOOL, C_EMAIL, C_ROLE,
)

# When merging two duplicate rows, prefer non-blank values from either
# side so we don't lose NS Contact IDs or hashes that were on the loser.
MERGEABLE_COLS = [
    "NS Contact ID", "NS Customer ID", "Last Synced", "Content Hash",
]


def main():
    if not GOOGLE_SHEET_ID:
        print("ERROR: GOOGLE_SHEET_ID env var not set.")
        sys.exit(1)

    gc = get_gspread_client()
    wb = gc.open_by_key(GOOGLE_SHEET_ID)
    ws = wb.worksheet("Contacts")

    values = ws.get_all_values()
    if not values:
        print("Contacts tab is empty.")
        return
    headers = values[0]
    if C_SCHOOL not in headers or C_EMAIL not in headers or C_ROLE not in headers:
        print(f"ERROR: Contacts tab missing one of '{C_SCHOOL}' / '{C_EMAIL}' / '{C_ROLE}'.")
        sys.exit(1)

    school_idx = headers.index(C_SCHOOL)
    email_idx  = headers.index(C_EMAIL)
    role_idx   = headers.index(C_ROLE)

    seen = {}                  # key -> row index in `kept`
    kept = []                  # list of row dicts (one per unique key)
    dups = 0

    for raw in values[1:]:
        row = {h: (raw[i] if i < len(raw) else "") for i, h in enumerate(headers)}
        school = str(row.get(C_SCHOOL, "")).strip()
        email  = str(row.get(C_EMAIL,  "")).strip().lower()
        role   = str(row.get(C_ROLE,   "")).strip().lower()
        # Blank-email rows can't be deduped reliably; keep them as-is.
        if not email:
            kept.append(row)
            continue
        key = (school, email, role)
        if key not in seen:
            seen[key] = len(kept)
            kept.append(row)
            continue
        # Duplicate — merge any non-blank metadata into the winner and drop this row.
        winner = kept[seen[key]]
        for col in MERGEABLE_COLS:
            if not str(winner.get(col, "")).strip() and str(row.get(col, "")).strip():
                winner[col] = row[col]
        dups += 1

    print(f"Contacts tab: {len(values) - 1} rows in, {len(kept)} unique, {dups} duplicates dropped.")
    if dups == 0:
        print("Nothing to write.")
        return

    # Rebuild the sheet from the deduplicated rows. Use full header order
    # from the existing sheet so we don't lose any column.
    new_values = [headers] + [
        [str(row.get(h, "")) for h in headers]
        for row in kept
    ]
    # ws.update() with one big values array replaces the entire used range.
    # We clear first so a smaller post-dedup sheet doesn't leave trailing
    # stale rows underneath.
    ws.clear()
    ws.update(range_name="A1", values=new_values, value_input_option="RAW")
    print(f"Wrote {len(kept)} rows back to the Contacts tab.")


if __name__ == "__main__":
    main()
