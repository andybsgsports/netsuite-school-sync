"""
fix_contact_school_names.py — manual repair that aligns the Contacts tab's
School Name column with the canonical names on the Schools tab and merges
the duplicate rows a school rename leaves behind.

A school rename (on the Schools tab or on the WIAA site) used to strand a
school's Contacts-tab rows under the old name; the scraper would then
re-add everyone as fresh rows under the new name — with blank NS ids — so
the next push created duplicate contacts in NetSuite. The nightly jobs now
heal this automatically (canonicalize_contact_school_names runs inside
push_only.py and rep_digests.py); this script is the same heal on demand,
with a dry run for review.

Per stale row it:
  1. Resolves the current school — by the row's NS Customer ID when it has
     one, otherwise by a unique normalized-name match ('Waupaca' ->
     'Waupaca High School'), and rewrites School Name.
  2. Merges the row with any duplicate for the same (school, email, role),
     keeping the row that holds an NS Contact ID and carrying NS ids over.

Rows that can't be resolved are reported and left untouched.

DRY-RUN by default — prints the full plan. Set LIVE=1 (or --live) to apply.
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from school_netsuite_sync import (
    get_gspread_client, load_contacts, save_contacts,
    canonicalize_contact_school_names,
    GOOGLE_SHEET_ID, MASTER_TAB,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true",
                        help="Apply changes. Default is dry-run.")
    args = parser.parse_args()
    live = args.live or os.environ.get("LIVE", "") == "1"

    print("=" * 66)
    print(f"  Fix Contacts-tab school names — {'LIVE' if live else 'DRY RUN'}")
    print("=" * 66)

    if not GOOGLE_SHEET_ID:
        print("ERROR: GOOGLE_SHEET_ID env var not set.")
        sys.exit(1)

    gc = get_gspread_client()
    schools_records = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(
        MASTER_TAB).get_all_records()
    contacts_data, contacts_ws = load_contacts(gc)
    print(f"  Schools tab rows: {len(schools_records)}  |  "
          f"Contacts tab rows: {len(contacts_data)}\n")

    renamed, merged, unresolved = canonicalize_contact_school_names(
        contacts_data, schools_records)

    print("=" * 66)
    print(f"  {'APPLIED' if live else 'DRY RUN (nothing changed)'}")
    print(f"  Rows renamed to canonical school:  {renamed}")
    print(f"  Duplicate rows merged away:        {merged}")
    print(f"  Rows left unresolved:              {unresolved}")
    print("=" * 66)

    if live and (renamed or merged):
        save_contacts(contacts_ws, contacts_data)
    elif live:
        print("Nothing to write.")


if __name__ == "__main__":
    main()
