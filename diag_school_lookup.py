"""
diag_school_lookup.py — read-only. For a school name substring, shows:
  - its row(s) on the Schools tab (Sync status, NS Customer ID, Sales Rep)
  - every Sync=Y Contacts-tab row at that school

Answers "why does this school have no/few contacts in NetSuite" by
checking whether the sync tracks it at all before looking at NetSuite
itself (see diag_duplicate_contacts.py / diag_salesrep_field.py for the
NetSuite-side half of that question).

Env: DIAG_SCHOOL_NAME (required, case-insensitive substring match)
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from school_netsuite_sync import (
    get_gspread_client, load_contacts, GOOGLE_SHEET_ID, MASTER_TAB,
    M_NAME, M_NS_ID, M_SALES, C_SCHOOL, C_FIRST, C_LAST, C_EMAIL,
    C_SYNC, C_NS_CID,
)

NAME = os.environ.get("DIAG_SCHOOL_NAME", "").strip().lower()


def main():
    print("=" * 70)
    print(f"  SCHOOL LOOKUP  |  name contains {NAME!r}  (read-only)")
    print("=" * 70)
    if not NAME:
        print("  ERROR: set DIAG_SCHOOL_NAME")
        return

    gc = get_gspread_client()
    schools = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(MASTER_TAB).get_all_records()
    matches = [s for s in schools if NAME in str(s.get(M_NAME, "")).strip().lower()]

    print(f"\nSchools tab: {len(matches)} matching row(s)")
    for s in matches:
        print(f"  {s.get(M_NAME)!r:45} NS ID={s.get(M_NS_ID)!r:10} "
              f"Sales Rep={s.get(M_SALES)!r}")

    contacts, _ws = load_contacts(gc)
    school_names = {str(s.get(M_NAME, "")).strip() for s in matches}
    rows = [c for c in contacts if str(c.get(C_SCHOOL, "")).strip() in school_names]
    y_rows = [c for c in rows if str(c.get(C_SYNC, "N")).strip().upper() == "Y"]

    print(f"\nContacts tab: {len(rows)} row(s) total, {len(y_rows)} with Sync=Y")
    for c in rows:
        print(f"  Sync={c.get(C_SYNC)!r:4} {c.get(C_FIRST)} {c.get(C_LAST):<20} "
              f"{c.get(C_EMAIL):<35} NS Contact ID={c.get(C_NS_CID)!r}")

    if not matches:
        print("\n  Not found on the Schools tab at all — the sync has never "
              "heard of this school (not in the WIAA/IHSA scrape roster, or "
              "the name doesn't match). It would need to be added manually "
              "or the scraper would need to pick it up.")
    elif not rows:
        print("\n  On the Schools tab but zero Contacts-tab rows — no coach/AD "
              "was ever scraped for it, so the sync has never had anyone to push.")


if __name__ == "__main__":
    main()
