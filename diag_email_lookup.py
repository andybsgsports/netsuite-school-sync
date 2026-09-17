"""
diag_email_lookup.py — read-only. Looks up every ACTIVE contact record
with a given email via SuiteQL, printing id, name, company, isInactive.

Distinguishes two very different things that can both make a name show
twice in a search result: (a) one shared contact record attached to two
companies (expected/correct for a co-op coach — one id, listed once per
company join) vs. (b) two genuinely separate contact records that
happen to share an email (the duplicate-contact bug from 2026-09-17,
should be zero after the September cleanup).

Env: DIAG_EMAIL (required)
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from netsuite_sync import ns_suiteql

EMAIL = os.environ.get("DIAG_EMAIL", "").strip()


def main():
    print("=" * 70)
    print(f"  EMAIL LOOKUP  |  {EMAIL!r}  (read-only)")
    print("=" * 70)
    if not EMAIL:
        print("  ERROR: set DIAG_EMAIL")
        return
    rows = ns_suiteql(
        "SELECT c.id, c.firstname, c.lastname, c.company, c.isinactive, "
        "cust.companyname FROM contact c "
        "LEFT JOIN customer cust ON cust.id = c.company "
        f"WHERE c.email = '{EMAIL}'", limit=50)
    if not rows:
        print("  no contact records found with this email")
        return
    for r in rows:
        print(f"  id={r.get('id'):<8} {r.get('firstname')} {r.get('lastname'):<20} "
              f"company={r.get('company')} ({r.get('companyname')})  "
              f"isinactive={r.get('isinactive')}")
    print(f"\n  {len(rows)} contact id(s) total for this email")
    print(f"  distinct ids: {len(set(r.get('id') for r in rows))}")


if __name__ == "__main__":
    main()
