"""
diag_salesrep_field.py — read-only. Confirms NetSuite's own Customer
"Sales Rep" field (the one a Contact-search filter can join to) actually
reflects what sync_customer() sets via the salesTeam sublist.

netsuite_sync.py sets a customer's PRIMARY salesTeam entry
(employee id from SALES_REP_MAP, isPrimary=True) but never writes the
plain `salesRep` field directly ("the salesRep field is ignored on this
form"). Many NetSuite accounts with "Use Multiple Sales Teams" enabled
keep `salesRep` automatically mirroring the primary salesTeam member —
this checks that's true here before building a saved-search filter on it
for Andy's "only my accounts" request (2026-09-17).

Env: DIAG_CUSTOMER_NAME (default "McHenry", substring match)
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from netsuite_sync import ns_suiteql

NAME = os.environ.get("DIAG_CUSTOMER_NAME", "McHenry").strip()


def main():
    print("=" * 70)
    print(f"  SALES REP FIELD DIAGNOSTIC  |  customer name contains {NAME!r}  (read-only)")
    print("=" * 70)
    rows = ns_suiteql(
        "SELECT id, companyname, salesrep FROM customer "
        f"WHERE companyname LIKE '%{NAME}%'", limit=20)
    if not rows:
        print("  no matching customers found")
        return
    for r in rows:
        print(f"  {r.get('id'):>8}  salesrep={r.get('salesrep')!r:>6}  {r.get('companyname')}")


if __name__ == "__main__":
    main()
