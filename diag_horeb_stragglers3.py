"""
diag_horeb_stragglers3.py — find ALL contacts named Tim Sarbacker / Brett Quale
across the entire NS system (any company).

After fix3 confirmed that SuiteQL finds 0 Tim/Brett at company 2217, yet the
rename still fails with "unique name already exists", the constraint must be
system-wide (NS enforces unique contact names globally, not per-customer).

This script:
  1. SuiteQL: SELECT all contacts named Tim Sarbacker or Brett Quale, any company
  2. Also queries for SarbackerFIX2217 / QualeFIX2217 to confirm temp names
  3. Reports all IDs, companies, emails, and active/inactive status

Env: NS_*
"""
from __future__ import annotations
from netsuite_sync import ns_suiteql

NAMES = [
    ("Tim",   "Sarbacker"),
    ("Brett", "Quale"),
    ("Tim",   "SarbackerFIX2217"),
    ("Brett", "QualeFIX2217"),
]


def main():
    print("Searching all contacts with target names (any company)…\n")

    for first, last in NAMES:
        query = (
            f"SELECT id, firstName, lastName, email, company, isInactive "
            f"FROM contact "
            f"WHERE firstName = '{first}' AND lastName = '{last}'"
        )
        rows = ns_suiteql(query)
        print(f"  '{first} {last}': {len(rows)} row(s)")
        for row in rows:
            print(f"    ID {row.get('id')}: company={row.get('company')} | "
                  f"email={row.get('email')} | inactive={row.get('isInactive')}")
        if not rows:
            print(f"    (none found)")
        print()

    print("Done.")


if __name__ == "__main__":
    main()
