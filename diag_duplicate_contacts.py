"""
diag_duplicate_contacts.py — read-only. Explain why a customer's email
recipient dropdown shows the same person twice.

The Ship To / email-recipient pickers list the customer's CONTACTS. A name
appearing twice means NetSuite holds two links for that person — either two
separate contact records, or one record reachable by two routes (its own
`company` field plus an explicit contactRoles attach).

For the given customer this prints, per contact id:
  - name, email, isInactive, and the contact's own `company`
  - whether it came from contactRoles, from a company= SuiteQL match, or both
then groups by name so duplicates are obvious, and classifies each duplicate:

  SAME ID TWICE      -> one record listed twice (contactRoles has a dup line)
  TWO RECORDS        -> genuinely two contact records for one person
  COMPANY+ATTACHED   -> one record linked via company AND attached

Nothing is written. Env: DIAG_CUSTOMER_ID (default 2885 = Pecatonica).
"""
from __future__ import annotations

import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from netsuite_sync import ns_get, ns_suiteql

CUSTOMER = os.environ.get("DIAG_CUSTOMER_ID", "2885").strip()


def contact_role_ids(customer_id):
    """Contact ids on the customer's contactRoles sublist, WITH duplicates
    preserved — a repeated id is itself the bug we're looking for."""
    ids, offset = [], 0
    while True:
        r = ns_get(f"customer/{customer_id}/contactRoles?limit=100&offset={offset}")
        if r.status_code != 200:
            if offset == 0:
                r2 = ns_get(f"customer/{customer_id}?expand=contactRoles")
                if r2.status_code == 200:
                    for item in r2.json().get("contactRoles", {}).get("items", []):
                        cid = (item.get("contact") or {}).get("id")
                        if cid:
                            ids.append(str(cid))
            break
        body = r.json()
        items = body.get("items", [])
        for item in items:
            cid = (item.get("contact") or {}).get("id")
            if not cid:
                href = (item.get("links") or [{}])[0].get("href", "")
                line = href.rstrip("/").split("/")[-1] if href else ""
                if line:
                    r3 = ns_get(f"customer/{customer_id}/contactRoles/{line}")
                    if r3.status_code == 200:
                        cid = (r3.json().get("contact") or {}).get("id")
            if cid:
                ids.append(str(cid))
        if not body.get("hasMore"):
            break
        offset += 100
    return ids


def main():
    print("=" * 70)
    print(f"  DUPLICATE-CONTACT DIAGNOSTIC  |  customer {CUSTOMER}  (read-only)")
    print("=" * 70)

    r = ns_get(f"customer/{CUSTOMER}")
    if r.status_code == 200:
        b = r.json()
        print(f"  Customer: {b.get('companyName') or b.get('entityId')}")

    roles = contact_role_ids(CUSTOMER)
    print(f"\ncontactRoles entries: {len(roles)} "
          f"({len(set(roles))} distinct contact ids)")
    dup_lines = {c for c in roles if roles.count(c) > 1}
    if dup_lines:
        print(f"  !! ids appearing on MORE THAN ONE contactRoles line: "
              f"{sorted(dup_lines)}")

    by_company = {}
    for row in ns_suiteql(
            f"SELECT id, firstname, lastname, email, isinactive, company "
            f"FROM contact WHERE company = {CUSTOMER}", limit=1000):
        by_company[str(row.get("id"))] = row
    print(f"contacts whose company = this customer (SuiteQL): {len(by_company)}")

    all_ids = list(dict.fromkeys(list(by_company) + roles))
    print(f"union of both routes: {len(all_ids)} distinct contact ids\n")

    info = {}
    for cid in all_ids:
        row = by_company.get(cid)
        if row:
            first = (row.get("firstname") or "").strip()
            last = (row.get("lastname") or "").strip()
            email = (row.get("email") or "").strip()
            inact = str(row.get("isinactive") or "F").upper().startswith("T")
            comp = str(row.get("company") or "")
        else:
            rc = ns_get(f"contact/{cid}")
            if rc.status_code != 200:
                info[cid] = {"name": f"(unreadable id {cid})", "email": "",
                             "inactive": None, "company": "?"}
                continue
            b = rc.json()
            first = (b.get("firstName") or "").strip()
            last = (b.get("lastName") or "").strip()
            email = (b.get("email") or "").strip()
            inact = bool(b.get("isInactive"))
            comp = str((b.get("company") or {}).get("id") or "")
        info[cid] = {
            "name": f"{first} {last}".strip(),
            "email": email, "inactive": inact, "company": comp,
            "in_roles": cid in roles, "in_company": cid in by_company,
            "role_lines": roles.count(cid),
        }

    print("-" * 70)
    print("ALL CONTACTS LINKED TO THIS CUSTOMER")
    print("-" * 70)
    for cid, d in sorted(info.items(), key=lambda kv: kv[1]["name"].lower()):
        route = ("company+roles" if d.get("in_company") and d.get("in_roles")
                 else "company-only" if d.get("in_company")
                 else "roles-only")
        flag = "  [INACTIVE]" if d.get("inactive") else ""
        lines = f"  x{d['role_lines']} role lines" if d.get("role_lines", 0) > 1 else ""
        print(f"  {cid:>8}  {d['name']:<28} {d['email']:<36} "
              f"{route:<14}{flag}{lines}")

    groups = defaultdict(list)
    for cid, d in info.items():
        groups[d["name"].strip().lower()].append(cid)

    print("\n" + "-" * 70)
    print("DUPLICATE NAMES (what the dropdown shows twice)")
    print("-" * 70)
    dupes = {n: c for n, c in groups.items() if len(c) > 1}
    multi = {cid: d for cid, d in info.items() if d.get("role_lines", 0) > 1}
    if not dupes and not multi:
        print("  none found via contactRoles/company — if the dropdown still")
        print("  shows duplicates, they come from a route neither query sees.")
    for name, cids in sorted(dupes.items()):
        print(f"\n  '{info[cids[0]]['name']}' -> {len(cids)} contact records")
        for cid in cids:
            d = info[cid]
            print(f"     id={cid:<8} email={d['email']:<34} "
                  f"inactive={d['inactive']} company={d['company']} "
                  f"in_roles={d.get('in_roles')} in_company={d.get('in_company')}")
        print("     VERDICT: TWO RECORDS — one person, two contact records")
    for cid, d in multi.items():
        print(f"\n  '{d['name']}' id={cid} appears on {d['role_lines']} "
              f"contactRoles lines")
        print("     VERDICT: SAME ID TWICE — duplicate attach line")

    print("\n" + "=" * 70)
    print(f"  distinct contact ids: {len(info)}   duplicate names: {len(dupes)}"
          f"   ids on multiple role lines: {len(multi)}")
    print("=" * 70)


if __name__ == "__main__":
    main()
