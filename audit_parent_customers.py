"""
audit_parent_customers.py — discover parent/child customer structures in NS
for every school on the Schools tab (all reps) and wire them into the sync.

For each school with a numeric NS Customer ID:
  - GET the customer from NS.
  - If it HAS a parent (it's a subcustomer, like Mount Horeb High School):
    write the parent's internal id into the Schools tab "NS Parent ID"
    column. push_only.py then maintains Ship-To addresses on the parent
    too, while contacts/dropdown live on the child the sheet points at.
  - If it has NO parent but its companyName looks like a district-level
    record while children might exist (the pre-fix Mount Horeb pattern),
    try to find its subcustomers and REPORT them as repoint candidates —
    fixable per school via the "Fix School Customer ID" workflow. The
    children query needs REST search permission; if blocked we still
    report the suspicious name for manual review.

Read-only on NetSuite; writes only the "NS Parent ID" column on the sheet.

Env: GOOGLE_SHEET_ID / GOOGLE_CREDENTIALS_JSON / NS_* tokens
     AUDIT_REP_FILTER - optional, audit one rep's schools only
"""
from __future__ import annotations

import os
import time

import gspread

from netsuite_sync import ns_get
from school_netsuite_sync import (
    get_gspread_client, GOOGLE_SHEET_ID, MASTER_TAB,
    M_NAME, M_NS_ID, M_SALES,
)

M_PARENT = "NS Parent ID"
REP_FILTER = os.environ.get("AUDIT_REP_FILTER", "").strip()
DELAY = 0.15


def find_children(customer_id):
    """Subcustomers of customer_id, or None if REST search is blocked."""
    r = ns_get(f"customer?q=parent EQUAL {customer_id}&limit=10")
    if r.status_code != 200:
        return None
    return [(it.get("id"), it.get("companyName") or it.get("refName") or "")
            for it in r.json().get("items", [])]


def main():
    print("=" * 60)
    print("  PARENT/CHILD CUSTOMER AUDIT")
    if REP_FILTER:
        print(f"  AUDIT_REP_FILTER: {REP_FILTER}")
    print("=" * 60)

    gc = get_gspread_client()
    ws = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(MASTER_TAB)
    values = ws.get_all_values()
    headers = values[0]
    name_c = headers.index(M_NAME)
    nsid_c = headers.index(M_NS_ID)
    rep_c  = headers.index(M_SALES) if M_SALES in headers else None

    if M_PARENT in headers:
        par_c = headers.index(M_PARENT)
    else:
        ws.add_cols(1)
        par_c = len(headers)
        ws.update_cell(1, par_c + 1, M_PARENT)
        print(f"  [SHEETS] Added column '{M_PARENT}' to {MASTER_TAB}")

    children_linked = []   # schools whose customer is a subcustomer -> parent id written
    repoint = []           # schools pointing at a parent/district record
    search_blocked = False
    updates = []

    for i, row in enumerate(values[1:], start=2):
        name  = row[name_c].strip() if len(row) > name_c else ""
        ns_id = row[nsid_c].strip() if len(row) > nsid_c else ""
        rep   = row[rep_c].strip() if rep_c is not None and len(row) > rep_c else ""
        cur_parent = row[par_c].strip() if len(row) > par_c else ""
        if not name or not ns_id.isdigit():
            continue
        if REP_FILTER and rep.lower() != REP_FILTER.lower():
            continue

        r = ns_get(f"customer/{ns_id}")
        if r.status_code != 200:
            print(f"  [WARN] {name}: GET customer/{ns_id} -> {r.status_code}")
            time.sleep(DELAY)
            continue
        data = r.json()
        comp = data.get("companyName", "") or ""
        parent = (data.get("parent") or {}).get("id", "")

        if parent:
            if cur_parent != str(parent):
                updates.append({"range": gspread.utils.rowcol_to_a1(i, par_c + 1),
                                "values": [[str(parent)]]})
            children_linked.append((name, rep, ns_id, comp, parent))
        else:
            kids = find_children(ns_id)
            if kids is None:
                search_blocked = True
                kids = []
            if kids:
                repoint.append((name, rep, ns_id, comp, kids))
            elif any(t in comp.upper() for t in ("DISTRICT", "S.D.", "SCHOOL DIST")):
                # No children found (or query blocked) but the record looks
                # district-level — flag for manual review.
                repoint.append((name, rep, ns_id, comp, []))
        time.sleep(DELAY)

    if updates:
        ws.batch_update(updates)

    print(f"\n--- CHILD records (sheet OK; '{M_PARENT}' set so the parent's "
          f"Ship-To book is maintained): {len(children_linked)}")
    for name, rep, ns_id, comp, parent in children_linked:
        print(f"  {name} ({rep}) -> child {ns_id} {comp!r}, parent {parent}")

    print(f"\n--- REPOINT candidates (sheet points at a parent/district "
          f"record): {len(repoint)}")
    for name, rep, ns_id, comp, kids in repoint:
        print(f"  {name} ({rep}) -> {ns_id} {comp!r}")
        for kid_id, kid_name in kids:
            print(f"      child: {kid_id} {kid_name!r}")
        if not kids:
            print(f"      (children unknown{' — REST search blocked' if search_blocked else ''};"
                  f" check the record's Subcustomers in NS)")

    print(f"\n  Sheet updates written: {len(updates)}")
    print("  AUDIT COMPLETE")


if __name__ == "__main__":
    main()
