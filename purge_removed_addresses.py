"""
purge_removed_addresses.py — one-time cleanup of legacy "(Removed) <name>"
Ship-To lines on NetSuite customers.

Departed contacts' Ship-To addresses used to be RELABELED "(Removed) <name>"
rather than deleted, because a customer-level PATCH can only ADD to
addressBook. Those lines still appear in the Ship To dropdown on every quote
and order, so the list grew without bound (and offered dead addresses as
pickable options). The nightly sync now deletes properly; this script clears
the backlog those runs left behind.

Deletes ONLY lines whose label starts with "(Removed)" — a marker this sync
wrote itself, so there's nothing else it can match.

Deletion path, per customer (same cascade as the nightly):
  1. REST DELETE on the sublist line
  2. RESTlet removeAddress with labelPrefix="(removed)"

DRY RUN BY DEFAULT. Set LIVE=1 to actually delete.

Env:
  LIVE=1                 apply changes (otherwise report only)
  SALES_REP_FILTER=name  limit to one rep's schools
  GOOGLE_SHEET_ID, GOOGLE_CREDENTIALS_JSON, NS_* (as usual)
"""
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from netsuite_sync import (
    ns_get, ns_delete, ns_restlet_remove_address, restlet_available,
)
from school_netsuite_sync import (
    get_gspread_client, GOOGLE_SHEET_ID, MASTER_TAB,
    M_NAME, M_NS_ID, M_SALES,
)

LIVE = os.environ.get("LIVE", "").strip() in ("1", "true", "True", "yes")
REP_FILTER = os.environ.get("SALES_REP_FILTER", "").strip()
REMOVED_RE = re.compile(r"^\(removed\)\s*", re.I)


def removed_lines(customer_id):
    """Return [(line_id, label), ...] for lines labeled '(Removed) ...'."""
    r = ns_get(f"customer/{customer_id}?expand=addressBook")
    if r.status_code != 200:
        return None  # couldn't read — distinguish from "none found"
    out = []
    for item in r.json().get("addressBook", {}).get("items", []):
        href = item.get("links", [{}])[0].get("href", "")
        line_id = href.rstrip("/").split("/")[-1] if href else None
        if not line_id:
            continue
        r2 = ns_get(f"customer/{customer_id}/addressBook/{line_id}")
        if r2.status_code != 200:
            continue
        lbl = (r2.json().get("label") or "").strip()
        if REMOVED_RE.match(lbl):
            out.append((line_id, lbl))
    return out


def purge(customer_id, lines):
    """Delete the given lines. Returns the number actually removed."""
    deleted = 0
    rest_ok = True
    for line_id, _lbl in lines:
        if not rest_ok:
            break
        d = ns_delete(f"customer/{customer_id}/addressBook/{line_id}")
        if d.status_code in (200, 204):
            deleted += 1
        else:
            rest_ok = False
    if deleted == len(lines):
        return deleted
    removed = ns_restlet_remove_address(customer_id, label_prefix="(removed)")
    return removed if removed is not None else deleted


def main():
    print("=" * 60)
    print(f"  PURGE '(Removed)' Ship-To lines  |  LIVE={LIVE}")
    if REP_FILTER:
        print(f"  SALES_REP_FILTER: {REP_FILTER}")
    print(f"  RESTlet available: {restlet_available()}")
    print("=" * 60)

    gc = get_gspread_client()
    wb = gc.open_by_key(GOOGLE_SHEET_ID)
    schools = wb.worksheet(MASTER_TAB).get_all_records()

    targets = []
    for s in schools:
        name = str(s.get(M_NAME, "")).strip()
        ns_id = str(s.get(M_NS_ID, "")).strip()
        rep = str(s.get(M_SALES, "")).strip()
        if not name or not ns_id:
            continue
        try:
            ns_id = str(int(float(ns_id)))   # sheet may hand back 1491.0
        except ValueError:
            continue
        if REP_FILTER and rep != REP_FILTER:
            continue
        targets.append((name, ns_id))

    print(f"\nSchools in scope: {len(targets)}\n")

    total_found = total_removed = schools_hit = unreadable = 0
    for i, (name, ns_id) in enumerate(targets, 1):
        lines = removed_lines(ns_id)
        if lines is None:
            print(f"[{i}/{len(targets)}] {name} (NS {ns_id}) — WARN: could not read addressBook")
            unreadable += 1
            continue
        if not lines:
            continue
        schools_hit += 1
        total_found += len(lines)
        print(f"[{i}/{len(targets)}] {name} (NS {ns_id}) — {len(lines)} '(Removed)' line(s)")
        for _lid, lbl in lines[:10]:
            print(f"      {lbl}")
        if len(lines) > 10:
            print(f"      ... and {len(lines) - 10} more")
        if LIVE:
            got = purge(ns_id, lines)
            total_removed += got
            print(f"      -> deleted {got}/{len(lines)}")
            time.sleep(0.2)

    print("\n" + "=" * 60)
    print(f"  Schools with '(Removed)' lines: {schools_hit}")
    print(f"  Total '(Removed)' lines found:  {total_found}")
    if LIVE:
        print(f"  Total deleted:                  {total_removed}")
    else:
        print("  DRY RUN — nothing deleted. Set LIVE=1 to apply.")
    if unreadable:
        print(f"  Customers whose addressBook could not be read: {unreadable}")
    print("=" * 60)


if __name__ == "__main__":
    main()
