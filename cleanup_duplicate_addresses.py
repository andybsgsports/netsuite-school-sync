"""
cleanup_duplicate_addresses.py
-------------------------------
Remove duplicate addressBook entries from NetSuite Customer records.

Uses PATCH /customer/{id}?replace=addressBook with the full list of lines
to keep. NetSuite deletes any addressBook lines not included in the body —
a true replace. Lines already labeled "(Duplicate) ..." or "(Removed) ..."
from earlier cleanup attempts are dropped entirely.

By default runs as dry-run — prints what it would remove without touching
anything. Pass --live to actually apply.

Usage:
  python cleanup_duplicate_addresses.py                   # dry-run, all schools in sheet
  python cleanup_duplicate_addresses.py --live            # apply, all schools
  python cleanup_duplicate_addresses.py 994               # dry-run, single customer
  python cleanup_duplicate_addresses.py 994 1029 --live   # apply, specific customers

Decision rules:
  - Lines with a label starting "(Duplicate)" or "(Removed)" → always removed
  - Lines whose label is shared with others (duplicates) → keep the oldest
    (lowest line_id), drop the rest
  - Everything else → kept as-is
"""

import argparse
import os
import sys
import time

from netsuite_sync import ns_get, ns_patch


CRUFT_PREFIXES = ("(duplicate)", "(removed)")


def fetch_address_line_ids(customer_id):
    """Return list of line IDs on the customer's addressBook (in NS order)."""
    r = ns_get(f"customer/{customer_id}?expand=addressBook")
    if r.status_code != 200:
        print(f"  [ERROR] fetch customer {customer_id}: {r.status_code} {r.text[:120]}")
        return []
    items = r.json().get("addressBook", {}).get("items", [])
    ids = []
    for item in items:
        href = item.get("links", [{}])[0].get("href", "")
        line_id = href.rstrip("/").split("/")[-1] if href else None
        if line_id:
            ids.append(line_id)
    return ids


def fetch_line_full(customer_id, line_id):
    """Fetch one addressBook line's full body, stripped to what a replace PATCH accepts."""
    r = ns_get(f"customer/{customer_id}/addressBook/{line_id}")
    if r.status_code != 200:
        return None
    data = r.json()
    data.pop("links", None)
    addr = data.get("addressBookAddress")
    if isinstance(addr, dict):
        addr.pop("links", None)
        country = addr.get("country")
        if isinstance(country, dict) and "id" in country:
            addr["country"] = {"id": country["id"]}
    return data


def classify_lines(customer_id, line_ids):
    """
    Fetch each line, return:
      keep_ids: set of line IDs to preserve
      remove_info: list of (line_id, label, reason) for reporting
      full_by_id: dict line_id -> full line body (for replace)
    """
    # First pull every line fully
    full_by_id = {}
    labels_by_id = {}
    for lid in line_ids:
        full = fetch_line_full(customer_id, lid)
        if full is None:
            continue
        full_by_id[lid] = full
        labels_by_id[lid] = (full.get("label") or "").strip()

    # Cruft (previously flagged duplicates / removed)
    cruft_ids = {
        lid for lid, lbl in labels_by_id.items()
        if lbl.lower().startswith(CRUFT_PREFIXES)
    }

    # Among remaining lines, group by lowercase label
    groups = {}
    for lid, lbl in labels_by_id.items():
        if lid in cruft_ids:
            continue
        key = lbl.lower()
        groups.setdefault(key, []).append(lid)

    keep_ids = set()
    dupe_remove_ids = set()
    for key, ids in groups.items():
        # Sort numerically by line id, keep the lowest (oldest)
        ids_sorted = sorted(ids, key=int)
        keep_ids.add(ids_sorted[0])
        for lid in ids_sorted[1:]:
            dupe_remove_ids.add(lid)

    remove_info = []
    for lid in sorted(cruft_ids, key=int):
        remove_info.append((lid, labels_by_id[lid], "cruft"))
    for lid in sorted(dupe_remove_ids, key=int):
        remove_info.append((lid, labels_by_id[lid], "duplicate"))

    return keep_ids, remove_info, full_by_id, labels_by_id


def cleanup_customer(customer_id, live=False):
    print(f"\n[CUSTOMER {customer_id}]")
    line_ids = fetch_address_line_ids(customer_id)
    if not line_ids:
        print(f"  No address lines found")
        return 0
    print(f"  Total address lines: {len(line_ids)}")

    keep_ids, remove_info, full_by_id, labels_by_id = classify_lines(customer_id, line_ids)

    if not remove_info:
        print(f"  No duplicates")
        return 0

    # Summarize by label
    print(f"  Keep {len(keep_ids)} line(s), remove {len(remove_info)}:")
    by_label = {}
    for lid, lbl, reason in remove_info:
        by_label.setdefault(lbl or "(blank)", []).append((lid, reason))
    for lbl in sorted(by_label.keys(), key=lambda s: s.lower()):
        rows = by_label[lbl]
        ids_str = ", ".join(lid for lid, _ in rows)
        reason_set = {r for _, r in rows}
        reason_str = "/".join(sorted(reason_set))
        print(f"    [{lbl}] remove {len(rows)} ({reason_str}): {ids_str}")

    if not live:
        print(f"  DRY RUN: would remove {len(remove_info)} line(s). Pass --live to apply.")
        return len(remove_info)

    # Build replace body from the kept lines' full data, in stable order
    keep_items = [full_by_id[lid] for lid in sorted(keep_ids, key=int) if lid in full_by_id]
    print(f"  PATCH ?replace=addressBook with {len(keep_items)} item(s)...")

    r = ns_patch(f"customer/{customer_id}?replace=addressBook",
                 {"addressBook": {"items": keep_items}})

    if r.status_code == 204:
        # Verify the result — make sure we didn't accidentally wipe more than intended
        time.sleep(1.0)
        after = fetch_address_line_ids(customer_id)
        if len(after) == len(keep_items):
            print(f"  OK: addressBook now has {len(after)} line(s) — "
                  f"removed {len(remove_info)} duplicate/cruft line(s)")
            return len(remove_info)
        else:
            print(f"  WARN: replace returned 204 but now has {len(after)} lines "
                  f"(expected {len(keep_items)}) — MANUAL REVIEW needed")
            return 0
    else:
        print(f"  FAIL: replace returned {r.status_code}: {r.text[:300]}")
        print(f"  No changes applied. Falling back is not automatic — "
              f"investigate the error above.")
        return 0


def load_customer_ids_from_sheet():
    """Read NS Customer IDs from the Schools tab of the Google Sheet."""
    try:
        import json
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        print("[WARN] gspread/google-auth not installed — pass customer IDs as args instead.")
        return []

    sheet_id = os.environ.get("GOOGLE_SHEET_ID", "")
    if not sheet_id:
        print("[WARN] GOOGLE_SHEET_ID not set — pass customer IDs as args instead.")
        return []

    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if creds_json:
        creds = Credentials.from_service_account_info(
            json.loads(creds_json), scopes=scopes)
    else:
        creds_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "credentials.json")
        creds = Credentials.from_service_account_file(creds_file, scopes=scopes)

    gc = gspread.authorize(creds)
    wb = gc.open_by_key(sheet_id)
    ws = wb.worksheet("Schools")
    rows = ws.get_all_records()
    ids = []
    for r in rows:
        cid = str(r.get("NS Customer ID", "")).strip()
        if cid and cid not in ("nan", "None", "0"):
            try:
                ids.append(int(cid))
            except ValueError:
                pass
    return ids


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("customer_ids", nargs="*", type=int,
                        help="NS Customer IDs. If omitted, reads from Google Sheet.")
    parser.add_argument("--live", action="store_true",
                        help="Actually apply the replace. Default is dry-run.")
    args = parser.parse_args()

    customer_ids = args.customer_ids or load_customer_ids_from_sheet()
    if not customer_ids:
        print("No customer IDs to process.")
        sys.exit(1)

    mode = "LIVE" if args.live else "DRY RUN"
    print(f"{'='*60}")
    print(f"  addressBook cleanup — {mode}")
    print(f"  Customers: {len(customer_ids)}")
    print(f"{'='*60}")

    total = 0
    for cid in customer_ids:
        total += cleanup_customer(cid, live=args.live)
        time.sleep(0.5)

    verb = "Removed" if args.live else "Would remove"
    print(f"\n{'='*60}")
    print(f"  {verb} {total} duplicate/cruft line(s) across {len(customer_ids)} customer(s)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
