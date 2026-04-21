"""
cleanup_duplicate_addresses.py
-------------------------------
Remove duplicate addressBook entries from NetSuite Customer records.

Each line in a Customer's addressBook has a "label". The daily sync uses
the contact's full name as the label, so duplicate contacts show up as
multiple lines with the same label. This script keeps the oldest line
(lowest line_id) per label and removes the rest.

By default runs as dry-run — shows what it would remove without touching
anything. Pass --live to actually remove.

Usage:
  python cleanup_duplicate_addresses.py                   # dry-run, all schools in sheet
  python cleanup_duplicate_addresses.py --live            # remove, all schools
  python cleanup_duplicate_addresses.py 1669              # dry-run, single customer
  python cleanup_duplicate_addresses.py 1669 1670 --live  # remove, specific customers

Removal strategy:
  1. Try HTTP DELETE on the address line. This is the clean path if the
     NetSuite role has permission.
  2. If DELETE fails, fall back to PATCHing the line to relabel it
     "(Duplicate) <original>" and clear default flags, so the sync code
     won't match it on future runs.
"""

import argparse
import os
import sys
import time

from netsuite_sync import ns_get, ns_patch, ns_delete


def fetch_address_lines(customer_id):
    """Return [{line_id, label, defaultShipping, defaultBilling}, ...] for a customer."""
    r = ns_get(f"customer/{customer_id}?expand=addressBook")
    if r.status_code != 200:
        print(f"  [ERROR] fetch customer {customer_id}: {r.status_code} {r.text[:120]}")
        return []

    items = r.json().get("addressBook", {}).get("items", [])
    lines = []
    for item in items:
        href = item.get("links", [{}])[0].get("href", "")
        line_id = href.rstrip("/").split("/")[-1] if href else None
        if not line_id:
            continue
        r2 = ns_get(f"customer/{customer_id}/addressBook/{line_id}")
        if r2.status_code != 200:
            continue
        data = r2.json()
        lines.append({
            "line_id":         line_id,
            "label":           (data.get("label") or "").strip(),
            "defaultShipping": data.get("defaultShipping", False),
            "defaultBilling":  data.get("defaultBilling", False),
        })
    return lines


def find_duplicate_groups(lines):
    """Group lines by lowercase label. Return dict of label -> [lines], only groups with >1."""
    groups = {}
    for line in lines:
        key = line["label"].lower()
        if not key:
            continue
        groups.setdefault(key, []).append(line)
    return {k: v for k, v in groups.items() if len(v) > 1}


def remove_line(customer_id, line):
    """Try DELETE; fall back to PATCH-relabel. Returns (ok, detail)."""
    r = ns_delete(f"customer/{customer_id}/addressBook/{line['line_id']}")
    if r.status_code in (200, 204):
        return True, "deleted"

    original = line["label"] or line["line_id"]
    r2 = ns_patch(f"customer/{customer_id}/addressBook/{line['line_id']}", {
        "label":           f"(Duplicate) {original}",
        "defaultShipping": False,
        "defaultBilling":  False,
    })
    if r2.status_code == 204:
        return True, f"relabeled (DELETE {r.status_code})"
    return False, f"DELETE {r.status_code}, PATCH {r2.status_code}"


def cleanup_customer(customer_id, live=False):
    print(f"\n[CUSTOMER {customer_id}]")
    lines = fetch_address_lines(customer_id)
    if not lines:
        print(f"  No address lines found")
        return 0
    print(f"  Total address lines: {len(lines)}")

    dupes = find_duplicate_groups(lines)
    if not dupes:
        print(f"  No duplicates")
        return 0

    total_removed = 0
    for label, group in sorted(dupes.items()):
        # Keep the numerically-lowest line_id (oldest)
        group_sorted = sorted(group, key=lambda x: int(x["line_id"]))
        keep = group_sorted[0]
        remove = group_sorted[1:]
        remove_ids = [x["line_id"] for x in remove]
        print(f"  [{keep['label']}] keep {keep['line_id']}, "
              f"remove {len(remove)}: {remove_ids}")

        if live:
            for line in remove:
                ok, detail = remove_line(customer_id, line)
                status = "OK" if ok else "FAIL"
                print(f"    {line['line_id']:>6}  {status}  {detail}")
                if ok:
                    total_removed += 1
                time.sleep(0.2)
        else:
            total_removed += len(remove)

    if live:
        print(f"  Removed {total_removed} duplicate line(s)")
    else:
        print(f"  DRY RUN: would remove {total_removed} line(s). "
              f"Pass --live to apply.")
    return total_removed


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
                        help="Actually remove duplicates. Default is dry-run.")
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
    print(f"  {verb} {total} duplicate address line(s) across {len(customer_ids)} customer(s)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
