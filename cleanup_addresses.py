"""
cleanup_addresses.py
--------------------
One-time script to deduplicate Ship-To addresses on all 28 school Customer records.
Uses replace=true on PATCH to force full sublist replacement.
"""

import sys, json
sys.path.insert(0, ".")
from netsuite_sync import ns_get, ns_patch, make_auth, BASE_URL
import requests
import time

SCHOOLS = {
    "Barneveld": 994, "Beloit Memorial": 1029, "Big Foot": 1009,
    "Brookfield Academy": 667, "Brookfield Central": 1094, "Brookfield East": 1037,
    "Brown Deer": 1047, "Burlington": 1048, "Catholic Central - Burlington": 1040,
    "Clinton": 1265, "Delavan-Darien": 1436, "East Troy": 1562,
    "Janesville Craig": 1967, "Janesville Parker": 1968, "Kenosha Tremper": 2033,
    "Mount Horeb": 2217, "Mukwonago": 2318, "Palmyra-Eagle": 2849,
    "Racine Lutheran": 3000, "Beloit Turner": 1002, "Union Grove": 3551,
    "Verona Area": 3589, "Waterford": 3651, "Westosha Central": 1230,
    "Whitnall": 3666, "Whitewater": 3670, "Williams Bay": 3710,
    "Wilmot Union": 3711,
}


def fetch_with_retry(path, max_retries=3):
    """GET with retry on connection errors."""
    for attempt in range(max_retries):
        try:
            r = ns_get(path)
            return r
        except Exception as e:
            if attempt < max_retries - 1:
                wait = 5 * (attempt + 1)
                print(f"    Connection error, retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


def patch_with_replace(path, body, max_retries=3):
    """PATCH with replace=true to force full sublist replacement."""
    url = f"{BASE_URL}/{path}?replace=addressBook"
    for attempt in range(max_retries):
        try:
            r = requests.patch(url, headers={
                "Authorization": make_auth("PATCH", url),
                "Content-Type": "application/json"
            }, json=body)
            return r
        except Exception as e:
            if attempt < max_retries - 1:
                wait = 5 * (attempt + 1)
                print(f"    Connection error on PATCH, retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


def fetch_addressbook(ns_id):
    """Fetch addressbook using expandSubResources."""
    r = fetch_with_retry(f"customer/{ns_id}?expandSubResources=true")
    if r.status_code == 200:
        data = r.json()
        ab = data.get("addressBook", {})
        if isinstance(ab, dict):
            return ab.get("items", [])
    return []


def cleanup_customer(name, ns_id):
    """Remove duplicate addresses from a customer record."""
    print(f"\n  {name} ({ns_id}):")
    items = fetch_addressbook(ns_id)

    if not items:
        print(f"    No addresses")
        return False

    print(f"    {len(items)} addresses found")

    # Deduplicate: keep first occurrence of each label
    seen_labels = set()
    kept = []
    removed_names = []
    has_billing = False

    for item in items:
        label = item.get("label", "").strip().lower()
        is_billing = item.get("defaultBilling", False)

        if is_billing:
            if not has_billing:
                kept.append(item)
                has_billing = True
            else:
                removed_names.append(item.get("label", "(bill-to dup)"))
        elif not label:
            kept.append(item)
        elif label not in seen_labels:
            kept.append(item)
            seen_labels.add(label)
        else:
            removed_names.append(item.get("label", "(unknown)"))

    if not removed_names:
        print(f"    No duplicates")
        return False

    print(f"    Removing {len(removed_names)} duplicates ({len(items)} -> {len(kept)})")

    # Use replace=addressBook to force full replacement of the sublist
    r = patch_with_replace(f"customer/{ns_id}", {"addressBook": {"items": kept}})
    if r.status_code == 204:
        print(f"    Done!")
        return True
    else:
        print(f"    ERROR: {r.status_code} {r.text[:300]}")
        return False


def main():
    print("=" * 60)
    print("ADDRESS DEDUPLICATION CLEANUP")
    print("=" * 60)

    # Test with Barneveld first (we know it still has dupes)
    print("\n--- Testing Barneveld first ---")
    success = cleanup_customer("Barneveld", 994)
    if success:
        print("\n    Verifying... fetching again...")
        time.sleep(2)
        items = fetch_addressbook(994)
        print(f"    After cleanup: {len(items)} addresses")
        labels = [i.get("label", "(no label)") for i in items]
        for lbl in labels:
            print(f"      {lbl}")
        input("\n    Check Barneveld in NetSuite. Press Enter to continue with all schools...")
    else:
        print("    Barneveld had no dupes or failed. Check manually.")
        input("    Press Enter to continue...")

    print("\n--- Processing all remaining schools ---")
    updated = 0
    for name, ns_id in sorted(SCHOOLS.items()):
        if name == "Barneveld":
            continue  # already done above
        success = cleanup_customer(name, ns_id)
        if success:
            updated += 1
        time.sleep(2)

    print(f"\nDone. {updated + (1 if success else 0)} customers updated total.")


if __name__ == "__main__":
    main()
