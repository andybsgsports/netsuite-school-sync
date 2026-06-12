"""
diag_horeb_stragglers2.py — deep-search for hidden Tim Sarbacker / Brett Quale
contacts at Mt. Horeb High School (NS customer 2217).

The previous fix attempt failed because:
  - contactRoles for 2217 shows 23 contacts, none named Tim/Brett
  - But PATCH of their district contacts to company=2217 fails with
    "unique name already exists"

This means there are Tim/Brett contacts at 2217 that are NOT in contactRoles
(no explicit role assigned). This script:
  1. Tries customer/2217?expand=contactList (often empty, but worth trying)
  2. Scans contact IDs in ranges around known 2217 contacts looking for
     company=2217 AND matching first/last name
  3. Reports any found IDs so fix_horeb_stragglers2.py can update them

Env: NS_* + GOOGLE_CREDENTIALS_JSON
"""
from __future__ import annotations
import concurrent.futures, os, sys
from netsuite_sync import ns_get

HS_ID  = "2217"
TARGETS = [
    {"first": "Tim",   "last": "Sarbacker"},
    {"first": "Brett", "last": "Quale"},
]

# ID ranges to scan. Known HS contactRoles IDs top out at 47243.
# District contacts are 48485/48486. Scan the gap + a bit beyond.
SCAN_RANGES = [
    (47000, 47099),
    (47100, 47299),
    (47300, 47499),
    (47500, 47999),
    (48000, 48490),
]

def check_contact(cid):
    """Return (cid, company_id, first, last, email, inactive) or None on error."""
    r = ns_get(f"contact/{cid}")
    if r.status_code != 200:
        return None
    c = r.json()
    company_id = str((c.get("company") or {}).get("id") or "")
    first      = (c.get("firstName") or "").strip()
    last       = (c.get("lastName")  or "").strip()
    email      = (c.get("email")     or "").strip()
    inactive   = c.get("isInactive", False)
    return (str(cid), company_id, first, last, email, inactive)


def main():
    # 1. contactList expand
    print("=" * 60)
    print(f"1. customer/{HS_ID}?expand=contactList")
    print("=" * 60)
    r = ns_get(f"customer/{HS_ID}?expand=contactList")
    print(f"  HTTP {r.status_code}")
    if r.status_code == 200:
        items = r.json().get("contactList", {}).get("items", [])
        print(f"  contactList items: {len(items)}")
        for item in items:
            print(f"    {item}")
    else:
        print(f"  {r.text[:300]}")

    # 2. Scan contact ID ranges
    print("\n" + "=" * 60)
    print("2. Scanning contact ID ranges for company=2217 or name match")
    print("=" * 60)

    target_names = {
        f"{t['first']} {t['last']}".lower() for t in TARGETS
    }
    found = []

    for (lo, hi) in SCAN_RANGES:
        ids = range(lo, hi + 1)
        print(f"\n  Scanning IDs {lo}–{hi} ({len(ids)} contacts)…")
        hits_in_range = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
            futures = {ex.submit(check_contact, cid): cid for cid in ids}
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result is None:
                    continue
                cid, company_id, first, last, email, inactive = result
                name = f"{first} {last}".lower()
                is_hs = company_id == HS_ID
                is_target = name in target_names
                if is_hs or is_target:
                    tag = []
                    if is_hs:     tag.append("company=2217")
                    if is_target: tag.append("TARGET-NAME")
                    print(f"    [{', '.join(tag)}] ID {cid}: {first} {last} | "
                          f"company={company_id} | email={email} | inactive={inactive}")
                    hits_in_range += 1
                    if is_hs and is_target:
                        found.append(result)

        if hits_in_range == 0:
            print(f"    (no matches in range)")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY — contacts matching TARGET-NAME AND company=2217")
    print("=" * 60)
    if found:
        for r in found:
            cid, company_id, first, last, email, inactive = r
            print(f"  ID {cid}: {first} {last} | email={email} | inactive={inactive}")
        print(f"\n  → Use these IDs in the follow-up fix script.")
    else:
        print("  None found in scanned ranges.")
        print("  Try expanding SCAN_RANGES or checking a different hypothesis.")


if __name__ == "__main__":
    main()
