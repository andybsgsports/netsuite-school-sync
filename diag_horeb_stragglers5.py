"""
diag_horeb_stragglers5.py — enumerate all contactRoles contacts at 2217
and scan ID ranges below 47000 to find the blocking Tim Sarbacker / Brett Quale.

SuiteQL and REST search are blocked by the token's role (HTTP 400). We must
scan by ID. The diag2 scan covered 47000-48490; the blocking contacts are older
(lower IDs). This script:
  1. Prints all 23 contactRoles IDs and names for customer 2217 (to know the range)
  2. Also prints contactRoles for customer 2290 (district) to confirm no Tim/Brett remain
  3. Scans IDs 40000-46999 with 50 workers looking for company=2217 AND name match

Env: NS_*
"""
from __future__ import annotations
import concurrent.futures
from netsuite_sync import ns_get, _list_customer_contact_ids

HS_ID       = "2217"
DISTRICT_ID = "2290"
TARGETS = [
    {"first": "Tim",   "last": "Sarbacker"},
    {"first": "Brett", "last": "Quale"},
]
SCAN_RANGES = [
    (44000, 46999),
]


def check_contact(cid):
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
    target_names = {f"{t['first']} {t['last']}".lower() for t in TARGETS}

    # 1. contactRoles for 2217
    print("=" * 60)
    print(f"1. contactRoles for customer {HS_ID}")
    print("=" * 60)
    ids_2217 = _list_customer_contact_ids(HS_ID)
    print(f"  Total IDs returned: {len(ids_2217)}")
    for cid in ids_2217:
        r = ns_get(f"contact/{cid}")
        if r.status_code == 200:
            c = r.json()
            first = c.get("firstName", "")
            last  = c.get("lastName", "")
            email = c.get("email", "")
            inactive = c.get("isInactive", False)
            print(f"  ID {cid}: {first} {last} | email={email} | inactive={inactive}")
        else:
            print(f"  ID {cid}: HTTP {r.status_code}")

    # 2. contactRoles for 2290
    print("\n" + "=" * 60)
    print(f"2. contactRoles for customer {DISTRICT_ID} — check for remaining Tim/Brett")
    print("=" * 60)
    ids_2290 = _list_customer_contact_ids(DISTRICT_ID)
    print(f"  Total IDs returned: {len(ids_2290)}")
    for cid in ids_2290:
        r = ns_get(f"contact/{cid}")
        if r.status_code == 200:
            c = r.json()
            first = c.get("firstName", "")
            last  = c.get("lastName", "")
            name = f"{first} {last}".lower()
            if name in target_names:
                print(f"  ** TARGET ** ID {cid}: {first} {last} | email={c.get('email')} | inactive={c.get('isInactive')}")
        # Only print target matches at 2290 to reduce noise

    # 3. Scan lower ID ranges
    print("\n" + "=" * 60)
    print("3. Scanning lower ID ranges for Tim/Brett (any company)")
    print("=" * 60)

    for lo, hi in SCAN_RANGES:
        ids = range(lo, hi + 1)
        print(f"\n  Scanning IDs {lo}–{hi} ({len(ids)} contacts) with 50 workers…")
        found_in_range = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as ex:
            futures = {ex.submit(check_contact, cid): cid for cid in ids}
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result is None:
                    continue
                cid, company_id, first, last, email, inactive = result
                name = f"{first} {last}".lower()
                if name in target_names:
                    print(f"    ** FOUND ** ID {cid}: {first} {last} | "
                          f"company={company_id} | email={email} | inactive={inactive}")
                    found_in_range.append(result)

        if not found_in_range:
            print(f"    (no Tim/Brett found in range)")

    print("\nDone.")


if __name__ == "__main__":
    main()
