"""
diag_horeb_stragglers6.py — find the blocking Tim Sarbacker / Brett Quale contacts.

diag5 showed:
  - The 2217 contacts live in the 10002-10424 ID range (plus a few at 47148/47243)
  - No Tim/Brett at 2290 (checked by name, 24 total contacts there)
  - Scan of 44000-46999 crashed mid-way (connection reset with 50 workers)

This script:
  1. Prints ALL 24 contactRoles for 2290 with names (so we can see every contact)
  2. Scans 9000-12500 (3500 IDs) — the era where the 2217 contacts were created
  3. Scans 44000-46999 again with 15 workers and connection-error handling

All contact scanning now wraps ns_get in try/except so a single dropped connection
does not abort the entire scan.

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
    (9000,  12500),
    (44000, 46999),
]


def check_contact(cid):
    try:
        r = ns_get(f"contact/{cid}")
    except Exception:
        return None
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

    # 1. All contacts at 2290
    print("=" * 60)
    print(f"1. ALL contactRoles for customer {DISTRICT_ID}")
    print("=" * 60)
    ids_2290 = _list_customer_contact_ids(DISTRICT_ID)
    print(f"  Total IDs returned: {len(ids_2290)}")
    for cid in ids_2290:
        try:
            r = ns_get(f"contact/{cid}")
        except Exception as e:
            print(f"  ID {cid}: ERROR {e}")
            continue
        if r.status_code == 200:
            c = r.json()
            first    = c.get("firstName", "")
            last     = c.get("lastName",  "")
            email    = c.get("email",     "")
            inactive = c.get("isInactive", False)
            flag = " ** TARGET **" if f"{first} {last}".lower() in target_names else ""
            print(f"  ID {cid}: {first} {last}{flag} | email={email} | inactive={inactive}")
        else:
            print(f"  ID {cid}: HTTP {r.status_code}")

    # 2. Scan ID ranges
    print("\n" + "=" * 60)
    print("2. Scanning ID ranges for Tim/Brett (any company)")
    print("=" * 60)

    for lo, hi in SCAN_RANGES:
        ids = range(lo, hi + 1)
        print(f"\n  Scanning IDs {lo}–{hi} ({len(ids)} contacts) with 15 workers…")
        found_in_range = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=15) as ex:
            futures = {ex.submit(check_contact, cid): cid for cid in ids}
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                except Exception:
                    continue
                if result is None:
                    continue
                cid, company_id, first, last, email, inactive = result
                name = f"{first} {last}".lower()
                if name in target_names:
                    print(f"    ** FOUND ** ID {cid}: {first} {last} | "
                          f"company={company_id} | email={email} | inactive={inactive}")
                    found_in_range.append(result)

        if not found_in_range:
            print(f"    (no Tim/Brett found in range {lo}-{hi})")

    print("\nDone.")


if __name__ == "__main__":
    main()
