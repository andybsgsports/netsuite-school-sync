"""
fix_horeb_stragglers4.py — final fix for Tim Sarbacker / Brett Quale at Mt. Horeb HS.

diag6 found the blockers: two INACTIVE duplicate contacts already at 2217 holding
the unique-name lock:
  - ID 10332: Tim Sarbacker  | company=2217 | timsarb@yahoo.com       | inactive=True
  - ID 10007: Brett Quale    | company=2217 | coachquale24@gmail.com  | inactive=True

Plan per target:
  1. Verify the old duplicate is still inactive and named as expected (abort if not)
  2. Rename the old duplicate to "<Last>OLDDUP<id>" to release the unique name
  3. Rename the fixed contact (48486/48485, currently temp-named) to the real name
  4. Verify final state with a GET

Env: NS_*
"""
from __future__ import annotations
from netsuite_sync import ns_get, ns_patch

TARGETS = [
    {"fix_cid": "48486", "dup_cid": "10332", "first": "Tim",   "last": "Sarbacker"},
    {"fix_cid": "48485", "dup_cid": "10007", "first": "Brett", "last": "Quale"},
]


def show(cid):
    r = ns_get(f"contact/{cid}")
    if r.status_code != 200:
        print(f"    contact/{cid}: HTTP {r.status_code} {r.text[:200]}")
        return None
    c = r.json()
    company = str((c.get("company") or {}).get("id") or "")
    print(f"    contact/{cid}: {c.get('firstName')} {c.get('lastName')} | "
          f"company={company} | email={c.get('email')} | inactive={c.get('isInactive')}")
    return c


def main():
    ok = True
    for t in TARGETS:
        first, last = t["first"], t["last"]
        dup_cid, fix_cid = t["dup_cid"], t["fix_cid"]
        print("=" * 60)
        print(f"{first} {last}: dup={dup_cid}, fix={fix_cid}")
        print("=" * 60)

        # 1. Verify the duplicate
        print("  1. Verify old duplicate")
        dup = show(dup_cid)
        if not dup or (dup.get("firstName") or "").strip() != first \
                or (dup.get("lastName") or "").strip() != last:
            print(f"    ABORT: duplicate {dup_cid} does not match expected name")
            ok = False
            continue
        if not dup.get("isInactive", False):
            print(f"    WARNING: duplicate {dup_cid} is ACTIVE (expected inactive) — continuing")

        # 2. Rename the duplicate to release the unique name
        new_dup_last = f"{last}OLDDUP{dup_cid}"
        print(f"  2. Rename duplicate {dup_cid} -> '{first} {new_dup_last}'")
        r = ns_patch(f"contact/{dup_cid}", {"lastName": new_dup_last})
        print(f"    HTTP {r.status_code}" + ("" if r.status_code == 204 else f" {r.text[:300]}"))
        if r.status_code != 204:
            print("    ABORT: could not rename duplicate")
            ok = False
            continue

        # 3. Rename the fixed contact to the real name
        print(f"  3. Rename fixed contact {fix_cid} -> '{first} {last}'")
        r = ns_patch(f"contact/{fix_cid}", {"firstName": first, "lastName": last})
        print(f"    HTTP {r.status_code}" + ("" if r.status_code == 204 else f" {r.text[:300]}"))
        if r.status_code != 204:
            print("    FAILED to rename fixed contact")
            ok = False
            continue

        # 4. Verify
        print("  4. Verify final state")
        show(fix_cid)
        show(dup_cid)
        print(f"  DONE: {first} {last} is now correctly named at company 2217\n")

    print("ALL FIXES SUCCEEDED." if ok else "ONE OR MORE FIXES FAILED — see above.")


if __name__ == "__main__":
    main()
