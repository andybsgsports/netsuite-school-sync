"""
fix_horeb_stragglers3.py — finish the rename-move for Tim Sarbacker / Brett Quale.

After fix_horeb_stragglers2.py ran:
  - Contact 48486 is now at company 2217 with temp name "Tim SarbackerFIX2217"
  - Contact 48485 is now at company 2217 with temp name "Brett QualeFIX2217"
  - Step 3 (restoring real names) failed because OLDER hidden contacts with those
    real names still exist at 2217 (IDs below the 47000 range we scanned).

This script:
  1. Uses SuiteQL to find all contacts at company=2217 named Tim Sarbacker / Brett Quale
  2. Any such contact NOT equal to 48486/48485 is a hidden duplicate → mark isInactive=True
  3. After deactivating the blockers, renames 48486 → "Tim Sarbacker" and 48485 → "Brett Quale"
  4. Verifies final state

Env: NS_* + GOOGLE_CREDENTIALS_JSON
"""
from __future__ import annotations
import os, sys
from netsuite_sync import ns_suiteql, ns_get, ns_patch

HS_ID       = "2217"
TEMP_SUFFIX = "FIX2217"

TARGETS = [
    {"first": "Tim",   "last": "Sarbacker", "fix_cid": "48486"},
    {"first": "Brett", "last": "Quale",     "fix_cid": "48485"},
]


def find_hidden_contacts(first, last, fix_cid):
    """SuiteQL: find contacts at 2217 with this name, excluding the known temp-named one."""
    query = (
        f"SELECT id, firstName, lastName, email, isInactive FROM contact "
        f"WHERE company = {HS_ID} "
        f"AND firstName = '{first}' AND lastName = '{last}'"
    )
    rows = ns_suiteql(query)
    hidden = []
    for row in rows:
        cid = str(row.get("id", ""))
        if cid and cid != fix_cid:
            hidden.append(row)
    return rows, hidden


def deactivate_contact(cid):
    r = ns_patch(f"contact/{cid}", {"isInactive": True})
    return r.status_code, r.text[:300] if r.status_code != 204 else ""


def restore_name(cid, first, last, email=""):
    body = {"firstName": first, "lastName": last}
    if email:
        body["email"] = email
    r = ns_patch(f"contact/{cid}", body)
    return r.status_code, r.text[:300] if r.status_code != 204 else ""


def get_contact(cid):
    r = ns_get(f"contact/{cid}")
    return r.json() if r.status_code == 200 else {}


def main():
    all_ok = True

    for target in TARGETS:
        first    = target["first"]
        last     = target["last"]
        fix_cid  = target["fix_cid"]
        temp_last = last + TEMP_SUFFIX

        print(f"\n{'='*60}")
        print(f"Processing: {first} {last} (fix contact={fix_cid})")

        # Verify the fix contact is at 2217 with temp name
        c = get_contact(fix_cid)
        if not c:
            print(f"  ERROR: could not GET contact {fix_cid}")
            all_ok = False
            continue

        current_company = str((c.get("company") or {}).get("id") or "")
        current_first   = c.get("firstName", "")
        current_last    = c.get("lastName", "")
        print(f"  Contact {fix_cid}: company={current_company}  name='{current_first} {current_last}'")

        if current_company != HS_ID:
            print(f"  UNEXPECTED: contact {fix_cid} is not at 2217 — skipping")
            all_ok = False
            continue

        if current_last == last:
            print(f"  Already has real name — nothing to do for {first} {last}")
            continue

        if current_last != temp_last:
            print(f"  UNEXPECTED temp name: '{current_last}' (expected '{temp_last}') — proceeding anyway")

        # Step 1: find hidden same-name contacts at 2217
        print(f"\n  SuiteQL: searching for '{first} {last}' at company {HS_ID}…")
        all_rows, hidden = find_hidden_contacts(first, last, fix_cid)
        print(f"  SuiteQL rows returned: {len(all_rows)}")
        for row in all_rows:
            cid2 = str(row.get("id", ""))
            marker = " ← FIX CONTACT (temp name, won't match)" if cid2 == fix_cid else " ← HIDDEN DUPLICATE"
            print(f"    ID {cid2}: {row.get('firstName')} {row.get('lastName')} | "
                  f"email={row.get('email')} | inactive={row.get('isInactive')}{marker}")

        # Step 2: deactivate hidden duplicates
        for row in hidden:
            cid2 = str(row.get("id", ""))
            print(f"\n  Deactivating hidden duplicate {cid2}: {row.get('firstName')} {row.get('lastName')}")
            status, body = deactivate_contact(cid2)
            if status == 204:
                print(f"  → Deactivated OK")
            else:
                print(f"  → FAIL deactivate {cid2}: HTTP {status} {body}")
                all_ok = False

        # Step 3: rename fix contact back to real name
        print(f"\n  Renaming contact {fix_cid}: '{first} {temp_last}' → '{first} {last}'")
        status, body = restore_name(fix_cid, first, last)
        if status == 204:
            print(f"  → Renamed OK — {first} {last} is now at company {HS_ID}")
        else:
            print(f"  → FAIL rename: HTTP {status} {body}")
            all_ok = False

    print(f"\n{'='*60}")
    if all_ok:
        print("All done — both contacts should now be at 2217 with real names.")
    else:
        print("SOME STEPS FAILED — review output above.")
    print("Done.")


if __name__ == "__main__":
    main()
