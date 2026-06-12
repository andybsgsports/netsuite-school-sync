"""
fix_horeb_stragglers2.py — rename-then-move strategy for Tim Sarbacker
and Brett Quale at Mt. Horeb High School (NS customer 2217).

Problem: PATCH of their district contacts (48486, 48485) to company=2217
fails with "unique name already exists". This means something (a hidden
contact with no role, or a parent-hierarchy uniqueness check) prevents
the move via a straight company PATCH.

Strategy (3 steps per contact):
  1. PATCH the contact to a temporary unique last name (e.g. "SarbackerFIX")
     → clears the name from the collision namespace
  2. PATCH the contact's company to 2217
     → moves it out of 2290's realm
  3. PATCH the contact's name back to the real first/last name
     → restores the correct name at 2217

If step 2 STILL fails (collision at 2217 with the temp name, which would
be very unusual), the script reports and rolls back to the original name.

After a successful 3-step move, sheet rows are repointed to NS Customer
ID = 2217 (NS Contact ID stays the same since it's the same record).

Env: NS_* + GOOGLE_SHEET_ID + GOOGLE_CREDENTIALS_JSON
"""
from __future__ import annotations
import os, sys
from netsuite_sync import ns_get, ns_patch
from school_netsuite_sync import (
    get_gspread_client, GOOGLE_SHEET_ID,
    C_FIRST, C_LAST, C_EMAIL, C_ROLE, C_NS_CID, C_NS_CUS, C_SCHOOL,
)

HS_ID        = "2217"
DISTRICT_ID  = "2290"
CONTACTS_TAB = "Contacts"
TEMP_SUFFIX  = "FIX2217"   # appended to lastName during the move

TARGETS = [
    {
        "first":             "Tim",
        "last":              "Sarbacker",
        "email":             "timsarb@yahoo.com",
        "stored_contact_id": "48486",
    },
    {
        "first":             "Brett",
        "last":              "Quale",
        "email":             "coachquale24@gmail.com",
        "stored_contact_id": "48485",
    },
]


def get_contact(cid):
    r = ns_get(f"contact/{cid}")
    return r.json() if r.status_code == 200 else {}


def patch_contact(cid, body):
    r = ns_patch(f"contact/{cid}", body)
    return r


def move_contact(cid, first, last, email, title=""):
    """
    Rename-then-move a contact to HS 2217.
    Returns True on success, False on failure.
    """
    temp_last = last + TEMP_SUFFIX

    # Step 1: rename to temporary unique name (stay at current company)
    print(f"  Step 1: rename {first} {last} → {first} {temp_last}")
    r1 = patch_contact(cid, {"firstName": first, "lastName": temp_last})
    if r1.status_code != 204:
        print(f"  FAIL step 1: {r1.status_code} {r1.text[:300]}")
        return False

    # Step 2: move to company 2217 (with temp name)
    print(f"  Step 2: set company={HS_ID}")
    body2 = {
        "firstName": first,
        "lastName":  temp_last,
        "company":   {"id": int(HS_ID)},
        "email":     email,
        "isInactive": False,
    }
    if title:
        body2["title"] = title
    r2 = patch_contact(cid, body2)
    if r2.status_code != 204:
        print(f"  FAIL step 2: {r2.status_code} {r2.text[:300]}")
        # Roll back: restore original name (still at 2290 since step 2 failed)
        rb = patch_contact(cid, {"firstName": first, "lastName": last})
        print(f"  Rollback rename: {'OK' if rb.status_code == 204 else rb.status_code}")
        return False

    # Step 3: restore real name at 2217
    print(f"  Step 3: restore name → {first} {last}")
    body3 = {"firstName": first, "lastName": last}
    r3 = patch_contact(cid, body3)
    if r3.status_code != 204:
        print(f"  WARN step 3 failed: {r3.status_code} {r3.text[:300]}")
        print(f"  Contact {cid} is now at 2217 with temp name {first} {temp_last}")
        print(f"  Manual rename required: {first} {temp_last} → {first} {last}")
        # Still treat this as a partial success — company is moved
        return True   # sheet should still be repointed

    print(f"  All 3 steps succeeded — {first} {last} is now on HS {HS_ID}")
    return True


def main():
    gc = get_gspread_client()
    wb = gc.open_by_key(GOOGLE_SHEET_ID)
    ws = wb.worksheet(CONTACTS_TAB)

    rows      = ws.get_all_values()
    headers   = rows[0]
    data_rows = rows[1:]

    col       = {h: i for i, h in enumerate(headers)}
    ci_first  = col.get(C_FIRST,  col.get("First",  0))
    ci_last   = col.get(C_LAST,   col.get("Last",   1))
    ci_email  = col.get(C_EMAIL,  col.get("Email",  3))
    ci_role   = col.get(C_ROLE,   col.get("Role",   4))
    ci_cid    = col.get(C_NS_CID, col.get("NS Contact ID",  9))
    ci_cus    = col.get(C_NS_CUS, col.get("NS Customer ID", 10))
    ci_school = col.get(C_SCHOOL, col.get("School Name", 0))

    for target in TARGETS:
        first  = target["first"]
        last   = target["last"]
        email  = target["email"].lower()
        cid    = target["stored_contact_id"]

        print(f"\n{'='*60}")
        print(f"Processing: {first} {last} (contact {cid})")

        # Verify current state
        c = get_contact(cid)
        if not c:
            print(f"  ERROR: could not GET contact {cid}")
            continue
        current_company = str((c.get("company") or {}).get("id") or "")
        print(f"  Current company: {current_company}")

        if current_company == HS_ID:
            print(f"  Already on HS {HS_ID} — checking sheet rows only")
        else:
            # Gather title from sheet rows for this person
            titles = []
            for row in data_rows:
                if (row[ci_first].strip().lower() == first.lower()
                        and row[ci_last].strip().lower() == last.lower()
                        and row[ci_email].strip().lower() == email
                        and "horeb" in row[ci_school].lower()):
                    t = row[ci_role].strip()
                    if t and t not in titles:
                        titles.append(t)
            title = titles[0] if titles else (c.get("title") or "").strip()

            ok = move_contact(cid, first, last, email, title)
            if not ok:
                print(f"  FAILED to move {first} {last} — manual action required")
                continue

        # Repoint sheet rows
        updates = []
        for i, row in enumerate(data_rows, start=2):
            if (row[ci_first].strip().lower() == first.lower()
                    and row[ci_last].strip().lower() == last.lower()
                    and row[ci_email].strip().lower() == email
                    and "horeb" in row[ci_school].lower()):
                if row[ci_cus] != HS_ID:
                    updates.append(i)
                    print(f"  Sheet row {i}: NS Customer ID {row[ci_cus]} → {HS_ID}")

        for row_i in updates:
            ws.update_cell(row_i, ci_cus + 1, HS_ID)
        if updates:
            print(f"  Updated {len(updates)} sheet row(s)")
        else:
            print(f"  Sheet rows already show NS Customer ID={HS_ID}")

    print("\nDone.")


if __name__ == "__main__":
    main()
