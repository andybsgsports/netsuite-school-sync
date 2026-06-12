"""
fix_horeb_stragglers.py — repair Tim Sarbacker and Brett Quale at
Mt. Horeb High School (NS customer 2217).

Both contacts are currently linked to the district parent (2290) in NS.
Every nightly sync tries to PATCH them to company=2217 and hits a NS
"unique name" rejection — meaning 2217 already owns a contact with
those first+last names.

This script:
  1. Walks customer 2217's contactRoles via the paginated sub-resource
     endpoint to find the existing HS-native contacts for each target.
  2. PATCHes the HS-native contact with the correct email/title/company.
  3. Updates the sheet's NS Contact ID rows to point at the HS contact.
  4. If NO HS-native contact is found, tries a direct PATCH on the stored
     district contact to move it to 2217 (only works if the "unique name"
     constraint is no longer triggered).

Env: NS_* + GOOGLE_SHEET_ID + GOOGLE_CREDENTIALS_JSON
"""
from __future__ import annotations
import os, sys
from netsuite_sync import ns_get, ns_patch
from school_netsuite_sync import (
    get_gspread_client, GOOGLE_SHEET_ID,
    C_FIRST, C_LAST, C_EMAIL, C_ROLE, C_NS_CID, C_NS_CUS, C_SCHOOL,
)

HS_ID       = "2217"
DISTRICT_ID = "2290"
CONTACTS_TAB = "Contacts"

TARGETS = [
    {
        "first": "Tim",
        "last":  "Sarbacker",
        "email": "timsarb@yahoo.com",
        "stored_contact_id": "48486",
    },
    {
        "first": "Brett",
        "last":  "Quale",
        "email": "coachquale24@gmail.com",
        "stored_contact_id": "48485",
    },
]


def get_contact_detail(cid):
    r = ns_get(f"contact/{cid}")
    return r.json() if r.status_code == 200 else {}


def list_hs_contacts():
    """Return list of (contact_id, first, last, email) for customer 2217."""
    results = []
    offset, limit = 0, 100
    while True:
        r = ns_get(f"customer/{HS_ID}/contactRoles?limit={limit}&offset={offset}")
        if r.status_code != 200:
            # Sub-resource failed; fall back to single-page expand
            r2 = ns_get(f"customer/{HS_ID}?expand=contactRoles")
            if r2.status_code == 200:
                for item in r2.json().get("contactRoles", {}).get("items", []):
                    cid = (item.get("contact") or {}).get("id")
                    if not cid:
                        href = (item.get("links") or [{}])[0].get("href", "")
                        line = href.rstrip("/").split("/")[-1] if href else ""
                        if line:
                            r3 = ns_get(f"customer/{HS_ID}/contactRoles/{line}")
                            if r3.status_code == 200:
                                cid = (r3.json().get("contact") or {}).get("id")
                    if cid:
                        c = get_contact_detail(cid)
                        results.append((
                            str(cid),
                            (c.get("firstName") or "").strip(),
                            (c.get("lastName")  or "").strip(),
                            (c.get("email")     or "").strip().lower(),
                        ))
            break
        body  = r.json()
        items = body.get("items", [])
        for item in items:
            cid = (item.get("contact") or {}).get("id")
            if not cid:
                href = (item.get("links") or [{}])[0].get("href", "")
                line = href.rstrip("/").split("/")[-1] if href else ""
                if line:
                    r2 = ns_get(f"customer/{HS_ID}/contactRoles/{line}")
                    if r2.status_code == 200:
                        cid = (r2.json().get("contact") or {}).get("id")
            if cid:
                c = get_contact_detail(cid)
                results.append((
                    str(cid),
                    (c.get("firstName") or "").strip(),
                    (c.get("lastName")  or "").strip(),
                    (c.get("email")     or "").strip().lower(),
                ))
        total  = body.get("totalResults", 0)
        offset += limit
        if offset >= total or not items:
            break
    return results


def patch_contact(cid, first, last, email, title="", company_id=HS_ID):
    body = {
        "firstName":  first,
        "lastName":   last,
        "email":      email,
        "company":    {"id": int(company_id)},
        "isInactive": False,
    }
    if title:
        body["title"] = title
    r = ns_patch(f"contact/{cid}", body)
    return r


def main():
    gc = get_gspread_client()
    wb = gc.open_by_key(GOOGLE_SHEET_ID)
    ws = wb.worksheet(CONTACTS_TAB)

    rows      = ws.get_all_values()
    headers   = rows[0]
    data_rows = rows[1:]

    col = {h: i for i, h in enumerate(headers)}
    ci_first  = col.get(C_FIRST,  col.get("First",  0))
    ci_last   = col.get(C_LAST,   col.get("Last",   1))
    ci_email  = col.get(C_EMAIL,  col.get("Email",  3))
    ci_role   = col.get(C_ROLE,   col.get("Role",   4))
    ci_cid    = col.get(C_NS_CID, col.get("NS Contact ID", 9))
    ci_cus    = col.get(C_NS_CUS, col.get("NS Customer ID", 10))
    ci_school = col.get(C_SCHOOL, col.get("School Name", 0))

    # Enumerate all contacts on HS 2217 once
    print(f"Loading all contacts on HS customer {HS_ID}…")
    hs_contacts = list_hs_contacts()
    print(f"  Found {len(hs_contacts)} contacts via contactRoles")
    for row in hs_contacts:
        print(f"    {row}")

    for target in TARGETS:
        first  = target["first"]
        last   = target["last"]
        email  = target["email"].lower()
        stored = target["stored_contact_id"]

        print(f"\n{'='*60}")
        print(f"Processing: {first} {last} (stored ID: {stored})")

        # --- Find existing HS-native contact by name or email ---
        hs_cid = None
        for (cid, fn, ln, em) in hs_contacts:
            if fn.lower() == first.lower() and ln.lower() == last.lower():
                hs_cid = cid
                print(f"  Found by name on HS 2217: contact {cid}")
                break
            if em == email:
                hs_cid = cid
                print(f"  Found by email on HS 2217: contact {cid} ({fn} {ln})")
                break

        if hs_cid:
            # PATCH the HS-native contact to ensure data is current,
            # then repoint all sheet rows for this person to that ID.
            # Gather all distinct titles from sheet rows for this person.
            titles = []
            for row in data_rows:
                if (row[ci_first].strip().lower() == first.lower()
                        and row[ci_last].strip().lower() == last.lower()
                        and row[ci_email].strip().lower() == email
                        and "horeb" in row[ci_school].lower()):
                    t = row[ci_role].strip()
                    if t and t not in titles:
                        titles.append(t)
            # Use the first distinct title for the primary company PATCH
            primary_title = titles[0] if titles else ""
            r = patch_contact(hs_cid, first, last, email, primary_title)
            if r.status_code == 204:
                print(f"  PATCHed HS contact {hs_cid} (title={primary_title!r})")
            else:
                print(f"  WARN: PATCH {hs_cid} → {r.status_code} {r.text[:200]}")

            # Update sheet rows: set NS Contact ID = hs_cid, NS Customer ID = 2217
            updates = []
            for i, row in enumerate(data_rows, start=2):
                if (row[ci_first].strip().lower() == first.lower()
                        and row[ci_last].strip().lower() == last.lower()
                        and row[ci_email].strip().lower() == email
                        and "horeb" in row[ci_school].lower()):
                    if row[ci_cid] != hs_cid or row[ci_cus] != HS_ID:
                        updates.append((i, hs_cid))
                        print(f"  Sheet row {i}: repointing "
                              f"NS Contact ID {row[ci_cid]} → {hs_cid}, "
                              f"NS Customer ID {row[ci_cus]} → {HS_ID}")

            if updates:
                for (row_i, new_cid) in updates:
                    ws.update_cell(row_i, ci_cid + 1, new_cid)
                    ws.update_cell(row_i, ci_cus + 1, HS_ID)
                print(f"  Updated {len(updates)} sheet row(s)")
            else:
                print("  Sheet rows already correct")

        else:
            # No HS-native contact found — try direct PATCH of stored district
            # contact to move it to HS 2217
            print(f"  No HS-native contact found for {first} {last}")
            print(f"  Trying direct PATCH of stored ID {stored} → company={HS_ID}")
            c = get_contact_detail(stored)
            title_now = (c.get("title") or "").strip()
            r = patch_contact(stored, first, last, email, title_now, company_id=HS_ID)
            if r.status_code == 204:
                print(f"  Direct PATCH succeeded — {first} {last} now on HS {HS_ID}")
                # Update sheet NS Customer ID (Contact ID stays the same)
                for i, row in enumerate(data_rows, start=2):
                    if (row[ci_first].strip().lower() == first.lower()
                            and row[ci_last].strip().lower() == last.lower()
                            and row[ci_email].strip().lower() == email
                            and "horeb" in row[ci_school].lower()):
                        if row[ci_cus] != HS_ID:
                            ws.update_cell(i, ci_cus + 1, HS_ID)
                            print(f"  Sheet row {i}: NS Customer ID → {HS_ID}")
            else:
                print(f"  Direct PATCH failed: {r.status_code} {r.text[:300]}")
                print(f"  Manual action required for {first} {last}")

    print("\nDone.")


if __name__ == "__main__":
    main()
