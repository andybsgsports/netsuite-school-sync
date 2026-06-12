"""
diag_horeb_contacts.py — pinpoint why Tim Sarbacker and Brett Quale
are not being found on Mt. Horeb High School (NS customer 2217).

Checks:
  1. Reads their rows from the Contacts sheet (stored NS Contact ID, name)
  2. GETs each stored NS Contact ID — shows actual name + company in NS
  3. Lists ALL contacts on customer 2217 (HS) via contactRoles, with pagination
  4. Lists ALL contacts on customer 2290 (district) via contactRoles
  5. Fuzzy-searches all found contacts for "sarbacker" / "quale" by last name

Env: NS_* + GOOGLE_SHEET_ID + GOOGLE_CREDENTIALS_JSON
"""
from __future__ import annotations
import json, os, sys
from netsuite_sync import ns_get
from school_netsuite_sync import get_gspread_client, GOOGLE_SHEET_ID

TARGETS = [
    {"first_hint": "tim",   "last_hint": "sarbacker"},
    {"first_hint": "brett", "last_hint": "quale"},
]
HS_ID       = "2217"
DISTRICT_ID = "2290"
CONTACTS_TAB = "Contacts"


# ── helpers ──────────────────────────────────────────────────────────────────

def get_contact(cid):
    r = ns_get(f"contact/{cid}")
    if r.status_code == 200:
        return r.json()
    return {"_error": r.status_code, "_text": r.text[:200]}


def list_all_contact_ids(customer_id):
    """Page through contactRoles and return every contact id."""
    ids = []
    offset = 0
    limit  = 1000
    while True:
        path = f"customer/{customer_id}?expand=contactRoles&limit={limit}&offset={offset}"
        r = ns_get(path)
        if r.status_code != 200:
            print(f"  [!] GET customer/{customer_id} contactRoles HTTP {r.status_code}")
            break
        items = r.json().get("contactRoles", {}).get("items", [])
        for item in items:
            cid = (item.get("contact") or {}).get("id")
            if not cid:
                href = (item.get("links") or [{}])[0].get("href", "")
                line = href.rstrip("/").split("/")[-1] if href else ""
                if line:
                    r2 = ns_get(f"customer/{customer_id}/contactRoles/{line}")
                    if r2.status_code == 200:
                        cid = (r2.json().get("contact") or {}).get("id")
            if cid:
                ids.append(str(cid))
        # totalResults tells us if there are more pages
        total = r.json().get("totalResults") or r.json().get("contactRoles", {}).get("totalResults") or 0
        offset += limit
        if offset >= total or not items:
            break
    return ids


def summarise_contact(cid):
    c = get_contact(cid)
    if "_error" in c:
        return f"ID {cid}: ERROR {c['_error']} {c['_text']}"
    first = (c.get("firstName") or "").strip()
    last  = (c.get("lastName")  or "").strip()
    email = (c.get("email")     or "").strip()
    comp  = ((c.get("company") or {}).get("id") or "")
    inactive = c.get("isInactive", False)
    return (f"ID {cid}: {first} {last} | email={email} | "
            f"company={comp} | inactive={inactive}")


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    # 1. Read sheet rows for Sarbacker and Quale
    print("=" * 60)
    print("1. SHEET ROWS for targets")
    print("=" * 60)
    gc = get_gspread_client()
    wb = gc.open_by_key(GOOGLE_SHEET_ID)
    try:
        ws = wb.worksheet(CONTACTS_TAB)
    except Exception:
        # Try the first sheet
        ws = wb.get_worksheet(0)

    rows = ws.get_all_records()
    target_rows = {}
    for row in rows:
        school = str(row.get("School Name", "")).strip()
        if "horeb" not in school.lower():
            continue
        first = str(row.get("First", "")).strip().lower()
        last  = str(row.get("Last",  "")).strip().lower()
        for t in TARGETS:
            if t["last_hint"] in last and t["first_hint"][:3] in first:
                key = f"{first} {last}"
                target_rows[key] = row
                print(f"\n  School: {school}")
                print(f"  Name  : {row.get('First')} {row.get('Last')}")
                print(f"  Email : {row.get('Email')}")
                print(f"  Role  : {row.get('Role')}")
                print(f"  NS Contact ID : {row.get('NS Contact ID')}")
                print(f"  NS Customer ID: {row.get('NS Customer ID')}")
                print(f"  Content Hash  : {row.get('Content Hash')}")

    if not target_rows:
        print("  (no matching rows found in Contacts tab for Horeb schools)")

    # 2. GET each stored NS Contact ID
    print("\n" + "=" * 60)
    print("2. NS LOOKUP for stored contact IDs")
    print("=" * 60)
    for key, row in target_rows.items():
        cid = str(row.get("NS Contact ID", "")).strip()
        print(f"\n  {key.title()} — stored NS Contact ID: {cid}")
        if cid and cid.isdigit():
            print("  " + summarise_contact(cid))
        else:
            print("  (no stored ID)")

    # 3. All contacts on HS (2217)
    print("\n" + "=" * 60)
    print(f"3. ALL CONTACTS on customer {HS_ID} (Mt. Horeb High School)")
    print("=" * 60)
    hs_ids = list_all_contact_ids(HS_ID)
    print(f"  Total contacts found: {len(hs_ids)}")
    for cid in hs_ids:
        c = get_contact(cid)
        first = (c.get("firstName") or "").strip()
        last  = (c.get("lastName")  or "").strip()
        inactive = c.get("isInactive", False)
        row_str = summarise_contact(cid)
        marker = ""
        for t in TARGETS:
            if t["last_hint"] in last.lower():
                marker = " ◄◄◄ TARGET"
        print(f"  {row_str}{marker}")

    # 4. All contacts on District (2290)
    print("\n" + "=" * 60)
    print(f"4. ALL CONTACTS on customer {DISTRICT_ID} (Mt. Horeb School District)")
    print("=" * 60)
    dist_ids = list_all_contact_ids(DISTRICT_ID)
    print(f"  Total contacts found: {len(dist_ids)}")
    for cid in dist_ids:
        c = get_contact(cid)
        first = (c.get("firstName") or "").strip()
        last  = (c.get("lastName")  or "").strip()
        row_str = summarise_contact(cid)
        marker = ""
        for t in TARGETS:
            if t["last_hint"] in last.lower():
                marker = " ◄◄◄ TARGET"
        print(f"  {row_str}{marker}")

    # 5. Cross-check: GET stored IDs' company — might still be on district
    print("\n" + "=" * 60)
    print("5. WHERE are they right now in NS?")
    print("=" * 60)
    for key, row in target_rows.items():
        cid = str(row.get("NS Contact ID", "")).strip()
        if not cid or not cid.isdigit():
            continue
        c = get_contact(cid)
        comp_id = str((c.get("company") or {}).get("id") or "")
        comp_name = str((c.get("company") or {}).get("refName") or "")
        print(f"  {key.title()}: company={comp_id} ({comp_name}), "
              f"inactive={c.get('isInactive')}")
        if comp_id == HS_ID:
            print("  → Already on HS 2217 ✓")
        elif comp_id == DISTRICT_ID:
            print("  → Still on District 2290 — needs repoint")
        else:
            print(f"  → On UNKNOWN customer {comp_id} — unexpected")


if __name__ == "__main__":
    main()
