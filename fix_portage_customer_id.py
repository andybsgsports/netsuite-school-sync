"""
fix_portage_customer_id.py — one-time fix for a wrong NS Customer ID on
Portage in the Schools tab (was 2823 "Portage High School", should be
2838 "Portage Senior High School").

Steps:
  1. Inactivate every NS contact that today's sync created under the
     wrong customer (2823) — identified via Contacts tab rows where
     school=Portage and NS Customer ID=2823.
  2. Remove those rows from the Contacts tab so the next daily sync
     re-scrapes Portage cleanly and creates fresh contact records
     under 2838.
  3. Update the Schools tab Portage row's NS Customer ID from 2823
     to 2838.
"""
from __future__ import annotations

import os

from netsuite_sync import ns_patch
from school_netsuite_sync import (
    get_gspread_client, load_contacts, save_contacts,
    GOOGLE_SHEET_ID, MASTER_TAB,
    M_NAME, M_NS_ID,
    C_SCHOOL, C_NS_CID, C_NS_CUS,
)

SCHOOL_NAME = os.environ.get("SCHOOL_NAME", "Portage").strip()
OLD_ID      = os.environ.get("OLD_NS_ID", "2823").strip()
NEW_ID      = os.environ.get("NEW_NS_ID", "2838").strip()


def main():
    print(f"Fixing {SCHOOL_NAME}: NS Customer ID {OLD_ID} -> {NEW_ID}\n")

    gc = get_gspread_client()
    contacts_data, contacts_ws = load_contacts(gc)

    bad_rows = [c for c in contacts_data
                if c.get(C_SCHOOL, "").strip() == SCHOOL_NAME
                and str(c.get(C_NS_CUS, "")).strip() == OLD_ID]
    print(f"Step 1/3 — Inactivating {len(bad_rows)} NS contacts under customer {OLD_ID}")

    inactivated = 0
    for c in bad_rows:
        nsid = str(c.get(C_NS_CID, "")).strip()
        if not nsid or nsid in ("UNLINKED", "nan", "None"):
            continue
        r = ns_patch(f"contact/{nsid}", {"isInactive": True})
        if r.status_code in (200, 204):
            inactivated += 1
            print(f"    - Inactivated {c.get('First','')} {c.get('Last','')} (NS {nsid})")
        else:
            print(f"    ! {nsid}: {r.status_code} {r.text[:120]}")
    print(f"  Inactivated {inactivated}/{len(bad_rows)}\n")

    print(f"Step 2/3 — Removing {len(bad_rows)} rows from Contacts tab")
    remaining = [c for c in contacts_data
                 if not (c.get(C_SCHOOL, "").strip() == SCHOOL_NAME
                         and str(c.get(C_NS_CUS, "")).strip() == OLD_ID)]
    save_contacts(contacts_ws, remaining)
    print(f"  Contacts tab: {len(contacts_data)} -> {len(remaining)}\n")

    print(f"Step 3/3 — Updating Schools tab NS Customer ID")
    wb = gc.open_by_key(GOOGLE_SHEET_ID)
    ws = wb.worksheet(MASTER_TAB)
    values = ws.get_all_values()
    headers = values[0]
    if M_NAME not in headers or M_NS_ID not in headers:
        print(f"  ! Schools tab missing expected columns")
        return
    name_col = headers.index(M_NAME)   # 0-based
    ns_col   = headers.index(M_NS_ID)  # 0-based
    updated = False
    for i, row in enumerate(values[1:], start=2):
        if len(row) <= max(name_col, ns_col):
            continue
        if row[name_col].strip() == SCHOOL_NAME:
            cur = row[ns_col].strip()
            ws.update_cell(i, ns_col + 1, NEW_ID)
            print(f"  Row {i}: {SCHOOL_NAME} NS Customer ID {cur} -> {NEW_ID}")
            updated = True
            break
    if not updated:
        print(f"  ! Couldn't find '{SCHOOL_NAME}' row in Schools tab")
        return

    print(f"\nDone. Next daily sync will scrape {SCHOOL_NAME} and create fresh "
          f"contacts under customer {NEW_ID}.")


if __name__ == "__main__":
    main()
