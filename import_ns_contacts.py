"""
import_ns_contacts.py — one-time backfill of existing NetSuite contacts
into the Contacts tab of the School Sync Master sheet.

For each school in the Schools tab that has an NS Customer ID, enumerates
every active contact linked to that customer in NetSuite (via the
contactRoles sublist, which NS auto-populates for primary-company links
too) and appends any that aren't already in the Contacts tab.

Imported rows land with Sync=N so the daily WI/IL syncs leave them alone
— they won't get inactivated just because they aren't on the WIAA/IHSA
scrape. Andy can flip Sync to Y on individual rows if he wants scraped
updates to own them.

Deploy as a manual-dispatch GitHub workflow. Safe to re-run: dedupes on
NS Contact ID and on (school, email, role).
"""
from __future__ import annotations

import os
import sys
import time

import gspread

from netsuite_sync import ns_get
from school_netsuite_sync import (
    get_gspread_client,
    load_contacts, save_contacts,
    GOOGLE_SHEET_ID, MASTER_TAB,
    M_NAME, M_NS_ID, M_STATE,
    C_SCHOOL, C_FIRST, C_LAST, C_EMAIL, C_ROLE, C_TYPE,
    C_SYNC, C_NS_CID, C_NS_CUS, C_SYNCED,
)


SCHOOL_FILTER = os.environ.get("SCHOOL_FILTER", "").strip()
STATE_FILTER  = os.environ.get("STATE_FILTER", "").strip().upper()  # "WI", "IL", or "" for all
ALL_CUSTOMERS = os.environ.get("ALL_CUSTOMERS", "").strip().lower() in ("1", "true", "yes", "y")


def load_all_schools(gc):
    """All schools with an NS Customer ID, regardless of state."""
    wb = gc.open_by_key(GOOGLE_SHEET_ID)
    ws = wb.worksheet(MASTER_TAB)
    values = ws.get_all_values()
    if not values:
        return []
    headers = values[0]
    out = []
    for raw in values[1:]:
        rec = dict(zip(headers, raw))
        name  = str(rec.get(M_NAME, "")).strip()
        ns_id = str(rec.get(M_NS_ID, "")).strip()
        state = str(rec.get(M_STATE, "")).strip().upper()
        if not name or ns_id in ("", "nan", "None", "0"):
            continue
        if STATE_FILTER and state != STATE_FILTER:
            continue
        if SCHOOL_FILTER and name != SCHOOL_FILTER:
            continue
        out.append({"name": name, "ns_id": ns_id, "state": state})
    return out


def _collect_contact_ids_via_contact_roles(customer_id):
    ids = []
    r = ns_get(f"customer/{customer_id}?expand=contactRoles")
    if r.status_code != 200:
        return ids
    items = r.json().get("contactRoles", {}).get("items", [])
    for item in items:
        cid = (item.get("contact") or {}).get("id")
        if cid:
            ids.append(str(cid))
            continue
        href = (item.get("links") or [{}])[0].get("href", "")
        line_id = href.rstrip("/").split("/")[-1] if href else None
        if not line_id:
            continue
        r2 = ns_get(f"customer/{customer_id}/contactRoles/{line_id}")
        if r2.status_code == 200:
            cid = (r2.json().get("contact") or {}).get("id")
            if cid:
                ids.append(str(cid))
    return ids


def _collect_contact_ids_via_contact_list(customer_id):
    """Contacts whose primary `company` = this customer. Many schools have
    contacts only reachable via this path (not contactRoles)."""
    ids = []
    r = ns_get(f"customer/{customer_id}?expand=contactList")
    if r.status_code != 200:
        return ids
    items = r.json().get("contactList", {}).get("items", [])
    for item in items:
        fields = item.get("fields", item)
        cid = (fields.get("contact") or {}).get("id") or fields.get("id")
        if cid:
            ids.append(str(cid))
            continue
        href = (item.get("links") or [{}])[0].get("href", "")
        line_id = href.rstrip("/").split("/")[-1] if href else None
        if line_id:
            ids.append(str(line_id))
    return ids


def _collect_contact_ids_via_search(customer_id):
    """REST contact search: /contact?q=company EQ <id>. May be blocked by
    role permissions — returns [] silently on failure."""
    ids = []
    offset = 0
    while True:
        r = ns_get(f"contact?q=company EQ {customer_id}&limit=100&offset={offset}")
        if r.status_code != 200:
            return ids
        body = r.json()
        items = body.get("items", [])
        for item in items:
            cid = item.get("id")
            href = (item.get("links") or [{}])[0].get("href", "")
            if not cid and href:
                cid = href.rstrip("/").split("/")[-1]
            if cid:
                ids.append(str(cid))
        if not body.get("hasMore"):
            break
        offset += 100
    return ids


def fetch_contacts_for_customer(customer_id):
    """Union of three enumeration paths so we don't miss contacts that are
    linked only via primary company field or only via the sublist."""
    seen = set()
    sources = []
    for fn, label in (
        (_collect_contact_ids_via_contact_roles, "contactRoles"),
        (_collect_contact_ids_via_contact_list,  "contactList"),
        (_collect_contact_ids_via_search,        "search"),
    ):
        ids = fn(customer_id)
        new = [i for i in ids if i not in seen]
        seen.update(new)
        sources.append(f"{label}={len(ids)}")
    if sources:
        print(f"    sources: {', '.join(sources)}")

    out = []
    for cid in seen:
        r3 = ns_get(
            f"contact/{cid}?fields=firstName,lastName,email,title,isInactive,externalId"
        )
        if r3.status_code != 200:
            continue
        c = r3.json()
        if c.get("isInactive"):
            continue
        out.append({
            "id":        cid,
            "first":     (c.get("firstName") or "").strip(),
            "last":      (c.get("lastName") or "").strip(),
            "email":     (c.get("email") or "").strip(),
            "role":      (c.get("title") or "").strip(),
            "externalId": c.get("externalId") or "",
        })
    return out


def fetch_all_customers():
    """Paginate every customer in NS, returning [{id, name}]. Used by
    ALL_CUSTOMERS mode to reach schools that aren't in the Schools tab
    (e.g. other salesmen's accounts)."""
    out = []
    offset = 0
    while True:
        r = ns_get(f"customer?limit=1000&offset={offset}&fields=id,companyName,entityId")
        if r.status_code != 200:
            print(f"  [NS] list customers failed at offset {offset}: "
                  f"{r.status_code} {r.text[:120]}")
            break
        body = r.json()
        items = body.get("items", [])
        for item in items:
            cid = item.get("id")
            href = (item.get("links") or [{}])[0].get("href", "")
            if not cid and href:
                cid = href.rstrip("/").split("/")[-1]
            if not cid:
                continue
            name = item.get("companyName") or item.get("entityId") or ""
            # Inline name may be absent — fetch per-customer if missing
            if not name:
                r2 = ns_get(f"customer/{cid}?fields=companyName,entityId")
                if r2.status_code == 200:
                    data = r2.json()
                    name = data.get("companyName") or data.get("entityId") or f"Customer {cid}"
                else:
                    name = f"Customer {cid}"
            out.append({"id": str(cid), "name": str(name).strip()})
        if not body.get("hasMore"):
            break
        offset += 1000
        print(f"  ...paged {len(out)} customers so far")
    return out


def main():
    print(f"{'=' * 60}")
    print(f"  IMPORT NS CONTACTS -> CONTACTS TAB")
    if ALL_CUSTOMERS: print(f"  ALL_CUSTOMERS mode: every NS customer (ignores Schools tab)")
    if STATE_FILTER:  print(f"  STATE_FILTER: {STATE_FILTER}")
    if SCHOOL_FILTER: print(f"  SCHOOL_FILTER: {SCHOOL_FILTER}")
    print(f"{'=' * 60}\n")

    gc = get_gspread_client()
    if ALL_CUSTOMERS:
        customers = fetch_all_customers()
        schools = [{"name": c["name"], "ns_id": c["id"], "state": ""} for c in customers]
    else:
        schools = load_all_schools(gc)
    contacts_data, contacts_ws = load_contacts(gc)

    existing_ns_ids = {
        str(c.get(C_NS_CID, "")).strip()
        for c in contacts_data
        if str(c.get(C_NS_CID, "")).strip() not in ("", "UNLINKED", "nan", "None")
    }
    existing_keys = {
        (str(c.get(C_SCHOOL, "")).strip(),
         str(c.get(C_EMAIL, "")).strip().lower(),
         str(c.get(C_ROLE, "")).strip().lower())
        for c in contacts_data
        if str(c.get(C_EMAIL, "")).strip()
    }

    print(f"  Schools to scan: {len(schools)}")
    print(f"  Existing contacts in sheet: {len(contacts_data)}\n")

    added = 0
    scanned = 0
    for sch in schools:
        scanned += 1
        print(f"[{scanned}/{len(schools)}] {sch['name']} ({sch['state']})  NS {sch['ns_id']}")
        found = fetch_contacts_for_customer(sch["ns_id"])
        print(f"    NS has {len(found)} active contacts")

        for p in found:
            email = p["email"].lower()
            role  = p["role"].lower()
            if not p["email"]:
                continue
            if p["id"] in existing_ns_ids:
                continue
            if (sch["name"], email, role) in existing_keys:
                continue

            contacts_data.append({
                C_SCHOOL: sch["name"],
                C_FIRST:  p["first"],
                C_LAST:   p["last"],
                C_EMAIL:  p["email"],
                C_ROLE:   p["role"],
                C_TYPE:   "",
                C_SYNC:   "N",
                C_NS_CID: p["id"],
                C_NS_CUS: sch["ns_id"],
                C_SYNCED: "",
            })
            existing_ns_ids.add(p["id"])
            existing_keys.add((sch["name"], email, role))
            added += 1
            print(f"    + Imported: {p['first']} {p['last']} — {p['role']}")

        time.sleep(0.2)

    print(f"\n{'=' * 60}")
    print(f"  IMPORT COMPLETE")
    print(f"  Schools scanned: {scanned}")
    print(f"  Contacts imported: {added}")
    print(f"{'=' * 60}")

    if added:
        save_contacts(contacts_ws, contacts_data)


if __name__ == "__main__":
    main()
