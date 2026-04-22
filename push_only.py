"""
push_only.py — push Contacts tab rows to NetSuite without scraping.

Reads the Contacts tab (already populated by the daily scrape) and
syncs each row with Sync=Y to NetSuite:
  - creates new contacts (no NS Contact ID yet) via sync_contact
  - updates existing ones (so title / email / names stay fresh)
  - inactivates rows flipped to Sync=N
  - refreshes the customer's Sales Team + Ship-To addressBook

Scope via SALES_REP_FILTER env var — one rep's schools only. Designed
for per-rep parallel invocations after the nightly scrape (one job
per rep; one rep's error doesn't block the others).

Env:
  GOOGLE_SHEET_ID, GOOGLE_CREDENTIALS_JSON, NS_* tokens
  SALES_REP_FILTER  - required for per-rep mode (blank = all reps)
  STATE_FILTER      - 'WI', 'IL', or blank for all
  SCHOOL_FILTER     - optional single-school testing
"""
from __future__ import annotations

import hashlib
import os
import sys
import time
from datetime import datetime

import gspread


def row_hash(first, last, email, role):
    """Stable hash of the fields that matter to NS. Used to skip rows that
    haven't changed since the last push."""
    s = f"{first.strip().lower()}|{last.strip().lower()}|{email.strip().lower()}|{role.strip().lower()}"
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:16]

from netsuite_sync import (
    sync_school, sync_contact, inactivate_contact,
    remove_contact_ship_to, sync_address_book, compute_school_domain,
)
from school_netsuite_sync import (
    get_gspread_client,
    load_contacts, save_contacts,
    GOOGLE_SHEET_ID, MASTER_TAB,
    M_NAME, M_URL, M_NS_ID, M_SALES, M_STATE, M_LOCKED, M_SYNCED,
    C_SCHOOL, C_FIRST, C_LAST, C_EMAIL, C_ROLE, C_TYPE,
    C_SYNC, C_NS_CID, C_NS_CUS, C_SYNCED, C_HASH,
)

SCHOOL_FILTER    = os.environ.get("SCHOOL_FILTER", "").strip()
STATE_FILTER     = os.environ.get("STATE_FILTER", "").strip().upper()
SALES_REP_FILTER = os.environ.get("SALES_REP_FILTER", "").strip()
DELAY = 0.3  # much lower than scrape-included sync; no WIAA throttling needed


def load_schools(gc):
    wb = gc.open_by_key(GOOGLE_SHEET_ID)
    ws = wb.worksheet(MASTER_TAB)
    values = ws.get_all_values()
    if not values:
        return [], ws, None
    headers = values[0]
    synced_col = headers.index(M_SYNCED) + 1 if M_SYNCED in headers else None
    out = []
    for i, raw in enumerate(values[1:], start=2):
        rec = dict(zip(headers, raw))
        name  = str(rec.get(M_NAME, "")).strip()
        ns_id = str(rec.get(M_NS_ID, "")).strip()
        url   = str(rec.get(M_URL, "")).strip()
        state = str(rec.get(M_STATE, "")).strip().upper()
        rep   = str(rec.get(M_SALES, "")).strip()
        locked = str(rec.get(M_LOCKED, "")).strip().upper() == "Y"
        if not name or locked:
            continue
        if ns_id in ("", "nan", "None", "0"):
            continue
        if SCHOOL_FILTER and name != SCHOOL_FILTER:
            continue
        if STATE_FILTER and state != STATE_FILTER:
            continue
        if SALES_REP_FILTER and rep.lower() != SALES_REP_FILTER.lower():
            continue
        out.append({"row": i, "name": name, "ns_id": ns_id, "url": url,
                    "state": state, "rep": rep})
    return out, ws, synced_col


def main():
    print("=" * 60)
    print(f"  PUSH ONLY  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    if SALES_REP_FILTER: print(f"  SALES_REP_FILTER: {SALES_REP_FILTER}")
    if STATE_FILTER:     print(f"  STATE_FILTER: {STATE_FILTER}")
    if SCHOOL_FILTER:    print(f"  SCHOOL_FILTER: {SCHOOL_FILTER}")
    print("=" * 60)

    if not GOOGLE_SHEET_ID:
        print("ERROR: GOOGLE_SHEET_ID env var not set.")
        sys.exit(1)

    gc = get_gspread_client()
    schools, master_ws, synced_col = load_schools(gc)
    contacts_data, contacts_ws = load_contacts(gc)

    print(f"  Schools in scope: {len(schools)}")
    print(f"  Contacts tab rows: {len(contacts_data)}\n")

    synced_schools = 0
    errors = 0
    synced_updates = []

    for sch in schools:
        school_name = sch["name"]
        ns_id       = sch["ns_id"]
        rep         = sch["rep"]
        state       = sch["state"]

        print(f"\n[{school_name}]  NS {ns_id}  (rep: {rep})")

        school_contacts = [c for c in contacts_data
                           if c.get(C_SCHOOL, "").strip() == school_name]
        if not school_contacts:
            print(f"  (no rows on Contacts tab)")
            continue

        # Update Customer (name/domain/sales team)
        try:
            result_id, school_info_out, _, _ = sync_school(
                school_name=school_name,
                school_url=sch["url"],
                state=state or "WI",
                sync_contacts=[],
                sales_rep=rep or None,
                ns_customer_id=ns_id,
            )
        except Exception as e:
            print(f"  ERROR syncing customer: {e}")
            errors += 1
            time.sleep(DELAY)
            continue
        if not result_id:
            print(f"  Could not sync Customer — skipping contacts")
            errors += 1
            continue

        synced_schools += 1
        synced_updates.append((sch["row"], datetime.now().strftime("%Y-%m-%d %H:%M")))

        # Compute school domain for home-school detection
        sync_y = [
            {"email": str(c.get(C_EMAIL, "")).strip()}
            for c in school_contacts
            if str(c.get(C_SYNC, "N")).strip().upper() == "Y"
            and str(c.get(C_EMAIL, "")).strip()
        ]
        school_info_out["domain"] = compute_school_domain(sync_y)
        if school_info_out["domain"]:
            print(f"  School domain: {school_info_out['domain']}")

        # Dedupe: one coach with multiple sports = multiple sheet rows but
        # should be one NS PATCH. Key by email (same person); carry the NS
        # Contact ID forward so we don't re-PATCH the same record.
        pushed_emails = {}    # email_lower -> ns_contact_id (after sync)
        for c in school_contacts:
            sync_flag  = str(c.get(C_SYNC, "N")).strip().upper()
            first      = str(c.get(C_FIRST, "")).strip()
            last       = str(c.get(C_LAST, "")).strip()
            email      = str(c.get(C_EMAIL, "")).strip()
            role       = str(c.get(C_ROLE, "")).strip()
            contact_ns = str(c.get(C_NS_CID, "")).strip()
            if not email:
                continue
            c[C_NS_CUS] = str(result_id)
            em_key = email.lower()

            if sync_flag == "Y":
                if contact_ns == "UNLINKED":
                    continue
                if em_key in pushed_emails:
                    # Same person, different sport row — reuse the known NS ID
                    if pushed_emails[em_key]:
                        c[C_NS_CID] = str(pushed_emails[em_key])
                        c[C_SYNCED] = datetime.now().strftime("%Y-%m-%d %H:%M")
                        c[C_HASH]   = row_hash(first, last, email, role)
                    continue

                # Change detection: skip if already in NS with matching hash
                current_hash = row_hash(first, last, email, role)
                stored_hash  = str(c.get(C_HASH, "")).strip()
                if contact_ns and contact_ns not in ("nan", "None") and stored_hash == current_hash:
                    # No change since last push — skip the NS PATCH
                    pushed_emails[em_key] = contact_ns
                    continue

                new_id = sync_contact(result_id, school_name, {
                    "first": first, "last": last,
                    "email": email, "role": role,
                    "ns_id": contact_ns if contact_ns not in ("", "nan", "None") else "",
                }, school_info_out)
                if new_id:
                    c[C_NS_CID] = str(new_id)
                    c[C_SYNCED] = datetime.now().strftime("%Y-%m-%d %H:%M")
                    c[C_HASH]   = current_hash
                    pushed_emails[em_key] = str(new_id)
                elif new_id is None and not contact_ns:
                    c[C_NS_CID] = "UNLINKED"
                    pushed_emails[em_key] = ""
                time.sleep(0.15)
            elif sync_flag == "N" and contact_ns not in ("", "nan", "None", "UNLINKED"):
                if em_key not in pushed_emails:
                    inactivate_contact(contact_ns, f"{first} {last}")
                    pushed_emails[em_key] = ""
                c[C_NS_CID] = ""
                time.sleep(0.15)

        # Ship-To addresses for active contacts at this school
        active_contacts = [
            {"first": str(c.get(C_FIRST, "")).strip(),
             "last":  str(c.get(C_LAST, "")).strip(),
             "email": str(c.get(C_EMAIL, "")).strip(),
             "role":  str(c.get(C_ROLE, "")).strip()}
            for c in school_contacts
            if str(c.get(C_SYNC, "N")).strip().upper() == "Y"
        ]
        if active_contacts and school_info_out:
            sync_address_book(result_id, school_info_out, active_contacts,
                              school_name=school_name)

        time.sleep(DELAY)

    # Save back to sheet
    save_contacts(contacts_ws, contacts_data)
    if synced_col and synced_updates:
        batch = [{
            "range": gspread.utils.rowcol_to_a1(row, synced_col),
            "values": [[ts]],
        } for row, ts in synced_updates]
        master_ws.batch_update(batch)

    print(f"\n{'=' * 60}")
    print(f"  PUSH COMPLETE")
    print(f"  Schools pushed: {synced_schools}")
    print(f"  Errors:         {errors}")
    print(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)


if __name__ == "__main__":
    main()
