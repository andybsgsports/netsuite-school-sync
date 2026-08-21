"""
fix_pecatonica_split.py — one-time: untangle the two schools that share the
name "Pecatonica High School".

The Schools tab holds BOTH Pecatonica High School, Blanchardville WI (WIAA,
NS customer 2833, Jeff Howard) AND Pecatonica High School, Pecatonica IL
(IHSA, NS customer 2885, Andrew Murray) under the identical School Name.
Everything downstream keys contacts by School Name, so the two schools'
staff merged into one blended roster, and each rep's nightly job pushed the
whole blend to ITS customer — thrashing NS Contact IDs, creating stray
cross-school contact records (WI people on the IL customer and vice versa),
and doubling names in the email-recipient picker.

What this does:
  1. Renames the IL row's School Name to "Pecatonica High School (IL)"
     (Full Name is untouched, so the NetSuite-facing customer name doesn't
     change).
  2. Re-scrapes both real rosters (WIAA page, IHSA API) and splits the
     blended Contacts-tab rows between the two school names, repointing
     NS Customer ID and re-linking NS Contact ID by email against each
     customer's own contact records. Rows on neither roster are marked
     Sync=N (departed during the blend).
  3. Retires stray cross-school contact records in NS: an ACTIVE contact
     whose company is one Pecatonica but whose email belongs only to the
     other school's roster gets the merge-style retirement (lastName
     "(dup {id})", isInactive) and its Ship-To line removed.
  4. Detaches the OTHER school's shared cards from each customer (the July
     co-op merge attached blended people to both customers).

DRY RUN by default; set LIVE=1 to apply. Aborts if either roster scrape
returns fewer than MIN_ROSTER people (a broken scrape must not partition).
"""
from __future__ import annotations

import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from netsuite_sync import (
    scrape_wiaa_school_detail, ns_suiteql, ns_patch,
    remove_contact_ship_to, ns_restlet_attach, restlet_available,
)
from ihsa_sync import scrape_school, extract_school_id
from school_netsuite_sync import (
    get_gspread_client, GOOGLE_SHEET_ID, MASTER_TAB,
    M_NAME, M_STATE, M_URL, M_NS_ID,
    load_contacts, save_contacts,
    C_SCHOOL, C_FIRST, C_LAST, C_EMAIL, C_ROLE, C_TYPE,
    C_SYNC, C_NS_CID, C_NS_CUS, C_SYNCED, C_HASH,
)

LIVE = os.environ.get("LIVE", "").strip() in ("1", "true", "True", "yes")
OLD_NAME = "Pecatonica High School"
IL_NAME = "Pecatonica High School (IL)"
MIN_ROSTER = 5


def ns_roster(customer_id):
    """{email_lower: contact_id} for ACTIVE contacts whose company is the
    customer, plus a parallel {contact_id: (name, email)} for reporting."""
    by_email, detail = {}, {}
    for row in ns_suiteql(
            f"SELECT id, firstname, lastname, email FROM contact "
            f"WHERE company = {customer_id} "
            f"AND (isinactive = 'F' OR isinactive IS NULL)", limit=1000):
        cid = str(row.get("id"))
        em = (row.get("email") or "").strip().lower()
        nm = f"{(row.get('firstname') or '').strip()} {(row.get('lastname') or '').strip()}".strip()
        detail[cid] = (nm, em)
        if em and em not in by_email:
            by_email[em] = cid
    return by_email, detail


def main():
    print("=" * 70)
    print(f"  PECATONICA SPLIT  |  LIVE={LIVE}")
    print("=" * 70)

    gc = get_gspread_client()
    wb = gc.open_by_key(GOOGLE_SHEET_ID)
    schools_ws = wb.worksheet(MASTER_TAB)
    values = schools_ws.get_all_values()
    headers = values[0]
    i_name = headers.index(M_NAME)
    i_state = headers.index(M_STATE)
    i_url = headers.index(M_URL)
    i_ns = headers.index(M_NS_ID)

    wi_row = il_row = None          # (sheet_row_number, url, ns_id)
    for rn, raw in enumerate(values[1:], start=2):
        name = raw[i_name].strip() if i_name < len(raw) else ""
        if name not in (OLD_NAME, IL_NAME):
            continue
        state = raw[i_state].strip().upper() if i_state < len(raw) else ""
        url = raw[i_url].strip() if i_url < len(raw) else ""
        ns = re.sub(r"\.0$", "", raw[i_ns].strip()) if i_ns < len(raw) else ""
        if state == "IL":
            il_row = (rn, url, ns, name)
        else:
            wi_row = (rn, url, ns, name)
    if not wi_row or not il_row:
        print(f"ABORT: expected both a WI and an IL Pecatonica row "
              f"(WI={wi_row}, IL={il_row})")
        sys.exit(1)
    print(f"WI row {wi_row[0]}: NS {wi_row[2]}  {wi_row[1][:60]}")
    print(f"IL row {il_row[0]}: NS {il_row[2]}  {il_row[1][:60]} "
          f"(currently named {il_row[3]!r})")
    wi_cust, il_cust = wi_row[2], il_row[2]

    # --- Real rosters -----------------------------------------------------
    print("\nScraping WIAA (WI roster)...")
    _info, wi_admins, wi_coaches = scrape_wiaa_school_detail(wi_row[1])
    # scrape_wiaa_school_detail returns lowercase keys ("email"), unlike
    # rep_digests' scrape_rep records ("Email").
    wi_emails = {str(p.get("email", "")).strip().lower()
                 for p in (wi_admins + wi_coaches) if str(p.get("email", "")).strip()}
    print(f"  WI roster: {len(wi_emails)} distinct emails")

    print("Scraping IHSA (IL roster)...")
    il_people = scrape_school(extract_school_id(il_row[1]))
    il_emails = {str(p.get("email", "")).strip().lower()
                 for p in il_people if str(p.get("email", "")).strip()}
    print(f"  IL roster: {len(il_emails)} distinct emails")

    if len(wi_emails) < MIN_ROSTER or len(il_emails) < MIN_ROSTER:
        print("ABORT: a roster came back too small — refusing to partition "
              "on a possibly-failed scrape.")
        sys.exit(1)
    both = wi_emails & il_emails
    if both:
        print(f"  NOTE: {len(both)} email(s) on BOTH rosters: {sorted(both)[:5]}")

    # --- NS contact rosters ----------------------------------------------
    wi_by_email, wi_detail = ns_roster(wi_cust)
    il_by_email, il_detail = ns_roster(il_cust)
    print(f"\nNS active contacts: WI({wi_cust})={len(wi_detail)}  "
          f"IL({il_cust})={len(il_detail)}")

    # --- Split the blended Contacts rows ---------------------------------
    contacts_data, contacts_ws = load_contacts(gc)
    moved_il = kept_wi = departed = 0
    for c in contacts_data:
        if str(c.get(C_SCHOOL, "")).strip() not in (OLD_NAME, IL_NAME):
            continue
        em = str(c.get(C_EMAIL, "")).strip().lower()
        who = f"{c.get(C_FIRST, '')} {c.get(C_LAST, '')}".strip()
        if em in il_emails and em not in wi_emails:
            c[C_SCHOOL] = IL_NAME
            c[C_NS_CUS] = il_cust
            c[C_NS_CID] = il_by_email.get(em, "")
            c[C_HASH] = ""
            moved_il += 1
            print(f"  -> IL: {who} <{em}>  (CID {c[C_NS_CID] or 'to create'})")
        elif em in wi_emails:
            c[C_SCHOOL] = OLD_NAME
            c[C_NS_CUS] = wi_cust
            c[C_NS_CID] = wi_by_email.get(em, "")
            c[C_HASH] = ""
            kept_wi += 1
        else:
            # On neither roster: departed while the schools were blended.
            # Guess the side by domain for reporting; NS retirement below is
            # roster-driven so a wrong guess only affects which name the
            # dead row sits under.
            c[C_SCHOOL] = IL_NAME if "pecschools.com" in em else OLD_NAME
            c[C_SYNC] = "N"
            c[C_NS_CID] = ""
            c[C_HASH] = ""
            departed += 1
            print(f"  -> departed (Sync=N): {who} <{em}>")
    print(f"\nSheet split: {kept_wi} stay WI, {moved_il} move to {IL_NAME!r}, "
          f"{departed} marked departed")

    # --- Stray cross-school NS records to retire --------------------------
    def strays(detail, own_roster):
        out = []
        for cid, (nm, em) in detail.items():
            if em and em not in own_roster:
                out.append((cid, nm, em))
        return out

    il_strays = strays(il_detail, il_emails)
    wi_strays = strays(wi_detail, wi_emails)
    print(f"\nStray records on IL customer {il_cust} (not on IL roster): "
          f"{len(il_strays)}")
    for cid, nm, em in il_strays:
        print(f"    {cid:<8} {nm:<26} {em}")
    print(f"Stray records on WI customer {wi_cust} (not on WI roster): "
          f"{len(wi_strays)}")
    for cid, nm, em in wi_strays:
        print(f"    {cid:<8} {nm:<26} {em}")

    # Shared cards attached to the WRONG customer by the July merge:
    # detach every id that (post-split) belongs only to the other school.
    wi_ids = {str(c.get(C_NS_CID, "")).strip() for c in contacts_data
              if str(c.get(C_SCHOOL, "")).strip() == OLD_NAME
              and str(c.get(C_NS_CID, "")).strip().isdigit()}
    il_ids = {str(c.get(C_NS_CID, "")).strip() for c in contacts_data
              if str(c.get(C_SCHOOL, "")).strip() == IL_NAME
              and str(c.get(C_NS_CID, "")).strip().isdigit()}
    detach_from_il = sorted(wi_ids - il_ids)
    detach_from_wi = sorted(il_ids - wi_ids)
    print(f"\nDetach from IL customer: {len(detach_from_il)} WI card(s); "
          f"detach from WI customer: {len(detach_from_wi)} IL card(s)")

    if not LIVE:
        print("\nDRY RUN — nothing changed. Set LIVE=1 to apply.")
        return

    # --- Apply -------------------------------------------------------------
    print("\nAPPLYING...")
    schools_ws.update_cell(il_row[0], i_name + 1, IL_NAME)
    print(f"  Schools tab row {il_row[0]}: School Name -> {IL_NAME!r}")
    save_contacts(contacts_ws, contacts_data)
    print("  Contacts tab saved")

    for cust, lst in ((il_cust, il_strays), (wi_cust, wi_strays)):
        for cid, nm, em in lst:
            last = nm.split(" ", 1)[1] if " " in nm else nm
            new_last = f"{last} (dup {cid})"[:80]
            r = ns_patch(f"contact/{cid}",
                         {"lastName": new_last, "isInactive": True})
            if r.status_code == 204:
                print(f"  retired stray {cid} ({nm}) on customer {cust}")
            else:
                print(f"  WARN: couldn't retire {cid}: {r.status_code} "
                      f"{r.text[:120]}")
            remove_contact_ship_to(cust, nm)
            time.sleep(0.2)

    if restlet_available():
        for cid in detach_from_il:
            ns_restlet_attach(cid, il_cust, "detach")
            time.sleep(0.2)
        for cid in detach_from_wi:
            ns_restlet_attach(cid, wi_cust, "detach")
            time.sleep(0.2)
    else:
        print("  WARN: RESTlet unavailable — skipped detach pass")

    print("\nDONE. Next nightly runs will re-link/create any still-blank "
          "Contact IDs under the now-distinct school names.")


if __name__ == "__main__":
    main()
