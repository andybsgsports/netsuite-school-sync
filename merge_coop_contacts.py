"""
merge_coop_contacts.py — one-time consolidation of co-op coaches' per-school
duplicate NS contact records into a single shared card per person.

Background: NetSuite's REST API can't write a customer's contactRoles (static
sublist), so the sync historically created a separate contact record per
(school, person) — e.g. Blake Panosh five times. The attach RESTlet
(suitescript/attach_contact_restlet.js) unlocks NetSuite's native sharing
(the UI's "Attach" button). This script migrates existing data to that model:

For each person (email) with Sync=Y rows at 2+ schools:
  1. Pick their PRIMARY record = the lowest (oldest) NS contact internal id
     among the group's stored, still-existing ids.
  2. Every other record in the group is a duplicate:
       - rename it (lastName += " (dup NNN)") to free NetSuite's per-customer
         unique-name space, and inactivate it — one PATCH.
  3. Attach the primary card to every school's customer in the group
     (skipped where already attached / where it's the primary company).
  4. Repoint every sheet row in the group: NS Contact ID -> primary id, and
     clear Content Hash so the next nightly push refreshes the card's fields.

Ship-To address lines are left alone: their labels are the person's name,
which is identical on the shared card.

DRY-RUN by default — prints the full plan. Set LIVE=1 (or --live) to apply.
Optional EMAIL_FILTER=someone@x.com (or --email) limits to one person for a
careful first test.

Requires the RESTlet to be deployed and NS_RESTLET_SCRIPT_ID /
NS_RESTLET_DEPLOY_ID set (see RESTLET_SETUP.md) — aborts otherwise.
"""
from __future__ import annotations

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from netsuite_sync import (
    ns_get, ns_patch,
    restlet_available, ensure_attached, ns_restlet_health,
)
from school_netsuite_sync import (
    get_gspread_client, load_contacts, save_contacts,
    GOOGLE_SHEET_ID, MASTER_TAB, M_NAME, M_NS_ID,
    C_SCHOOL, C_FIRST, C_LAST, C_EMAIL, C_SYNC, C_NS_CID, C_HASH,
)


def load_school_customer_map(gc):
    """School Name -> NS Customer ID from the Schools tab."""
    ws = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(MASTER_TAB)
    out = {}
    for r in ws.get_all_records():
        name = str(r.get(M_NAME, "")).strip()
        cid = str(r.get(M_NS_ID, "")).strip()
        if name and cid.isdigit():
            out[name] = cid
    return out


def get_contact(contact_id):
    r = ns_get(f"contact/{contact_id}")
    return r.json() if r.status_code == 200 else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true",
                        help="Apply changes. Default is dry-run.")
    parser.add_argument("--email", default="",
                        help="Limit to one person (email, exact match).")
    args = parser.parse_args()
    live = args.live or os.environ.get("LIVE", "") == "1"
    email_filter = (args.email or os.environ.get("EMAIL_FILTER", "")).strip().lower()

    print("=" * 66)
    print(f"  Merge co-op duplicate contacts into shared cards — "
          f"{'LIVE' if live else 'DRY RUN'}")
    if email_filter:
        print(f"  EMAIL_FILTER: {email_filter}")
    print("=" * 66)

    if not restlet_available():
        print("\nABORT: attach RESTlet not configured "
              "(NS_RESTLET_SCRIPT_ID / NS_RESTLET_DEPLOY_ID unset). "
              "Deploy it first — see RESTLET_SETUP.md.")
        sys.exit(1)

    ok, detail = ns_restlet_health()
    if not ok:
        print(f"\nABORT: attach RESTlet health check FAILED: {detail}")
        sys.exit(1)
    print("\nRESTlet health check: OK (reachable with the sync's credentials)")

    gc = get_gspread_client()
    school_cust = load_school_customer_map(gc)
    contacts_data, contacts_ws = load_contacts(gc)

    # Group Sync=Y rows by email across schools
    groups = {}  # email_lower -> list of row dicts
    for c in contacts_data:
        if str(c.get(C_SYNC, "N")).strip().upper() != "Y":
            continue
        em = str(c.get(C_EMAIL, "")).strip().lower()
        if not em:
            continue
        if email_filter and em != email_filter:
            continue
        groups.setdefault(em, []).append(c)

    coop = {em: rows for em, rows in groups.items()
            if len({str(r.get(C_SCHOOL, "")).strip() for r in rows}) > 1}
    print(f"\nCo-op people found (Sync=Y at 2+ schools): {len(coop)}\n")

    merged = attached = renamed = sheet_rows = skipped = 0
    for em, rows in sorted(coop.items()):
        schools = sorted({str(r.get(C_SCHOOL, "")).strip() for r in rows})
        name = f"{rows[0].get(C_FIRST, '')} {rows[0].get(C_LAST, '')}".strip()
        ids = sorted({int(str(r.get(C_NS_CID, "")).strip())
                      for r in rows if str(r.get(C_NS_CID, "")).strip().isdigit()})
        print(f"{name} <{em}>  schools={schools}  ids={ids}")

        if not ids:
            print("   (no NS ids yet — nightly sync will create+share; skipping)")
            skipped += 1
            continue

        # Verify which ids still exist; primary = oldest surviving record
        live_recs = {}
        for cid in ids:
            rec = get_contact(cid)
            if rec:
                live_recs[cid] = rec
        if not live_recs:
            print("   (none of the stored ids exist in NS — skipping)")
            skipped += 1
            continue
        primary = min(live_recs)
        dups = [cid for cid in live_recs if cid != primary]
        print(f"   primary={primary}  duplicates-to-retire={dups or 'none'}")

        if live:
            # Retire duplicates: rename to free unique-name space + inactivate
            for cid in dups:
                rec = live_recs[cid]
                new_last = f"{(rec.get('lastName') or '').strip()} (dup {cid})"[:80]
                r = ns_patch(f"contact/{cid}",
                             {"lastName": new_last, "isInactive": True})
                if r.status_code == 204:
                    renamed += 1
                    print(f"   retired duplicate {cid} -> '{new_last}', inactive")
                else:
                    print(f"   WARN: couldn't retire {cid}: "
                          f"{r.status_code} {r.text[:150]}")
                time.sleep(0.2)

            # Attach primary card to every school in the group
            for sch in schools:
                cust = school_cust.get(sch)
                if not cust:
                    print(f"   WARN: no NS Customer ID for school {sch!r} — skipping attach")
                    continue
                if ensure_attached(primary, cust):
                    attached += 1
                time.sleep(0.2)

        # Repoint sheet rows to the primary id (and clear hash so the next
        # push refreshes the shared card's fields)
        for r in rows:
            if str(r.get(C_NS_CID, "")).strip() != str(primary):
                r[C_NS_CID] = str(primary)
                r[C_HASH] = ""
                sheet_rows += 1
        merged += 1
        print()

    if live:
        save_contacts(contacts_ws, contacts_data)

    print("=" * 66)
    print(f"  {'APPLIED' if live else 'DRY RUN (nothing changed)'}")
    print(f"  People processed:        {merged}")
    print(f"  Duplicates retired:      {renamed}")
    print(f"  Attach operations:       {attached}")
    print(f"  Sheet rows repointed:    {sheet_rows}")
    print(f"  Skipped:                 {skipped}")
    print("=" * 66)


if __name__ == "__main__":
    main()
