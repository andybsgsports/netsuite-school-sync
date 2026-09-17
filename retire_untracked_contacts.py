"""
retire_untracked_contacts.py — retire NetSuite contact records the sync
doesn't track: ACTIVE contacts whose company is one of our school customers
but whose id no Sync=Y sheet row at that school references.

Where they come from:
  * legacy per-school records that predate the July shared-card migration
    and were never referenced by the sheet, so the migration never retired
    them — Mary Jo Mutchler 46176 on Crystal Lake South next to her shared
    card 48315, which is why she shows twice in the invoice email picker;
  * people who left while the Contacts tab was empty (2026-08-31 → 09-02),
    so no Sync=N row ever flipped and push_only never inactivated them.

Per ACTIVE NS record R with company = C, compared against the sheet's
Sync=Y rows at C:
  TRACKED      R.id is on a row at C                      → keep
  DUP_OF_KEPT  R.email is on a row at C under another id  → retire, keep
               its Ship-To (the kept card's line has the same label)
  ORPHAN       R.email is on no row at C                  → retire +
               remove Ship-To line
  NO_EMAIL     nothing to reason with                     → skip
  NOT_OURS     comments lacks "Auto-synced by School Sync" → skip
               (a contact Andy made by hand is never touched)
Retire = the merge's convention: lastName += " (dup {id})", isInactive=True.
The ownership check GETs each retire candidate's comments — candidates
only, not every contact.

Safety: a school whose SuiteQL roster comes back EMPTY while the sheet has
5+ active rows there is treated as a query failure (ns_suiteql swallows
errors into []) and skipped. Schools with a name collision on the Schools
tab are skipped, same as the nightly.

DRY RUN by default; LIVE=1 applies. SALES_REP_FILTER / SCHOOL_FILTER scope.
"""
from __future__ import annotations

import os
import re
import sys
import time
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from netsuite_sync import ns_get, ns_patch, ns_suiteql, remove_contact_ship_to
from school_netsuite_sync import (
    get_gspread_client, load_contacts, screen_school_name_collisions,
    GOOGLE_SHEET_ID, MASTER_TAB, M_NAME, M_NS_ID, M_SALES,
    C_SCHOOL, C_FIRST, C_LAST, C_EMAIL, C_SYNC, C_NS_CID,
)

LIVE = os.environ.get("LIVE", "").strip() in ("1", "true", "True", "yes")
REP_FILTER = os.environ.get("SALES_REP_FILTER", "").strip()
SCHOOL_FILTER = os.environ.get("SCHOOL_FILTER", "").strip()
OWNER_MARK = "Auto-synced by School Sync"
MIN_ROWS_FOR_EMPTY_GUARD = 5


def classify(ns_records, sheet_rows):
    """Pure. ns_records: [{id, first, last, email}] ACTIVE with company=C.
    sheet_rows: the Sync=Y Contacts rows at C. Returns [(verdict, rec)]."""
    tracked_ids = {str(r.get(C_NS_CID, "")).strip() for r in sheet_rows
                   if str(r.get(C_NS_CID, "")).strip().isdigit()}
    sheet_emails = {str(r.get(C_EMAIL, "")).strip().lower() for r in sheet_rows
                    if str(r.get(C_EMAIL, "")).strip()}
    out = []
    for rec in ns_records:
        cid = str(rec.get("id", "")).strip()
        em = str(rec.get("email", "")).strip().lower()
        if cid in tracked_ids:
            out.append(("TRACKED", rec))
        elif not em:
            out.append(("NO_EMAIL", rec))
        elif em in sheet_emails:
            out.append(("DUP_OF_KEPT", rec))
        else:
            out.append(("ORPHAN", rec))
    return out


def active_roster(customer_id):
    return [{"id": str(r.get("id") or ""),
             "first": (r.get("firstname") or "").strip(),
             "last": (r.get("lastname") or "").strip(),
             "email": (r.get("email") or "").strip()}
            for r in ns_suiteql(
                f"SELECT id, firstname, lastname, email FROM contact "
                f"WHERE company = {int(customer_id)} "
                f"AND (isinactive = 'F' OR isinactive IS NULL)", limit=1000)]


def owned_by_sync(contact_id):
    r = ns_get(f"contact/{contact_id}?fields=comments")
    return r.status_code == 200 and OWNER_MARK in (r.json().get("comments") or "")


def retire(rec):
    new_last = f"{rec['last']} (dup {rec['id']})"[:80]
    r = ns_patch(f"contact/{rec['id']}", {"lastName": new_last, "isInactive": True})
    return r.status_code == 204


def main():
    print("=" * 70)
    print(f"  RETIRE UNTRACKED CONTACTS  |  LIVE={LIVE}")
    if REP_FILTER:
        print(f"  SALES_REP_FILTER: {REP_FILTER}")
    if SCHOOL_FILTER:
        print(f"  SCHOOL_FILTER: {SCHOOL_FILTER}")
    print("=" * 70)

    gc = get_gspread_client()
    schools = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(MASTER_TAB).get_all_records()
    quarantined = screen_school_name_collisions(
        [(s.get(M_NAME, ""), s.get(M_NS_ID, "")) for s in schools])
    contacts, _ws = load_contacts(gc)
    y_by_school = defaultdict(list)
    for c in contacts:
        if str(c.get(C_SYNC, "N")).strip().upper() == "Y":
            y_by_school[str(c.get(C_SCHOOL, "")).strip()].append(c)

    targets = []
    for s in schools:
        name = str(s.get(M_NAME, "")).strip()
        ns_id = re.sub(r"\.0$", "", str(s.get(M_NS_ID, "")).strip())
        rep = str(s.get(M_SALES, "")).strip()
        if not name or not ns_id.isdigit() or name in quarantined:
            continue
        if REP_FILTER and rep != REP_FILTER:
            continue
        if SCHOOL_FILTER and name != SCHOOL_FILTER:
            continue
        targets.append((name, ns_id))
    print(f"\nSchools in scope: {len(targets)}\n")

    totals = Counter()
    retired = shipto = 0
    for i, (name, ns_id) in enumerate(targets, 1):
        roster = active_roster(ns_id)
        rows = y_by_school.get(name, [])
        if not roster and len(rows) >= MIN_ROWS_FOR_EMPTY_GUARD:
            print(f"[{i}/{len(targets)}] {name} (NS {ns_id}) — WARN: SuiteQL returned "
                  f"0 active contacts but sheet has {len(rows)} — treating as a "
                  f"query failure, skipping")
            totals["QUERY_FAIL"] += 1
            continue
        verdicts = classify(roster, rows)
        cands = [(v, r) for v, r in verdicts if v in ("DUP_OF_KEPT", "ORPHAN")]
        for v, _r in verdicts:
            totals[v] += 1
        if not cands:
            continue
        print(f"[{i}/{len(targets)}] {name} (NS {ns_id}) — {len(roster)} active in NS, "
              f"{len(rows)} sheet rows, {len(cands)} untracked:")
        for v, rec in cands:
            ours = owned_by_sync(rec["id"])
            tag = v if ours else "NOT_OURS"
            if not ours:
                totals[v] -= 1
                totals["NOT_OURS"] += 1
            print(f"     {tag:<12} {rec['id']:<8} {rec['first']} {rec['last']:<22} {rec['email']}")
            if LIVE and ours:
                if retire(rec):
                    retired += 1
                    if v == "ORPHAN":
                        remove_contact_ship_to(ns_id, f"{rec['first']} {rec['last']}")
                        shipto += 1
                else:
                    print(f"        WARN: retire failed for {rec['id']}")
                time.sleep(0.2)

    print("\n" + "=" * 70)
    for k in ("TRACKED", "DUP_OF_KEPT", "ORPHAN", "NOT_OURS", "NO_EMAIL", "QUERY_FAIL"):
        if totals.get(k):
            print(f"  {k:<12} {totals[k]}")
    if LIVE:
        print(f"  retired: {retired}   Ship-To lines removed: {shipto}")
    else:
        print("  DRY RUN — nothing changed. Set LIVE=1 to apply.")
    print("=" * 70)


if __name__ == "__main__":
    main()
