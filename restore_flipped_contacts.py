"""
restore_flipped_contacts.py — undo a bad digest run's Sync=N flips using the
committed rep-digest snapshots as the record of who was there.

Why (2026-10-13): IHSA put its staff-email lookups behind a human-verification
gate (HTTP 401 "turnstile-required"). The digest read every 401 as "this
person has no email", so 24 Illinois schools (Grant, Freeport, Durand, ...)
came back nearly empty, 834 people looked like departures, were flipped to
Sync=N, and the nightly push inactivated them in NetSuite. ihsa_sync /
rep_digests no longer treat a 401 as "no email" and refuse a one-run school
collapse; this script puts the damage back.

How: snapshots/<rep>.json is committed after every digest run. Keys present
at GOOD_REV but gone at BAD_REV (the bad run's commit) are the people that
run dropped. Contacts-tab rows matching one of those keys on
(school, email, sport-or-role) that are Sync != Y are set back to Sync=Y,
limited to STATE (default IL) schools so genuine Wisconsin churn from the
same run is left alone. NS Contact ID is left as is (inactive contacts are
re-linked and reactivated by the next push); UNLINKED rows are never touched.

DRY RUN by default; LIVE=1 applies (one guarded save_contacts call).
Env: GOOD_REV, BAD_REV (git revisions), REP (default "Andrew Murray"),
     STATE (default IL), LIVE.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from school_netsuite_sync import (
    get_gspread_client, load_contacts, save_contacts,
    GOOGLE_SHEET_ID, MASTER_TAB, M_NAME, M_STATE,
    C_SCHOOL, C_FIRST, C_LAST, C_EMAIL, C_ROLE, C_SYNC, C_NS_CID,
)
from rep_digests import snapshot_path

LIVE = os.environ.get("LIVE", "").strip() in ("1", "true", "True", "yes")
GOOD_REV = os.environ.get("GOOD_REV", "").strip()
BAD_REV = os.environ.get("BAD_REV", "").strip()
REP = os.environ.get("REP", "Andrew Murray").strip()
STATE = os.environ.get("STATE", "IL").strip().upper()


def norm(s):
    return str(s or "").strip().lower()


def snapshot_keys_at(rev, rep):
    """{(school, email, sport-or-role)} for rep's snapshot at a git revision,
    in the same shape rep_digests uses for departed triples."""
    rel = os.path.relpath(snapshot_path(rep), os.path.dirname(os.path.abspath(__file__)))
    raw = subprocess.run(["git", "show", f"{rev}:{rel}"], capture_output=True,
                         text=True, check=True).stdout
    keys = set()
    for r in json.loads(raw).get("records", []):
        keys.add((norm(r.get("school")), norm(r.get("email")),
                  norm(r.get("sport")) or norm(r.get("role"))))
    return keys


def dropped_keys(good, bad, states_by_school):
    """Pure. Keys in good but not bad, at schools in STATE."""
    return {k for k in good - bad if states_by_school.get(k[0]) == STATE}


def rows_to_restore(contacts, dropped):
    """Pure. Contacts rows matching a dropped key that are Sync != Y and not
    UNLINKED."""
    out = []
    for c in contacts:
        key = (norm(c.get(C_SCHOOL)), norm(c.get(C_EMAIL)), norm(c.get(C_ROLE)))
        if key not in dropped:
            continue
        if str(c.get(C_SYNC, "N")).strip().upper() == "Y":
            continue
        if str(c.get(C_NS_CID, "")).strip().upper() == "UNLINKED":
            continue
        out.append(c)
    return out


def main():
    print("=" * 70)
    print(f"  RESTORE FLIPPED CONTACTS  |  LIVE={LIVE}  REP={REP}  STATE={STATE}")
    print(f"  good snapshot: {GOOD_REV}   bad snapshot: {BAD_REV}")
    print("=" * 70)
    if not (GOOD_REV and BAD_REV):
        print("  ERROR: set GOOD_REV and BAD_REV")
        sys.exit(1)

    good = snapshot_keys_at(GOOD_REV, REP)
    bad = snapshot_keys_at(BAD_REV, REP)

    gc = get_gspread_client()
    records = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(MASTER_TAB).get_all_records()
    states = {norm(r.get(M_NAME)): str(r.get(M_STATE, "")).strip().upper() for r in records}

    dropped = dropped_keys(good, bad, states)
    print(f"\n  snapshot keys: good={len(good)} bad={len(bad)}  "
          f"dropped at {STATE} schools: {len(dropped)}")

    contacts, ws = load_contacts(gc)
    rows = rows_to_restore(contacts, dropped)
    by_school = Counter(c.get(C_SCHOOL) for c in rows)
    print(f"  Contacts rows to set back to Sync=Y: {len(rows)} at {len(by_school)} school(s)\n")
    for sch, n in sorted(by_school.items(), key=lambda x: -x[1]):
        print(f"    {n:4d}  {sch}")
    for c in rows[:15]:
        print(f"      e.g. {c.get(C_FIRST)} {c.get(C_LAST)} <{c.get(C_EMAIL)}> {c.get(C_ROLE)}")

    print("\n" + "=" * 70)
    if not rows:
        print("  nothing to restore.")
    elif not LIVE:
        print("  DRY RUN — nothing changed. Set LIVE=1 to apply.")
    else:
        for c in rows:
            c[C_SYNC] = "Y"
        if save_contacts(ws, contacts):
            print(f"  APPLIED: {len(rows)} row(s) set to Sync=Y. The next push "
                  f"re-links and reactivates them in NetSuite.")
        else:
            print("  NOT SAVED — save_contacts refused (see message above). Nothing changed.")
    print("=" * 70)


if __name__ == "__main__":
    main()
