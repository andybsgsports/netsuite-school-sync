"""
repair_ihsa_orphans.py — find (and optionally repair) Illinois contacts that
were wrongly flipped Sync=N although IHSA still lists them today.

Why this exists (2026-10-05/06): Lena-Winslow had 25 of 26 people on Sync=N
and Durand's contacts weren't syncing either. Root cause: the daily digest
scrape (rep_digests.scrape_il_schools) resolves each person's email one
call at a time and silently dropped anyone whose lookup failed (throttling /
a hiccup) — yet still counted the school as "scraped OK". The snapshot diff
then read the dropped people as departures, flipped them to Sync=N, and the
nightly push inactivated them in NetSuite and removed their Ship-To. The flip
is one-way: the Contacts-tab merge never re-adds a person whose row already
exists, so they stayed N even though every later scrape listed them.
rep_digests / ihsa_sync now retry the lookups and refuse to run departure
logic on a school whose lookups failed. THIS script repairs the damage that
already happened.

How it decides: re-scrapes every Illinois school with the guarded scraper,
and for each school whose scrape was fully RELIABLE, finds Contacts-tab rows
that are Sync != Y but whose (school, email, role) key is on IHSA's current
roster. Those are "wrongly N" candidates.

  * A school with MASS_MIN or more candidates is a mass flip (the outage
    signature) — its candidates are repaired.
  * Schools with fewer (1-2) are listed but left alone unless
    INCLUDE_SINGLES=1: a lone Sync=N row may be a deliberate exclusion by a
    rep, and nothing in the sheet distinguishes the two.
  * Rows marked NS Contact ID = UNLINKED are never touched (the push could
    not link them on purpose).

Repair = set Sync=Y only. The NS Contact ID stays blank, so the next push
re-links each person (reactivating the contact NetSuite inactivated; records
renamed "(dup NNN)" are still never reactivated). Writes the Contacts tab in
one guarded save_contacts call. Does not touch NetSuite.

Schools are scraped ONE AT A TIME with a pause between them (IHSA's email
endpoint cuts a long single run off after ~17 schools: a 50-school scan
verified only 17, while single-school runs verify cleanly). Schools whose
lookups still fail are retried in later rounds after a longer pause. With
LIVE=1 each school's repair is saved as soon as it is verified, so a
timeout or crash keeps the progress made.

DRY RUN by default: prints the plan, writes nothing. LIVE=1 applies.
Env: LIVE, SCHOOL_FILTER (exact School Name; comma-separated list OK),
     SALES_REP_FILTER (exact), MASS_MIN (default 3), INCLUDE_SINGLES,
     PAUSE_SECONDS (between schools, default 45), RETRY_ROUNDS (default 2),
     RETRY_PAUSE_SECONDS (before each retry round, default 180).
"""
from __future__ import annotations

import os
import sys
import time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from school_netsuite_sync import (
    get_gspread_client, load_contacts, save_contacts, screen_school_name_collisions,
    GOOGLE_SHEET_ID, MASTER_TAB, M_NAME, M_URL, M_SALES, M_STATE, M_NS_ID,
    C_SCHOOL, C_FIRST, C_LAST, C_EMAIL, C_ROLE, C_TYPE, C_SYNC, C_NS_CID,
)
from rep_digests import scrape_il_schools

LIVE = os.environ.get("LIVE", "").strip() in ("1", "true", "True", "yes")
SCHOOL_FILTER = os.environ.get("SCHOOL_FILTER", "").strip()
REP_FILTER = os.environ.get("SALES_REP_FILTER", "").strip()
MASS_MIN = int(os.environ.get("MASS_MIN", "3") or "3")
INCLUDE_SINGLES = os.environ.get("INCLUDE_SINGLES", "").strip() in ("1", "true", "True", "yes")
PAUSE_SECONDS = float(os.environ.get("PAUSE_SECONDS", "45") or "45")
RETRY_ROUNDS = int(os.environ.get("RETRY_ROUNDS", "2") or "2")
RETRY_PAUSE_SECONDS = float(os.environ.get("RETRY_PAUSE_SECONDS", "180") or "180")


def norm(s):
    return str(s or "").strip().lower()


def live_keys(admins, coaches):
    """(school, email, role-column) keys on IHSA's current roster — the same
    key rep_digests.merge_scraped_into_master_sheet builds: admins use their
    title, coaches their sport."""
    keys = set()
    for a in admins:
        keys.add((norm(a["School"]), norm(a["Email"]), norm(a["Role"])))
    for c in coaches:
        keys.add((norm(c["School"]), norm(c["Email"]), norm(c["Sport"])))
    return keys


def find_candidates(contacts, live, reliable_schools):
    """Pure. Returns {school_norm: [row, ...]} of wrongly-N candidates, plus
    per-school counts of how many live people are already Sync=Y."""
    cands = defaultdict(list)
    y_live = defaultdict(int)
    for c in contacts:
        sch = norm(c.get(C_SCHOOL))
        em = norm(c.get(C_EMAIL))
        if not em or sch not in reliable_schools:
            continue
        key = (sch, em, norm(c.get(C_ROLE)))
        if key not in live:
            continue
        sync = str(c.get(C_SYNC, "N")).strip().upper()
        if sync == "Y":
            y_live[sch] += 1
        elif str(c.get(C_NS_CID, "")).strip().upper() != "UNLINKED":
            cands[sch].append(c)
    return cands, y_live


def main():
    print("=" * 70)
    print(f"  REPAIR IHSA ORPHANS  |  LIVE={LIVE}  MASS_MIN={MASS_MIN}  "
          f"INCLUDE_SINGLES={INCLUDE_SINGLES}")
    if SCHOOL_FILTER:
        print(f"  SCHOOL_FILTER: {SCHOOL_FILTER}")
    if REP_FILTER:
        print(f"  SALES_REP_FILTER: {REP_FILTER}")
    print("=" * 70)

    gc = get_gspread_client()
    records = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(MASTER_TAB).get_all_records()
    quarantined = screen_school_name_collisions(
        [(r.get(M_NAME, ""), r.get(M_NS_ID, "")) for r in records])
    contacts, ws = load_contacts(gc)

    # Only schools that have at least one non-Y row with an email can have a
    # candidate — skip the rest (saves most of the IHSA calls).
    schools_with_n = {norm(c.get(C_SCHOOL)) for c in contacts
                      if norm(c.get(C_EMAIL))
                      and str(c.get(C_SYNC, "N")).strip().upper() != "Y"}

    il, seen = [], set()
    for r in records:
        if str(r.get(M_STATE, "")).strip().upper() != "IL":
            continue
        name = str(r.get(M_NAME, "")).strip()
        url = str(r.get(M_URL, "")).strip()
        if not (name and url) or name in quarantined or (name, url) in seen:
            continue
        if SCHOOL_FILTER and name not in {x.strip() for x in SCHOOL_FILTER.split(",")}:
            continue
        if REP_FILTER and str(r.get(M_SALES, "")).strip() != REP_FILTER:
            continue
        if norm(name) not in schools_with_n:
            continue
        seen.add((name, url))
        il.append((name, url))
    print(f"\nIL schools to re-scrape (have >=1 Sync=N row): {len(il)}\n")

    flush = lambda: sys.stdout.flush()
    mass, singles, y_live = {}, {}, {}
    applied = 0
    reliable = set()
    pending = list(il)
    for rnd in range(RETRY_ROUNDS + 1):
        if not pending:
            break
        if rnd:
            print(f"\n--- retry round {rnd}: {len(pending)} school(s) after a "
                  f"{RETRY_PAUSE_SECONDS:.0f}s pause ---"); flush()
            time.sleep(RETRY_PAUSE_SECONDS)
        still = []
        for i, (name, url) in enumerate(pending):
            if i:
                time.sleep(PAUSE_SECONDS)
            admins, coaches, scraped = scrape_il_schools([(name, url)])
            if norm(name) not in {norm(x) for x in scraped}:
                still.append((name, url)); flush()
                continue
            reliable.add(norm(name))
            cands, yl = find_candidates(contacts, live_keys(admins, coaches), {norm(name)})
            y_live.update(yl)
            rows = cands.get(norm(name), [])
            if not rows:
                flush(); continue
            is_mass = len(rows) >= MASS_MIN
            (mass if is_mass else singles)[norm(name)] = rows
            print(f"    -> {name}: {len(rows)} wrongly-N, {yl.get(norm(name), 0)} already Y "
                  f"({'mass flip' if is_mass else 'single'})")
            if LIVE and (is_mass or INCLUDE_SINGLES):
                # Re-read the tab right before writing: this run lasts a while
                # and the nightly jobs edit the same sheet — never save a stale copy.
                fresh, fresh_ws = load_contacts(gc)
                frows = find_candidates(fresh, live_keys(admins, coaches), {norm(name)})[0] \
                    .get(norm(name), [])
                for c in frows:
                    c[C_SYNC] = "Y"
                if frows and save_contacts(fresh_ws, fresh):
                    applied += len(frows)
                    contacts = fresh
                    print(f"    APPLIED {len(frows)} row(s) -> Sync=Y at {name}")
                elif frows:
                    print(f"    NOT SAVED at {name} — save_contacts refused")
            flush()
        pending = still
    unreliable = sorted(n for n, _ in pending)

    def show(title, groups):
        print(f"\n{title}")
        for sch in sorted(groups, key=lambda s: -len(groups[s])):
            rows = groups[sch]
            print(f"\n  {rows[0].get(C_SCHOOL)}  — {len(rows)} wrongly-N, "
                  f"{y_live.get(sch, 0)} already Y")
            for c in rows:
                print(f"     {c.get(C_FIRST)} {c.get(C_LAST)} <{c.get(C_EMAIL)}> "
                      f"{c.get(C_ROLE)} [{c.get(C_TYPE)}]")

    show(f"MASS-FLIP SCHOOLS (>= {MASS_MIN} candidates) — {len(mass)} school(s), "
         f"{sum(len(v) for v in mass.values())} row(s):", mass)
    show(f"SINGLES (< {MASS_MIN}; left alone unless INCLUDE_SINGLES=1) — "
         f"{len(singles)} school(s), {sum(len(v) for v in singles.values())} row(s):", singles)

    if unreliable:
        print(f"\nSkipped (scrape still not reliable after {RETRY_ROUNDS} retry "
              f"round(s) — re-run later): {len(unreliable)}")
        for n in unreliable:
            print(f"   {n}")

    n_target = sum(len(v) for v in mass.values()) + \
        (sum(len(v) for v in singles.values()) if INCLUDE_SINGLES else 0)
    print("\n" + "=" * 70)
    print(f"  schools: {len(il)}   verified: {len(reliable)}   "
          f"unreliable/skipped: {len(unreliable)}")
    print(f"  mass-flip schools: {len(mass)}   rows needing Sync=Y: {n_target}   "
          f"applied: {applied}")
    if not LIVE:
        print("  DRY RUN — nothing changed. Set LIVE=1 to apply.")
    print("=" * 70)


if __name__ == "__main__":
    main()
