"""
diag_ihsa_roster.py — read-only. Calls IHSA's public staff2 API directly
for one school id and prints every person it currently returns (name,
title, section). No NetSuite or Google Sheets credentials touched.

Built to settle a specific question for Lena-Winslow (IHSA school id
1219): the Contacts tab has 26 scraped rows but only 1 (Renee Schultz)
is Sync=Y — the other 25 (including the listed Athletic Director, Ryan
Hahne) got auto-flipped to Sync=N by the "no longer on this run's fresh
scrape" departed-contact logic in rep_digests.py. This checks whether
IHSA's own API currently returns a thin roster for this school (expected
behavior reflecting a thin upstream source) or the fuller one our sheet
remembers (would point at a scraper/parsing bug instead).

Env: DIAG_SCHOOL_ID (required, IHSA school id — the number at the end of
the Schools tab's "School URL", e.g. ihsa.org/schools/details/1219 -> 1219).
     DIAG_PROBE_EMAILS (optional, comma-separated school ids or "1") — also
     call the gated email-reveal endpoint once per person with HasEmail and
     print status code + latency; failures are retried after 20s and 60s to
     show whether they're persistent for that person or clear with time.
     (Built to find out why 23 of 50 Illinois schools kept failing email
     lookups on 2026-10-06 — see repair_ihsa_orphans.py.) Emails themselves
     are never printed, only whether one came back.
"""
from __future__ import annotations

import json
import os
import sys
import time
from collections import Counter

import requests

SCHOOL_ID = os.environ.get("DIAG_SCHOOL_ID", "").strip()
PROBE = os.environ.get("DIAG_PROBE_EMAILS", "").strip()

IHSA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Sec-Fetch-Site": "same-site",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "Referer": "https://www.ihsa.org/",
    "Origin": "https://www.ihsa.org",
}
IHSA_API = "https://api.ihsa.org/v1"


def probe_one(school_id, pid):
    t0 = time.time()
    try:
        r = requests.get(f"{IHSA_API}/schools/{school_id}/staff/{pid}/email",
                         headers=IHSA_HEADERS, timeout=15)
        got = ""
        try:
            got = str(r.json().get("email", "")).strip() if r.status_code == 200 else ""
        except ValueError:
            got = "<bad json>"
        retry_after = r.headers.get("Retry-After", "")
        return r.status_code, bool(got), round((time.time() - t0) * 1000), retry_after, \
            (r.text[:80].replace("\n", " ") if r.status_code != 200 else "")
    except requests.RequestException as e:
        return None, False, round((time.time() - t0) * 1000), "", repr(e)[:80]


def probe_school(school_id):
    print(f"\n--- EMAIL PROBE school {school_id} ---")
    r = requests.get(f"{IHSA_API}/schools/{school_id}/staff2", headers=IHSA_HEADERS, timeout=15)
    print(f"staff2 -> HTTP {r.status_code}")
    if r.status_code != 200:
        print(r.text[:200]); return
    people = [(m.get("PersonID"), m.get("Name"), m.get("DefaultTitle"))
              for ms in r.json().get("data", {}).values() for m in ms
              if m.get("HasEmail") and m.get("PersonID") and (m.get("DefaultTitle") or "").strip()]
    seen, uniq = set(), []
    for pid, name, title in people:
        if pid not in seen:
            seen.add(pid); uniq.append((pid, name, title))
    print(f"{len(uniq)} distinct people with HasEmail")
    stats, failed = Counter(), []
    for pid, name, title in uniq:
        status, ok, ms, ra, snip = probe_one(school_id, pid)
        stats[status] += 1
        flag = "" if status == 200 and ok else "   <-- not a clean 200+email"
        print(f"  {pid!s:>8} {str(name)[:26]:<26} HTTP {status} email={'yes' if ok else 'NO '} "
              f"{ms}ms{(' Retry-After=' + ra) if ra else ''}{flag} {snip}")
        if status != 200 or not ok:
            failed.append((pid, name))
        time.sleep(0.15)
    print(f"status counts: {dict(stats)}")
    for wait in (20, 60):
        if not failed:
            break
        print(f"\nwaiting {wait}s, then retrying {len(failed)} failed lookup(s) ...")
        time.sleep(wait)
        still = []
        for pid, name in failed:
            status, ok, ms, ra, snip = probe_one(school_id, pid)
            print(f"  retry {pid!s:>8} {str(name)[:26]:<26} HTTP {status} email={'yes' if ok else 'NO '} {ms}ms {snip}")
            if status != 200 or not ok:
                still.append((pid, name))
            time.sleep(0.15)
        failed = still
    print(f"\nSTILL FAILING after waits: {len(failed)}")


def main():
    print("=" * 70)
    print(f"  IHSA ROSTER CHECK  |  school id {SCHOOL_ID}  (read-only, no NS/Sheets)")
    print("=" * 70)
    if PROBE:
        ids = [x.strip() for x in PROBE.split(",") if x.strip().isdigit()] or [SCHOOL_ID]
        for sid in ids:
            probe_school(sid)
        return
    if not SCHOOL_ID:
        print("  ERROR: set DIAG_SCHOOL_ID")
        return

    r = requests.get(f"{IHSA_API}/schools/{SCHOOL_ID}/staff2",
                      headers=IHSA_HEADERS, timeout=15)
    print(f"\nGET /schools/{SCHOOL_ID}/staff2 -> HTTP {r.status_code}")
    if r.status_code != 200:
        print(r.text[:1000])
        return

    data = r.json().get("data", {})
    total = 0
    for section, members in data.items():
        titled = [m for m in members if (m.get("DefaultTitle") or "").strip()]
        print(f"\n[{section}] {len(members)} entr(ies), {len(titled)} with a title:")
        for m in titled:
            print(f"  {m.get('Name')!r:30} title={m.get('DefaultTitle')!r:30} "
                  f"roleId={m.get('RoleID')!r} hasEmail={m.get('HasEmail')}")
        total += len(titled)

    print(f"\n{'=' * 70}\n  total titled people across all sections: {total}\n{'=' * 70}")


if __name__ == "__main__":
    main()
