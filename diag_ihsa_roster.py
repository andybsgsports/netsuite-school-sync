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
the Schools tab's "School URL", e.g. ihsa.org/schools/details/1219 -> 1219)
"""
from __future__ import annotations

import json
import os
import sys

import requests

SCHOOL_ID = os.environ.get("DIAG_SCHOOL_ID", "").strip()

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


def main():
    print("=" * 70)
    print(f"  IHSA ROSTER CHECK  |  school id {SCHOOL_ID}  (read-only, no NS/Sheets)")
    print("=" * 70)
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
