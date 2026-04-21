"""
create_missing_ns_customers.py
------------------------------
One-off: for rows on Schools_Master where Match Confidence == "none"
(no NS customer exists for the school at all), scrape the source
(WIAA for WI, IHSA API for IL), and CREATE a NetSuite customer record
using that info. Writes the newly-created NS Customer ID back to the
Schools_Master tab.

Only touches rows with:
  - NS Customer ID blank
  - Match Confidence == "none"
  - Locked != "Y"

Rows that are "ambiguous" or "low" already have a plausible NS ID
suggested in Notes — those need human review and are skipped here.

Usage:
  python create_missing_ns_customers.py                # dry-run
  python create_missing_ns_customers.py --live         # create
  python create_missing_ns_customers.py --school X     # limit to one row
"""

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

import gspread
import requests
from google.oauth2.service_account import Credentials

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from netsuite_sync import (
    scrape_wiaa_school_detail,
    sync_customer,
    slugify,
)

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
SHEET_ID = os.environ.get(
    "GOOGLE_SHEET_ID", "1iWhtasin-gmk3jllDvls7G1eI_pgzMm4yfQUP_qZHEM"
)
MASTER_TAB = "Schools_Master"

IHSA_API = "https://api.ihsa.org/v1"
IHSA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Sec-Fetch-Site": "same-site",
    "Referer": "https://www.ihsa.org/",
    "Origin": "https://www.ihsa.org",
}


def get_gspread_client():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if creds_json:
        creds = Credentials.from_service_account_info(
            json.loads(creds_json), scopes=GOOGLE_SCOPES
        )
    else:
        creds_file = Path(__file__).parent / "credentials.json"
        creds = Credentials.from_service_account_file(str(creds_file), scopes=GOOGLE_SCOPES)
    return gspread.authorize(creds)


def _normalize_url(u):
    """NetSuite rejects bare domains on the url field — require http(s)://."""
    u = (u or "").strip()
    if not u:
        return ""
    if not u.lower().startswith(("http://", "https://")):
        u = "https://" + u
    return u


def fetch_ihsa_school_info(url):
    """Return a school_info dict compatible with sync_customer()."""
    m = re.search(r"/details/(\d+)", url or "")
    if not m:
        return {}
    school_id = m.group(1).zfill(4)
    r = requests.get(f"{IHSA_API}/schools/{school_id}", headers=IHSA_HEADERS, timeout=15)
    if r.status_code != 200:
        return {}
    d = r.json().get("data", {})
    return {
        "level":         "High School",
        "school_class":  d.get("PublicPrivate") or "",
        "nickname":      (d.get("NicknameBoys") or d.get("NicknameGirls") or "").strip(),
        "colors":        d.get("Colors") or "",
        "conference":    "",
        "wiaa_district": "",
        "school_size":   "",
        "state":         "IL",
        "enrollment":    _parse_int(d.get("EnrollmentString") or d.get("Enrollment")),
        "address1":      d.get("Address") or "",
        "address2":      (f"PO BOX {d.get('POBox')}" if d.get("POBox") else ""),
        "city":          d.get("City") or "",
        "zip":           d.get("Zip") or "",
        "phone":         d.get("Phone") or "",
        "website":       _normalize_url(d.get("URL") or ""),
    }


def _parse_int(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def scrape_source(url, state):
    if "wiaawi.org" in url.lower():
        info, _admins, _coaches = scrape_wiaa_school_detail(url)
        return info
    if "ihsa.org" in url.lower():
        return fetch_ihsa_school_info(url)
    return {}


def load_master(gc):
    wb = gc.open_by_key(SHEET_ID)
    ws = wb.worksheet(MASTER_TAB)
    return wb, ws, ws.get_all_records()


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--live", action="store_true",
                        help="Actually create the NS customers and write IDs back. "
                             "Default is dry-run.")
    parser.add_argument("--school",
                        help="Additional school names (comma-separated, exact match) "
                             "to force-include. These are ADDED to the default set of "
                             "rows with Match Confidence == 'none'.")
    args = parser.parse_args()

    print(f"{'=' * 60}")
    print(f"  Create missing NS customers  —  {'LIVE' if args.live else 'DRY RUN'}")
    print(f"{'=' * 60}")

    gc = get_gspread_client()
    wb, ws, rows = load_master(gc)
    print(f"Loaded {len(rows)} rows from {MASTER_TAB}\n")

    # --school is ADDITIVE: the named schools are force-included regardless
    # of confidence. Without --school, only "none" rows are processed.
    # This lets you pick off an ambiguous/low row (e.g. "Brookwood", which
    # was matched to the middle school) while still creating everything in
    # the default none-set in one run.
    extra_names = {n.strip() for n in (args.school or "").split(",") if n.strip()}

    targets = []
    for idx, r in enumerate(rows):
        if str(r.get("NS Customer ID", "")).strip():
            continue
        if str(r.get("Locked", "")).strip().upper() == "Y":
            continue
        name = str(r.get("School Name", "")).strip()
        if not name:
            continue
        conf = str(r.get("Match Confidence", "")).strip().lower()
        if conf == "none" or name in extra_names:
            targets.append((idx, r))

    if not targets:
        print("Nothing to do — no rows with Match Confidence == 'none'.")
        return

    print(f"Will create {len(targets)} NS customer(s):")
    for _, r in targets:
        print(f"  {r['State']}  {r['School Name']}  (rep={r.get('Sales Rep','')})")
    print()

    if not args.live:
        print("DRY RUN — pass --live to actually create.")
        return

    created = []
    for row_idx, r in targets:
        name  = str(r["School Name"]).strip()
        state = str(r["State"]).strip() or "WI"
        url   = str(r["Scraper URL"]).strip()
        rep   = str(r.get("Sales Rep", "")).strip() or None

        print(f"\n[CREATE] {name}  ({state})")
        info = scrape_source(url, state)
        if not info:
            print(f"  WARN: couldn't scrape source {url!r}")
            info = {"state": state}  # minimal body

        try:
            ns_id, was_created = sync_customer(name, state, info,
                                               contacts=None, ns_customer_id=None,
                                               sales_rep=rep)
        except Exception as exc:
            print(f"  ERROR: {exc}")
            continue

        if ns_id and was_created:
            print(f"  Created NS Customer ID: {ns_id}")
            # Write back to the sheet
            headers = list(rows[0].keys())
            col = headers.index("NS Customer ID") + 1
            ws.update_cell(row_idx + 2, col, str(ns_id))  # +2 for header + 1-indexing
            # Also update Match Confidence so it's clear this was auto-created.
            if "Match Confidence" in headers:
                mc_col = headers.index("Match Confidence") + 1
                ws.update_cell(row_idx + 2, mc_col, "created")
            created.append((name, ns_id))
        else:
            print(f"  FAIL — sync_customer returned ({ns_id}, {was_created})")
        time.sleep(0.5)

    print(f"\n{'=' * 60}")
    print(f"  Created {len(created)} NS customer(s)")
    for name, nid in created:
        print(f"    {nid:>6}  {name}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
