"""
rebuild_schools_tab.py
----------------------
One-time: consolidate all schools into a single `Schools` tab with the
full per-school detail columns the user's original format had. Pulls
fresh metadata from the source (WIAA scrape for WI, IHSA API for IL)
and preserves the user-owned columns from the current Schools_Master
tab (NS Customer ID, Sales Rep, Locked, Notes).

After this runs:
  - The `Schools` tab holds all 327 rows with the unified 24-column schema.
  - The sync scripts are updated to read from `Schools` directly.
  - `Schools_Master` and `IL_Schools` tabs can be deleted.

Usage:
  python rebuild_schools_tab.py            # dry-run: build rows, print summary
  python rebuild_schools_tab.py --live     # actually write the Schools tab
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
from netsuite_sync import scrape_wiaa_school_detail, slugify

SHEET_ID = os.environ.get("GOOGLE_SHEET_ID",
                          "1iWhtasin-gmk3jllDvls7G1eI_pgzMm4yfQUP_qZHEM")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]

SOURCE_TAB = "Schools_Master"
TARGET_TAB = "Schools"

# Unified 24-column schema
COLUMNS = [
    "School Name", "Full Name", "State", "School URL", "NS Ext ID",
    "Sales Rep", "Class", "Level", "Nickname", "Colors", "Conference",
    "WIAA District", "Enrollment", "Size", "Phone",
    "Address1", "Address2", "City", "Zip", "Website",
    "NS Customer ID", "Locked", "Last Synced", "Notes",
]

# IHSA API
IHSA_API = "https://api.ihsa.org/v1"
IHSA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Sec-Fetch-Site": "same-site",
    "Referer": "https://www.ihsa.org/",
    "Origin": "https://www.ihsa.org",
}


def _normalize_url(u):
    u = (u or "").strip()
    if not u:
        return ""
    if not u.lower().startswith(("http://", "https://")):
        u = "https://" + u
    return u


def fetch_ihsa(url):
    m = re.search(r"/details/(\d+)", url or "")
    if not m:
        return {}
    school_id = m.group(1).zfill(4)
    r = requests.get(f"{IHSA_API}/schools/{school_id}",
                     headers=IHSA_HEADERS, timeout=15)
    if r.status_code != 200:
        return {}
    d = r.json().get("data", {}) or {}
    return {
        "Full Name":     d.get("NameFormal") or d.get("NameIHSA") or "",
        "Class":         d.get("PublicPrivate") or "",
        "Level":         "High School",
        "Nickname":      (d.get("NicknameBoys") or d.get("NicknameGirls") or "").strip(),
        "Colors":        d.get("Colors") or "",
        "Conference":    "",
        "WIAA District": "",
        "Enrollment":    _int_str(d.get("EnrollmentString") or d.get("Enrollment")),
        "Size":          "",
        "Phone":         d.get("Phone") or "",
        "Address1":      d.get("Address") or "",
        "Address2":      (f"PO BOX {d.get('POBox')}" if d.get("POBox") else ""),
        "City":          d.get("City") or "",
        "Zip":           d.get("Zip") or "",
        "Website":       _normalize_url(d.get("URL") or ""),
    }


def fetch_wiaa(url):
    try:
        info, _admins, _coaches = scrape_wiaa_school_detail(url)
    except Exception as exc:
        print(f"    [WIAA error] {exc}")
        return {}
    level = info.get("level") or "High School"
    school_class = info.get("school_class") or ""
    return {
        # Full Name is built from School Name + Level in the old schema — we
        # fill it in after we have the School Name.
        "Full Name":     "",
        "Class":         school_class,
        "Level":         level,
        "Nickname":      info.get("nickname") or "",
        "Colors":        info.get("colors") or "",
        "Conference":    info.get("conference") or "",
        "WIAA District": info.get("wiaa_district") or "",
        "Enrollment":    _int_str(info.get("enrollment")),
        "Size":          info.get("school_size") or "",
        "Phone":         info.get("phone") or "",
        "Address1":      info.get("address1") or "",
        "Address2":      info.get("address2") or "",
        "City":          info.get("city") or "",
        "Zip":           info.get("zip") or "",
        "Website":       _normalize_url(info.get("website") or ""),
    }


def _int_str(v):
    try:
        return str(int(float(v)))
    except (TypeError, ValueError):
        return ""


def get_gc():
    creds_file = Path(__file__).parent / "credentials.json"
    creds = Credentials.from_service_account_file(str(creds_file), scopes=SCOPES)
    return gspread.authorize(creds)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()

    gc = get_gc()
    wb = gc.open_by_key(SHEET_ID)
    src_ws = wb.worksheet(SOURCE_TAB)
    src_rows = src_ws.get_all_records()
    print(f"Loaded {len(src_rows)} rows from {SOURCE_TAB}")

    rows_out = []
    for i, src in enumerate(src_rows, 1):
        name  = str(src.get("School Name", "")).strip()
        state = str(src.get("State", "")).strip().upper()
        url   = str(src.get("Scraper URL", "")).strip()
        if not (name and state and url):
            continue

        print(f"[{i}/{len(src_rows)}] {state} {name}")
        if "wiaawi.org" in url.lower():
            meta = fetch_wiaa(url)
        elif "ihsa.org" in url.lower():
            meta = fetch_ihsa(url)
        else:
            meta = {}

        level = meta.get("Level", "") or "High School"
        full_name = meta.get("Full Name") or (f"{name} {level}".strip() if level else name)

        rows_out.append({
            "School Name":    name,
            "Full Name":      full_name,
            "State":          state,
            "School URL":     url,
            "NS Ext ID":      slugify(name),
            "Sales Rep":      str(src.get("Sales Rep", "")).strip(),
            "Class":          meta.get("Class", ""),
            "Level":          level,
            "Nickname":       meta.get("Nickname", ""),
            "Colors":         meta.get("Colors", ""),
            "Conference":     meta.get("Conference", ""),
            "WIAA District":  meta.get("WIAA District", ""),
            "Enrollment":     meta.get("Enrollment", ""),
            "Size":           meta.get("Size", ""),
            "Phone":          meta.get("Phone", ""),
            "Address1":       meta.get("Address1", ""),
            "Address2":       meta.get("Address2", ""),
            "City":           meta.get("City", ""),
            "Zip":            meta.get("Zip", ""),
            "Website":        meta.get("Website", ""),
            "NS Customer ID": str(src.get("NS Customer ID", "")).strip(),
            "Locked":         str(src.get("Locked", "")).strip(),
            "Last Synced":    str(src.get("Last Synced", "")).strip(),
            "Notes":          str(src.get("Notes", "")).strip(),
        })
        time.sleep(0.4)

    print(f"\nBuilt {len(rows_out)} rows.")
    if not args.live:
        print("DRY RUN — pass --live to write the Schools tab.")
        return

    # Overwrite Schools tab (create if missing)
    try:
        schools_ws = wb.worksheet(TARGET_TAB)
        schools_ws.clear()
    except Exception:
        schools_ws = wb.add_worksheet(title=TARGET_TAB,
                                      rows=len(rows_out) + 20,
                                      cols=len(COLUMNS))

    values = [COLUMNS] + [[str(r.get(h, "") or "") for h in COLUMNS] for r in rows_out]
    schools_ws.update(range_name="A1", values=values)
    schools_ws.freeze(rows=1)
    print(f"Wrote {len(rows_out)} rows to '{TARGET_TAB}' tab, froze header row.")


if __name__ == "__main__":
    main()
