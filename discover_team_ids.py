"""
discover_team_ids.py
--------------------
Builds scores_schools.csv automatically — no manual TeamID hunting.

For every WI school on the master sheet's Schools tab:
  1. Fetch the school's WIAA detail page (same URL the nightly scrape uses)
  2. Find every link containing "TeamID=" — these are the school's team
     schedule links (one per sport/level)
  3. Capture the sport name from the link text / surrounding row
  4. Write all discovered teams to scores_schools.csv

Run from GitHub Actions (Discover Team IDs workflow) — wiaawi.org blocks
most other environments but allows the Actions runners (proven nightly).

Env vars:
  GOOGLE_SHEET_ID          master sheet (Schools tab)
  GOOGLE_CREDENTIALS_JSON  service account JSON
  SPORT_FILTER             only keep sports containing this text (e.g. "Baseball")
  SCHOOL_FILTER            only process this school name (testing)
  OUT_CSV                  output path (default scores_schools.csv)
  DUMP_HTML                "1" → dump first school's HTML for debugging
"""

import csv
import json
import os
import re
import sys
import time
from pathlib import Path

import gspread
import requests
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "")
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
SPORT_FILTER  = os.environ.get("SPORT_FILTER", "").strip().lower()
SCHOOL_FILTER = os.environ.get("SCHOOL_FILTER", "").strip()
OUT_CSV       = os.environ.get("OUT_CSV", "scores_schools.csv")
DUMP_HTML     = os.environ.get("DUMP_HTML", "0") == "1"
DELAY         = 1.0

WIAA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer":         "https://schools.wiaawi.org/Directory/School/List",
}

TEAMID_RE = re.compile(r"TeamID=(\d+)", re.I)


def get_gspread_client():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if creds_json:
        creds = Credentials.from_service_account_info(
            json.loads(creds_json), scopes=GOOGLE_SCOPES)
    else:
        creds_file = Path(__file__).parent / "credentials.json"
        creds = Credentials.from_service_account_file(str(creds_file), scopes=GOOGLE_SCOPES)
    return gspread.authorize(creds)


def load_wi_schools(gc):
    """Returns [(school_name, school_url), ...] for WI rows on the Schools tab."""
    wb = gc.open_by_key(GOOGLE_SHEET_ID)
    ws = wb.worksheet("Schools")
    out = []
    for rec in ws.get_all_records():
        name  = str(rec.get("School Name", "")).strip()
        state = str(rec.get("State", "")).strip().upper()
        url   = str(rec.get("School URL", "")).strip()
        if not (name and url) or state != "WI":
            continue
        if SCHOOL_FILTER and name != SCHOOL_FILTER:
            continue
        out.append((name, url))
    return out


def _sport_label(a_tag):
    """Best-effort sport/level label for a TeamID link: prefer link text, fall
    back to the text of the table row or list item containing the link."""
    text = a_tag.get_text(" ", strip=True)
    if text and not text.lower() in ("schedule", "view", "view schedule", "team"):
        return text
    parent = a_tag.find_parent(["tr", "li", "div"])
    if parent:
        ptext = parent.get_text(" ", strip=True)
        ptext = re.sub(r"\b(view\s+)?schedule\b", "", ptext, flags=re.I).strip(" -|·")
        if ptext:
            return re.sub(r"\s+", " ", ptext)[:80]
    return text or "Unknown"


def discover_teams(school_name, school_url, dump=False):
    """Fetch the WIAA school page and return [{'sport':…, 'team_id':…}, …]."""
    try:
        resp = requests.get(school_url, headers=WIAA_HEADERS, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        print(f"  [WARN] {school_name}: fetch failed — {e}")
        return []

    if dump:
        p = Path(f"dump_school_{re.sub(r'[^A-Za-z0-9]+', '_', school_name)}.html")
        p.write_text(resp.text, encoding="utf-8")
        print(f"  [DUMP] Wrote {p}")

    soup = BeautifulSoup(resp.text, "html.parser")

    teams = {}
    for a in soup.find_all("a", href=True):
        m = TEAMID_RE.search(a["href"])
        if not m:
            continue
        team_id = m.group(1)
        label = _sport_label(a)
        if team_id not in teams:
            teams[team_id] = label

    if not teams:
        # Fallback: scan raw HTML for TeamID values even outside <a> tags
        for m in TEAMID_RE.finditer(resp.text):
            teams.setdefault(m.group(1), "Unknown")
        if teams:
            print(f"  [INFO] {school_name}: TeamIDs found via raw scan (no labels)")

    return [{"sport": label, "team_id": tid} for tid, label in teams.items()]


def main():
    print("=" * 60)
    print(f"  Discover Team IDs  |  filter sport='{SPORT_FILTER or 'all'}' "
          f"school='{SCHOOL_FILTER or 'all'}'")
    print("=" * 60)

    gc = get_gspread_client()
    schools = load_wi_schools(gc)
    print(f"\nWI schools on sheet: {len(schools)}\n")
    if not schools:
        print("Nothing to do.")
        sys.exit(0)

    rows = []
    first = True
    for name, url in schools:
        print(f"[WI] {name}")
        teams = discover_teams(name, url, dump=DUMP_HTML and first)
        first = False
        kept = 0
        for t in teams:
            if SPORT_FILTER and SPORT_FILTER not in t["sport"].lower():
                continue
            rows.append({
                "School Name": name,
                "State":       "WI",
                "Sport":       t["sport"],
                "TeamID":      t["team_id"],
                "Notes":       "",
            })
            kept += 1
        print(f"  → {len(teams)} team link(s) found, {kept} kept")
        time.sleep(DELAY)

    rows.sort(key=lambda r: (r["School Name"], r["Sport"]))
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["School Name", "State", "Sport", "TeamID", "Notes"])
        w.writeheader()
        w.writerows(rows)

    print(f"\n[DONE] Wrote {len(rows)} team row(s) to {OUT_CSV}")
    if not rows:
        print("[WARN] No teams discovered — the school pages may not link to "
              "schedules directly. Re-run with DUMP_HTML=1 and check the artifact.")


if __name__ == "__main__":
    main()
