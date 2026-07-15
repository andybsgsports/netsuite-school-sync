"""
probe_standings.py — diagnostic round 2. POSTs the WIAA ScoreCenter
StandingList endpoint (found in round 1) to learn the standings table
format. Prints structure into the workflow log. Runs from GitHub Actions.
"""

import re
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://schools.wiaawi.org/ScoreCenter/Conference/Standings",
    "Origin": "https://schools.wiaawi.org",
}

BASE = "https://schools.wiaawi.org"


def show(label, resp):
    print("=" * 70)
    print(f"{label}: HTTP {resp.status_code} bytes={len(resp.text)}")
    soup = BeautifulSoup(resp.text, "html.parser")
    for sel in soup.find_all("select")[:6]:
        opts = [(o.get("value"), o.get_text(strip=True)) for o in sel.find_all("option")]
        print(f"SELECT name={sel.get('name')} id={sel.get('id')} nopts={len(opts)} "
              f"first={opts[:6]}")
        # print any baseball/softball option
        for v, t in opts:
            if re.search(r"baseball|softball", t, re.I):
                print(f"    SPORT OPT: {v!r} {t!r}")
    for t in soup.find_all("table")[:6]:
        trs = t.find_all("tr")
        head = trs[0] if trs else None
        cols = ([c.get_text(' ', strip=True) for c in head.find_all(['th', 'td'])]
                if head else [])
        print(f"TABLE id={t.get('id')} class={t.get('class')} rows={len(trs)} headers={cols}")
        for tr in trs[1:6]:
            print(f"    ROW: {[c.get_text(' ', strip=True) for c in tr.find_all(['td','th'])]}")
    if not soup.find_all("table"):
        text = soup.get_text(" ", strip=True)[:600]
        print(f"NO TABLES; text: {text}")
    return soup


s = requests.Session()
s.headers.update(HEADERS)

# Step 0: load the Standings page to pick up cookies / antiforgery token
r0 = s.get(f"{BASE}/ScoreCenter/Conference/Standings", timeout=20)
soup0 = BeautifulSoup(r0.text, "html.parser")
token_el = soup0.find("input", {"name": "__RequestVerificationToken"})
token = token_el["value"] if token_el else ""
print(f"Antiforgery token found: {bool(token)}")

# Step 1: POST for school year 2025 (=2025-26) with no sport — hoping the
# response carries that year's SportSeasonID options
data1 = {"Options.SchoolYear": "2025", "Options.SportSeasonID": "0",
         "Options.ConferenceID": "0"}
if token:
    data1["__RequestVerificationToken"] = token
r1 = s.post(f"{BASE}/ScoreCenter/Conference/StandingList", data=data1, timeout=20)
soup1 = show("POST StandingList year=2025 sport=0 conf=ALL", r1)

# Step 2: find a 2025-26 baseball/softball SportSeasonID anywhere in either
# response and POST for its statewide standings
ssid = None
for soup in (soup1, soup0):
    for sel in soup.find_all("select"):
        if "SportSeasonID" not in str(sel.get("name")):
            continue
        for o in sel.find_all("option"):
            label = o.get_text(strip=True)
            if re.search(r"baseball", label, re.I) and ("2025" in label or "SchoolYear" not in str(sel)):
                ssid = o.get("value")
                print(f"Chose SSID {ssid} ({label})")
                break
        if ssid:
            break
    if ssid:
        break

if not ssid:
    # Fall back: 2026-27 Boys Baseball SSID from round 1
    ssid = "1535"
    print("Falling back to SSID 1535 (2026-27 Boys Baseball)")

data2 = {"Options.SchoolYear": "2025", "Options.SportSeasonID": ssid,
         "Options.ConferenceID": "0"}
if token:
    data2["__RequestVerificationToken"] = token
r2 = s.post(f"{BASE}/ScoreCenter/Conference/StandingList", data=data2, timeout=20)
show(f"POST StandingList year=2025 sport={ssid} conf=ALL", r2)
