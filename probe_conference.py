"""
probe_conference.py — diagnostic round 3. POST the Conference directory's
ConferenceTeams endpoint (found in round 2) to see the member-team format.
"""

import re
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://schools.wiaawi.org/Directory/Conference/Listing",
    "Origin": "https://schools.wiaawi.org",
}
BASE = "https://schools.wiaawi.org"
s = requests.Session()
s.headers.update(HEADERS)
s.get(f"{BASE}/Directory/Conference/Listing", timeout=20)  # cookies


def show(label, resp, max_rows=10):
    print("=" * 70)
    print(f"{label}: HTTP {resp.status_code} bytes={len(resp.text)}")
    sp = BeautifulSoup(resp.text, "html.parser")
    tables = sp.find_all("table")
    for t in tables[:4]:
        trs = t.find_all("tr")
        head = [c.get_text(" ", strip=True) for c in trs[0].find_all(["th", "td"])] if trs else []
        print(f"TABLE id={t.get('id')} rows={len(trs)} headers={head}")
        for tr in trs[1:1 + max_rows]:
            cells = [c.get_text(" ", strip=True)[:32] for c in tr.find_all(["td", "th"])]
            links = [a["href"][:70] for a in tr.find_all("a", href=True)][:2]
            print(f"    ROW {cells} links={links}")
    if not tables:
        main = sp.find("main") or sp
        txt = main.get_text(" ", strip=True)
        i = txt.find("Team Members")
        print("no tables; text:", txt[max(0, i):i + 700] if i >= 0 else txt[:700])
    ids = sorted(set(re.findall(r"TeamID=(\d+)", resp.text)))
    print(f"TeamID links in response: {len(ids)} e.g. {ids[:8]}")
    return sp


EP = f"{BASE}/Directory/Conference/ConferenceTeams"
# Boys Soccer 2026-27 (SSID 1541) — Badger (5274) and its Large/Small splits
for cid, name in (("5274", "Badger"), ("5324", "Badger - Large"), ("5325", "Badger - Small")):
    data = {"SchoolYear": "2026", "SportSeasonID": "1541",
            "Conf.ConferenceID": cid, "IsAdmin": "False"}
    show(f"POST ConferenceTeams {name} boys soccer {data}", s.post(EP, data=data, timeout=20))

# Without a sport: does it list all sports for the conference at once?
data = {"SchoolYear": "2026", "SportSeasonID": "0", "Conf.ConferenceID": "5274", "IsAdmin": "False"}
show(f"POST ConferenceTeams Badger, no sport {data}", s.post(EP, data=data, timeout=20), max_rows=16)

# Alternate field spellings, in case the form binds differently
for alt in ({"Conf.SchoolYear": "2026", "Conf.SportSeasonID": "1541", "Conf.ConferenceID": "5274", "IsAdmin": "False"},
            {"SchoolYear": "2026", "SSID": "1541", "ConferenceID": "5274"}):
    show(f"POST ConferenceTeams alt {alt}", s.post(EP, data=alt, timeout=20), max_rows=5)
