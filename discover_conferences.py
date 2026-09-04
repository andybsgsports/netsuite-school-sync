"""
discover_conferences.py
-----------------------
Builds snapshots/conferences.json — authoritative conference membership per
sport from WIAA's conference directory — so the weekly scores email ranks
conference standings against the real conference (e.g. "Badger - Small"
boys soccer = 8 teams) instead of inferring membership from (C) games.

For the current school year, for every sport season × conference on
/Directory/Conference/Listing, POST /Directory/Conference/ConferenceTeams
and record the member teams (TeamID + name). ~4,000 small requests; a few
minutes with WORKERS parallel workers. Runs from GitHub Actions monthly
(with the TeamID discovery) or on demand.

Output shape:
  {"year": "2026", "built": "...",
   "conferences": {"<ssid>:<confid>": {"sport": "...", "conference": "...",
                                        "teams": {"<teamid>": "<name>", ...}}},
   "team_conf": {"<teamid>": "<ssid>:<confid>"}}
"""

import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

BASE = "https://schools.wiaawi.org"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": f"{BASE}/Directory/Conference/Listing",
    "Origin": BASE,
}
OUT_PATH = Path(__file__).parent / "snapshots" / "conferences.json"
WORKERS  = int(os.environ.get("WORKERS", "6") or "6")
DELAY    = 0.1
TEAMID_RE = re.compile(r"TeamID=(\d+)", re.I)


def listing_options(session):
    """(year, [(ssid, sport)], [(confid, conference)]) from the Listing page."""
    r = session.get(f"{BASE}/Directory/Conference/Listing", timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    def opts(sel_id):
        sel = soup.find("select", id=sel_id)
        return [(o.get("value", ""), o.get_text(" ", strip=True))
                for o in sel.find_all("option")] if sel else []

    years = [v for v, _ in opts("YearSel") if v.isdigit()]
    year = years[0] if years else str(datetime.now().year)   # first = current
    sports = [(v, re.sub(r"^\d{4}-\d{4}\s+", "", t))          # "2026-2027 Boys Soccer" -> "Boys Soccer"
              for v, t in opts("SSIDSel") if v.isdigit() and v != "0"]
    confs = [(v, t) for v, t in opts("ConfSel") if v.isdigit() and v != "0"]
    return year, sports, confs


def fetch_members(session, year, ssid, cid):
    """{teamid: name} for one sport season × conference (empty if none)."""
    data = {"SchoolYear": year, "SportSeasonID": ssid,
            "Conf.ConferenceID": cid, "IsAdmin": "False"}
    for attempt in range(3):
        try:
            r = session.post(f"{BASE}/Directory/Conference/ConferenceTeams",
                             data=data, timeout=20)
            r.raise_for_status()
            break
        except Exception as e:
            if attempt == 2:
                print(f"  [WARN] ssid={ssid} conf={cid}: {e}")
                return {}
            time.sleep(1.5 * (attempt + 1))
    soup = BeautifulSoup(r.text, "html.parser")
    teams = {}
    for a in soup.find_all("a", href=True):
        m = TEAMID_RE.search(a["href"])
        if m:
            teams[m.group(1)] = a.get_text(" ", strip=True)
    time.sleep(DELAY)
    return teams


def main():
    session = requests.Session()
    session.headers.update(HEADERS)
    year, sports, confs = listing_options(session)
    print(f"School year {year}: {len(sports)} sport seasons × {len(confs)} conferences "
          f"= {len(sports) * len(confs)} lookups")

    jobs = [(ssid, sport, cid, conf) for ssid, sport in sports for cid, conf in confs]

    def one(job):
        ssid, sport, cid, conf = job
        return job, fetch_members(session, year, ssid, cid)

    conferences, team_conf = {}, {}
    done = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for (ssid, sport, cid, conf), teams in ex.map(one, jobs):
            done += 1
            if done % 500 == 0:
                print(f"  ... {done}/{len(jobs)}")
            if not teams:
                continue
            key = f"{ssid}:{cid}"
            conferences[key] = {"sport": sport, "conference": conf, "teams": teams}
            for tid in teams:
                team_conf[tid] = key

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps({
        "year": year,
        "built": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "conferences": conferences,
        "team_conf": team_conf,
    }, indent=1), encoding="utf-8")

    sizes = [len(c["teams"]) for c in conferences.values()]
    print(f"[DONE] {len(conferences)} sport-conferences, {len(team_conf)} teams "
          f"(avg {sum(sizes) / max(len(sizes), 1):.1f} teams/conference) -> {OUT_PATH.name}")


if __name__ == "__main__":
    main()
