"""
probe_standings.py — one-off diagnostic. Fetches the WIAA ScoreCenter
conference standings pages and prints their structure (forms, selects,
tables, links) into the workflow log so the standings scraper can be
written against the real markup. Runs from GitHub Actions only.
"""

import re
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer":         "https://schools.wiaawi.org/Directory/School/List",
}

URLS = [
    "https://schools.wiaawi.org/ScoreCenter/Conference/Standings",
    "https://schools.wiaawi.org/ScoreCenter/Conference/Schedule",
    # Mount Horeb 2025-26 girls softball — schedule page, checking for any
    # standings/conference link specific to the team's conference
    "https://schools.wiaawi.org/Directory/Schedule/Index?TeamID=127196",
]


def probe(url):
    print("=" * 70)
    print(f"URL: {url}")
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        print(f"HTTP {r.status_code}  final={r.url}  bytes={len(r.text)}")
        soup = BeautifulSoup(r.text, "html.parser")
        title = soup.find("title")
        print(f"TITLE: {title.get_text(strip=True) if title else '?'}")

        for f in soup.find_all("form")[:5]:
            print(f"FORM action={f.get('action')} method={f.get('method')}")

        for sel in soup.find_all("select")[:8]:
            opts = [(o.get("value"), o.get_text(strip=True))
                    for o in sel.find_all("option")[:8]]
            print(f"SELECT name={sel.get('name')} id={sel.get('id')} opts={opts}")

        for t in soup.find_all("table")[:4]:
            head = t.find("tr")
            cols = ([c.get_text(' ', strip=True) for c in head.find_all(['th', 'td'])]
                    if head else [])
            print(f"TABLE id={t.get('id')} rows={len(t.find_all('tr'))} headers={cols}")

        seen = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if re.search(r"standing|conference", href, re.I) and href not in seen:
                seen.add(href)
                print(f"LINK {a.get_text(' ', strip=True)!r} -> {href[:180]}")
            if len(seen) >= 15:
                break

        # Script-embedded endpoints (ajax data sources)
        for m in re.finditer(r"""["'](/[A-Za-z0-9/_.-]*(?:Standing|Conference)[A-Za-z0-9/_.?=&-]*)["']""",
                             r.text):
            print(f"JSREF {m.group(1)[:180]}")
    except Exception as e:
        print(f"ERROR: {e}")


if __name__ == "__main__":
    for u in URLS:
        probe(u)
