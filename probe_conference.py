"""
probe_conference.py — diagnostic. Inspects the WIAA conference directory
pages to learn how to get authoritative conference membership per sport
(so standings rank the true conference instead of an inferred cluster).
Prints structure into the workflow log. Runs from GitHub Actions only.
"""

import re
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://schools.wiaawi.org/Directory/School/List",
}
BASE = "https://schools.wiaawi.org"
s = requests.Session()
s.headers.update(HEADERS)


def show(label, r, max_rows=6):
    print("=" * 70)
    print(f"{label}: HTTP {r.status_code} bytes={len(r.text)} final={r.url}")
    soup = BeautifulSoup(r.text, "html.parser")
    for t in soup.find_all("table")[:4]:
        trs = t.find_all("tr")
        head = [c.get_text(" ", strip=True) for c in trs[0].find_all(["th", "td"])] if trs else []
        print(f"TABLE id={t.get('id')} rows={len(trs)} headers={head}")
        for tr in trs[1:1 + max_rows]:
            cells = [c.get_text(" ", strip=True)[:40] for c in tr.find_all(["td", "th"])]
            links = [a["href"][:90] for a in tr.find_all("a", href=True)][:2]
            print(f"    ROW {cells}  links={links}")
    for sel in soup.find_all("select")[:4]:
        opts = [(o.get("value"), o.get_text(strip=True)) for o in sel.find_all("option")]
        print(f"SELECT name={sel.get('name')} id={sel.get('id')} n={len(opts)} first={opts[:6]}")
    seen = 0
    for a in soup.find_all("a", href=True):
        h = a["href"]
        if re.search(r"conference", h, re.I) and "Listing" not in h and "List" != h.split("/")[-1]:
            print(f"LINK {a.get_text(' ', strip=True)[:40]!r} -> {h[:120]}")
            seen += 1
            if seen >= 12:
                break
    return soup


soup = show("Conference Listing", s.get(f"{BASE}/Directory/Conference/Listing", timeout=20))
show("Conference List", s.get(f"{BASE}/Directory/Conference/List", timeout=20))

# Follow the first conference detail link we can find
detail = None
for a in soup.find_all("a", href=True):
    if re.search(r"Conference/(Detail|Index|School|View)", a["href"], re.I) or "ConferenceID=" in a["href"]:
        detail = a["href"]
        break
if detail:
    url = detail if detail.startswith("http") else BASE + detail
    dsoup = show(f"Conference detail: {url}", s.get(url, timeout=20), max_rows=12)
    # Sport-specific membership? print any text mentioning sports next to schools
    txt = dsoup.get_text(" ", strip=True)
    for kw in ("Football", "Soccer", "Volleyball", "Basketball"):
        i = txt.find(kw)
        if i >= 0:
            print(f"TEXT near '{kw}': {txt[max(0, i-120):i+160]!r}")
else:
    print("No conference detail link found on the listing page.")

# Also: does a team schedule page name its conference anywhere?
r = s.get(f"{BASE}/Directory/Schedule/Index?TeamID=146992", timeout=20)  # Menomonee Falls boys soccer
t = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
for m in re.finditer(r"(?i)conference", t):
    print("SCHEDULE PAGE ctx:", t[max(0, m.start()-80):m.start()+80].replace("\n", " "))
    break
