"""
probe_standings.py — diagnostic round 3. The StandingList POST returns
202KB with no <table> markup; this dumps the raw structure around school
names to reveal how standings rows are encoded. Runs from GitHub Actions.
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

s = requests.Session()
s.headers.update(HEADERS)
s.get(f"{BASE}/ScoreCenter/Conference/Standings", timeout=20)

# 2026-27 Boys Baseball statewide (SSID 1535 from round 1). The season just
# started so rows may be sparse, but the STRUCTURE is what we need.
r = s.post(f"{BASE}/ScoreCenter/Conference/StandingList",
           data={"Options.SchoolYear": "2026", "Options.SportSeasonID": "1535",
                 "Options.ConferenceID": "0"},
           timeout=30)
print(f"HTTP {r.status_code} bytes={len(r.text)}")
text = r.text

# Where do school-ish names appear? Show raw HTML around a few anchors
hits = [m.start() for m in re.finditer(r"Horeb|Barneveld|Waunakee|Kettle|Badger", text)][:4]
print(f"name hits: {len(hits)}")
for pos in hits:
    print("-" * 60)
    print(text[max(0, pos - 600):pos + 400].replace("\n", " ")[:1000])

# Frequency of interesting class names / element ids
for pat in (r'class="([^"]*grid[^"]*)"', r'class="([^"]*standing[^"]*)"',
            r'id="([A-Za-z]*[Ss]tanding[^"]*)"', r'class="([^"]*conf[^"]*)"'):
    counts = {}
    for m in re.finditer(pat, text, re.I):
        counts[m.group(1)] = counts.get(m.group(1), 0) + 1
    top = sorted(counts.items(), key=lambda kv: -kv[1])[:8]
    print(f"PATTERN {pat}: {top}")

# Any JSON blobs with W/L data?
for m in list(re.finditer(r'\{[^{}]*"[Ww]ins?"[^{}]*\}', text))[:3]:
    print("JSON-ish:", m.group(0)[:300])

# divs that repeat a lot (row containers)
soup = BeautifulSoup(text, "html.parser")
from collections import Counter
c = Counter(" ".join(d.get("class", [])) for d in soup.find_all("div"))
print("TOP DIV CLASSES:", c.most_common(10))

# print one representative repeated div's inner HTML
if c:
    top_class = [cls for cls, n in c.most_common(20) if n > 20 and cls]
    for cls in top_class[:2]:
        d = soup.find("div", class_=cls.split())
        print(f"SAMPLE DIV [{cls}]:", str(d)[:800])
