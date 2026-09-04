"""
probe_conference.py — diagnostic round 2. The Conference Listing page has
Year / Sport / Conference dropdowns (138 conferences). Find the form or
endpoint it submits to and fetch one conference's member list.
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

r = s.get(f"{BASE}/Directory/Conference/Listing", timeout=20)
soup = BeautifulSoup(r.text, "html.parser")

print("=== FORMS on Listing page ===")
for f in soup.find_all("form"):
    inputs = [(i.get("name"), i.get("type"), (i.get("value") or "")[:20])
              for i in f.find_all("input")][:8]
    print(f"FORM action={f.get('action')} method={f.get('method')} inputs={inputs}")

print("=== JS endpoints mentioning Conference ===")
for m in sorted(set(re.findall(r"""["'](/Directory/Conference/[A-Za-z0-9_./?=&-]*)["']""", r.text))):
    print("  ", m)
for m in sorted(set(re.findall(r"""url\s*:\s*["']([^"']+)["']""", r.text)))[:15]:
    print("  ajax url:", m)

sel_year = soup.find("select", id="YearSel")
sel_ssid = soup.find("select", id="SSIDSel")
sel_conf = soup.find("select", id="ConfSel")
opts = lambda sel: [(o.get("value"), o.get_text(strip=True)) for o in sel.find_all("option")] if sel else []
ssids, confs = opts(sel_ssid), opts(sel_conf)
print(f"=== {len(confs)} conferences ===")
print([t for _, t in confs][:138])
soccer = next(((v, t) for v, t in ssids if "Boys Soccer" in t), None)
badger = [(v, t) for v, t in confs if "Badger" in t or "Rock Valley" in t]
print("Boys Soccer SSID:", soccer, "| Badger-ish conferences:", badger)

# Try the obvious submissions: POST to Listing, POST to a *List endpoint, GET with params
year = "2026"
cid = badger[0][0] if badger else confs[1][0]
payload = {"SchoolYear": year, "SportSeasonID": soccer[0] if soccer else "0",
           "Conf.ConferenceID": cid}
tok = soup.find("input", {"name": "__RequestVerificationToken"})
if tok:
    payload["__RequestVerificationToken"] = tok["value"]


def show(label, resp):
    print("=" * 70)
    print(f"{label}: HTTP {resp.status_code} bytes={len(resp.text)}")
    sp = BeautifulSoup(resp.text, "html.parser")
    tables = sp.find_all("table")
    for t in tables[:3]:
        trs = t.find_all("tr")
        head = [c.get_text(" ", strip=True) for c in trs[0].find_all(["th", "td"])] if trs else []
        print(f"TABLE id={t.get('id')} rows={len(trs)} headers={head}")
        for tr in trs[1:8]:
            cells = [c.get_text(" ", strip=True)[:35] for c in tr.find_all(["td", "th"])]
            links = [a["href"][:80] for a in tr.find_all("a", href=True)][:2]
            print(f"    ROW {cells} links={links}")
    if not tables:
        body = sp.find("main") or sp
        print("no tables; text:", body.get_text(" ", strip=True)[:500])


for path in ("/Directory/Conference/Listing", "/Directory/Conference/ListingList",
             "/Directory/Conference/ConferenceListing", "/Directory/Conference/Schools"):
    try:
        show(f"POST {path} {payload}", s.post(BASE + path, data=payload, timeout=20))
    except Exception as e:
        print(f"POST {path} failed: {e}")

try:
    show(f"GET Listing?params", s.get(f"{BASE}/Directory/Conference/Listing", params=payload, timeout=20))
except Exception as e:
    print("GET failed:", e)
