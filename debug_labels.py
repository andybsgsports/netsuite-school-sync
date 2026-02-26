"""
debug_labels.py - run locally to show label structure for two schools
One with PO Box (Webster) and one without (Beloit Memorial)
"""
import requests
from bs4 import BeautifulSoup

WIAA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://schools.wiaawi.org/Directory/School/List",
}

SCHOOLS = [
    ("Webster",         "https://schools.wiaawi.org/Directory/School/GetDirectorySchool?orgID=462"),
    ("Beloit Memorial", "https://schools.wiaawi.org/Directory/School/GetDirectorySchool?orgID=36"),
]

NAV_H5S = {
    "Schools", "Contests", "General", "Tournaments", "Conferences",
    "School", "Conference", "Officials", "All Sports", "Football",
    "Golf", "Soccer",
}

for name, url in SCHOOLS:
    print(f"\n{'='*60}")
    print(f"SCHOOL: {name}")
    print(f"{'='*60}")
    resp = requests.get(url, headers=WIAA_HEADERS, timeout=15)
    soup = BeautifulSoup(resp.text, "html.parser")

    # Show all content h5s with index
    content_h5s = [h.get_text(strip=True) for h in soup.find_all("h5")
                   if h.get_text(strip=True) and h.get_text(strip=True) not in NAV_H5S]
    print(f"\nContent h5s ({len(content_h5s)} total):")
    for i, v in enumerate(content_h5s):
        print(f"  [{i:02d}] {v}")

    import time; time.sleep(1)
