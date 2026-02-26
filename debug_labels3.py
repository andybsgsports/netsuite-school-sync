import requests
from bs4 import BeautifulSoup
import time

WIAA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://schools.wiaawi.org/Directory/School/List",
}

NAV_H5S = {
    "Schools", "Contests", "General", "Tournaments", "Conferences",
    "School", "Conference", "Officials", "All Sports", "Football",
    "Golf", "Soccer",
}

SCHOOLS = [
    ("Brookfield Academy",  "https://schools.wiaawi.org/Directory/School/GetDirectorySchool?orgID=51"),
    ("Janesville Parker",   "https://schools.wiaawi.org/Directory/School/GetDirectorySchool?orgID=176"),
    ("Menomonee Falls",     "https://schools.wiaawi.org/Directory/School/GetDirectorySchool?orgID=245"),
]

for name, url in SCHOOLS:
    print(f"\n{'='*50}")
    print(f"SCHOOL: {name}")
    print(f"{'='*50}")
    resp = requests.get(url, headers=WIAA_HEADERS, timeout=15)
    soup = BeautifulSoup(resp.text, "html.parser")
    content_h5s = [h.get_text(strip=True) for h in soup.find_all("h5")
                   if h.get_text(strip=True) and h.get_text(strip=True) not in NAV_H5S]
    for i, v in enumerate(content_h5s):
        print(f"  [{i:02d}] {v}")
    time.sleep(1)
