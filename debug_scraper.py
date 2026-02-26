"""
debug_scraper.py - run this locally to test WIAA scraping
"""
import requests
from bs4 import BeautifulSoup
import re

url = "https://schools.wiaawi.org/Directory/School/GetDirectorySchool?orgID=462"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://schools.wiaawi.org/Directory/School/List",
}

print("Fetching WIAA page...")
resp = requests.get(url, headers=headers, timeout=12)
print(f"Status: {resp.status_code}")
print(f"Content length: {len(resp.text)}")

soup = BeautifulSoup(resp.text, "html.parser")

# Print all <p> tags to see structure
print("\n=== All <p> tags ===")
for p in soup.find_all("p"):
    txt = p.get_text(strip=True)
    if txt:
        print(f"  <p>: {txt[:80]}")

print("\n=== All <h5> tags ===")
for h in soup.find_all("h5"):
    txt = h.get_text(strip=True)
    if txt:
        print(f"  <h5>: {txt[:80]}")
