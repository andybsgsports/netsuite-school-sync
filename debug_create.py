import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from netsuite_sync import _h, BASE_URL
import requests, json, time

# Read all existing address labels on Barneveld
ab_url  = f"{BASE_URL}/customer/994/addressBook"
ab_resp = requests.get(ab_url, headers=_h("GET", ab_url))
items   = ab_resp.json().get("items", [])
print(f"Total addresses: {len(items)}")

# Read each one to get its label
for item in items:
    href = item.get("links", [{}])[0].get("href", "")
    r    = requests.get(href, headers=_h("GET", href))
    if r.status_code == 200:
        d = r.json()
        print(f"  ID: {d.get('id')}  label: {repr(d.get('label',''))}  billing: {d.get('defaultBilling')}  shipping: {d.get('defaultShipping')}")
    time.sleep(0.1)
