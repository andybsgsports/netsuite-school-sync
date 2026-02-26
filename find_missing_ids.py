"""
find_missing_ids.py
-------------------
Searches NetSuite for schools with bad/missing IDs.
Uses ns_get from netsuite_sync.py (same auth that run_sync.py uses).
Run once, fix IDs in spreadsheet, then delete this file.

  python find_missing_ids.py
"""
from netsuite_sync import ns_get, slugify
import time

SCHOOLS = [
    "Elkhorn",
    "Greendale",
    "Greenfield",
    "Hamilton",
    "Kenosha Bradford",
    "Kenosha St. Joseph",
    "Menomonee Falls",
    "Oconomowoc",
    "Racine St. Catherine",
    "Waukesha North",
    "Waukesha South",
    "Waukesha West",
    "Wauwatosa West",
    "West Bend East",
    "West Bend West",
]

print("Searching NetSuite for missing schools...\n")

for school in SCHOOLS:
    ext_id = slugify(school)
    resp = ns_get(f"customer/{ext_id}?idtype=EXTERNAL_ID")

    if resp.status_code == 200:
        data = resp.json()
        ns_id = data.get("id", "")
        name = data.get("companyName", "")
        print(f"{school}:")
        print(f"  {ns_id:>6}  {name}  (Ext ID: {ext_id})")
    else:
        print(f"{school}:")
        print(f"  [NOT FOUND] Ext ID '{ext_id}' -- search manually in NetSuite")

    print()
    time.sleep(0.5)

print("Done. Update School_Master_List.xlsx with the correct IDs above.")
print("For NOT FOUND schools, search in NetSuite: Lists > Relationships > Customers")
