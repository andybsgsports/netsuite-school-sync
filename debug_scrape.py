import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from netsuite_sync import scrape_wiaa_school

url = "https://schools.wiaawi.org/Directory/School/GetDirectorySchool?orgID=29"
info, admins, coaches = scrape_wiaa_school(url)

print("=== School Info ===")
for k, v in info.items():
    print(f"  {k:20s} = {repr(v)}")
print(f"\nAdmins:  {len(admins)}")
print(f"Coaches: {len(coaches)}")
