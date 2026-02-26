"""
ns_test_create.py
Updates Webster customer with split Bill-To (PO Box) and Ship-To (physical) addresses.
"""
from netsuite_sync import (ns_patch, ns_get, scrape_wiaa_school_detail,
                            build_customer_body, get_contact_by_external_id,
                            make_contact_external_id, NS_ACCOUNT)

CUSTOMER_ID = "6908"
WIAA_URL    = "https://schools.wiaawi.org/Directory/School/GetDirectorySchool?orgID=462"

print("=== Updating Webster High School ===\n")
print("  Scraping WIAA...")
school_info = scrape_wiaa_school_detail(WIAA_URL)
print(f"  Address1 (Ship-To): {school_info.get('address1')}")
print(f"  Address2 (Bill-To): {school_info.get('address2')}")
print(f"  City/St/Zip: {school_info.get('city')} {school_info.get('state')} {school_info.get('zip')}")
print(f"  Website: {school_info.get('website')}")

# Step 1: Clear existing addressbook first to avoid duplicates
print("\n  Clearing existing addresses...")
r = ns_patch(f"customer/{CUSTOMER_ID}", {"addressbook": {"items": []}})
print(f"  Clear status: {r.status_code}")

# Step 2: Patch with full body including split addresses
body = build_customer_body("Webster", "WI", school_info)
print(f"\n  Patching with split addresses...")
r = ns_patch(f"customer/{CUSTOMER_ID}", body)
print(f"  Status: {r.status_code}")
if r.status_code == 204:
    print(f"  ✅ Updated!")
    print(f"  View: https://{NS_ACCOUNT}.app.netsuite.com/app/common/entity/custjob.nl?id={CUSTOMER_ID}")
else:
    print(f"  ❌ Error: {r.text[:400]}")

# Contact status
print("\n  Checking Taran Wols contact...")
ext_id = make_contact_external_id("Webster", "twols@webster.k12.wi.us", "Athletic Director")
contact_id, is_inactive = get_contact_by_external_id(ext_id)
if contact_id:
    print(f"  ✅ Contact found (ID: {contact_id})")
else:
    print(f"  Contact exists in NetSuite but needs external ID set manually to:")
    print(f"  {ext_id}")

print("\nDone!")
