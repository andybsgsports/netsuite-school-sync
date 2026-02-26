"""
delete_duplicates.py
--------------------
Deletes the duplicate customer records created by accident (IDs 8714-8816).
Run: python delete_duplicates.py
"""
import requests
import time
from netsuite_sync import make_auth, NS_ACCOUNT

BASE_URL = f"https://{NS_ACCOUNT}.suitetalk.api.netsuite.com/services/rest/record/v1"

# All duplicate IDs created today by accident
DUPLICATE_IDS = list(range(8714, 8817))

def delete_customer(ns_id):
    url = f"{BASE_URL}/customer/{ns_id}"
    r = requests.delete(url, headers={
        "Authorization": make_auth("DELETE", url),
        "Content-Type": "application/json"
    })
    return r.status_code

print(f"Deleting {len(DUPLICATE_IDS)} duplicate customers...\n")
deleted = 0
errors = 0
for ns_id in DUPLICATE_IDS:
    status = delete_customer(ns_id)
    if status in (200, 204):
        print(f"  [DELETED] {ns_id}")
        deleted += 1
    elif status == 404:
        pass  # doesn't exist, skip silently
    else:
        print(f"  [ERROR] {ns_id} — HTTP {status}")
        errors += 1
    time.sleep(0.3)

print(f"\nDone. Deleted: {deleted}  Errors: {errors}")
