"""
test_shared_contact.py — test whether NS REST API supports sharing one
contact across multiple customers via contactRoles.

Uses Tim Sarbacker (contact 48486, already at customer 2217) as the test
subject. Attempts to also link him to customer 2290. Cleans up afterward.
"""
from __future__ import annotations
import requests
from netsuite_sync import ns_get, ns_post, ns_delete, NS_ACCOUNT, make_auth

CONTACT_ID   = "48486"   # Tim Sarbacker — exists at customer 2217
CUSTOMER_ID  = "2290"    # Mt. Horeb School District (parent) — safe test target

BASE = f"https://{NS_ACCOUNT}.suitetalk.api.netsuite.com/services/rest/record/v1"


def ns_post_raw(path, body):
    url = f"{BASE}/{path}"
    r = requests.post(url, headers={
        "Authorization": make_auth("POST", url),
        "Content-Type":  "application/json",
    }, json=body)
    return r


def ns_delete_raw(path):
    url = f"{BASE}/{path}"
    r = requests.delete(url, headers={
        "Authorization": make_auth("DELETE", url),
    })
    return r


def main():
    print("=" * 60)
    print("Test: share one contact across multiple customers")
    print(f"Contact: {CONTACT_ID}  |  Target customer: {CUSTOMER_ID}")
    print("=" * 60)

    # 1. Confirm contact exists
    r = ns_get(f"contact/{CONTACT_ID}")
    if r.status_code != 200:
        print(f"ABORT: contact/{CONTACT_ID} returned HTTP {r.status_code}")
        return
    c = r.json()
    print(f"\nContact confirmed: {c.get('firstName')} {c.get('lastName')} "
          f"(company id={c.get('company', {}).get('id')})")

    # 2. Check contact is NOT already at customer 2290
    r2 = ns_get(f"customer/{CUSTOMER_ID}/contactRoles?limit=100")
    existing_ids = []
    if r2.status_code == 200:
        for item in r2.json().get("items", []):
            cid = (item.get("contact") or {}).get("id")
            if cid:
                existing_ids.append(str(cid))
    if CONTACT_ID in existing_ids:
        print(f"\nContact {CONTACT_ID} already linked to customer {CUSTOMER_ID} — nothing to test.")
        return
    print(f"\nCustomer {CUSTOMER_ID} current contact count: {len(existing_ids)} (contact not present — good)")

    # 3. Attempt to POST the existing contact as a new contactRole
    print(f"\nAttempting POST customer/{CUSTOMER_ID}/contactRoles ...")
    body = {"contact": {"id": CONTACT_ID}}
    r3 = ns_post_raw(f"customer/{CUSTOMER_ID}/contactRoles", body)
    print(f"  HTTP {r3.status_code}")
    print(f"  Response: {r3.text[:500]}")

    if r3.status_code in (200, 201, 204):
        print("\nSUCCESS — NS REST API accepted the shared contact link.")

        # Extract the new role ID from Location header or response body
        location = r3.headers.get("Location", "")
        role_id = location.rstrip("/").split("/")[-1] if location else None
        if not role_id and r3.status_code != 204:
            try:
                role_id = r3.json().get("id")
            except Exception:
                pass
        print(f"  New contactRole ID: {role_id}")

        # Verify it appears in GET
        r4 = ns_get(f"customer/{CUSTOMER_ID}/contactRoles?limit=100")
        if r4.status_code == 200:
            new_ids = [(item.get("contact") or {}).get("id")
                       for item in r4.json().get("items", [])]
            linked = CONTACT_ID in [str(x) for x in new_ids]
            print(f"  Verified in GET contactRoles: {linked}")

        # Clean up — remove the test link
        if role_id:
            print(f"\nCleaning up: DELETE customer/{CUSTOMER_ID}/contactRoles/{role_id}")
            r5 = ns_delete_raw(f"customer/{CUSTOMER_ID}/contactRoles/{role_id}")
            print(f"  DELETE HTTP {r5.status_code}")
        else:
            print("\nCould not determine role ID — manual cleanup may be needed.")

    else:
        print(f"\nFAILED — API rejected the shared contact link.")
        print("  The per-school duplicate approach remains necessary.")

    print("\nDone.")


if __name__ == "__main__":
    main()
