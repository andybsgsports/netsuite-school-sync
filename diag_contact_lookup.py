"""
diag_contact_lookup.py — probe which NS contact-discovery mechanisms the
integration token can use. Read-only.

The "unique name" collision recovery needs to find the same-named contact
record a customer already owns, but customer?expand=contactList comes back
empty for these customers. This probes the alternatives so the recovery
can be built on whichever works:
  1. GET  customer/{id}?expand=contactList   (current, returns nothing)
  2. POST query/v1/suiteql contact-by-company query
  3. GET  contact?q=company EQUAL {id}       (REST collection search)

Env: DIAG_CUSTOMER_ID (default 2217), DIAG_EMAIL (optional), NS_* tokens
"""
from __future__ import annotations

import json
import os

import requests

from netsuite_sync import BASE_URL, NS_ACCOUNT, make_auth, ns_get

CUSTOMER = os.environ.get("DIAG_CUSTOMER_ID", "2217").strip()
EMAIL    = os.environ.get("DIAG_EMAIL", "").strip()

SUITEQL_URL = (f"https://{NS_ACCOUNT}.suitetalk.api.netsuite.com"
               f"/services/rest/query/v1/suiteql")


def show(label, status, text):
    print(f"\n--- {label}: HTTP {status}")
    print((text or "")[:900])


def main():
    r = ns_get(f"customer/{CUSTOMER}?expand=contactList")
    body = r.json() if r.status_code == 200 else {}
    cl = body.get("contactList")
    show("1. expand=contactList", r.status_code,
         json.dumps(cl)[:900] if cl is not None
         else f"(no contactList key; keys: {sorted(body.keys())[:40]})")

    q = (f"SELECT id, entityid, email, company FROM contact "
         f"WHERE company = {CUSTOMER}")
    headers = {
        "Authorization": make_auth("POST", SUITEQL_URL),
        "Content-Type":  "application/json",
        "Prefer":        "transient",
    }
    r = requests.post(SUITEQL_URL, headers=headers, json={"q": q})
    show("2. SuiteQL contact-by-company", r.status_code, r.text)

    url = f"contact?q=company EQUAL {CUSTOMER}&limit=50"
    r = ns_get(url)
    show("3. REST search contact?q=company", r.status_code, r.text)

    if EMAIL:
        r = ns_get(f'contact?q=email IS "{EMAIL}"&limit=10')
        show("4. REST search contact?q=email", r.status_code, r.text)


if __name__ == "__main__":
    main()
