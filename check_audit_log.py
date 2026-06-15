"""
check_audit_log.py — check NetSuite login/audit trail for the integration token.

Looks for any API activity on token NS_TOKEN_KEY between 2026-02-26 (when
credentials were first committed) and today, to detect unauthorized use.

Tries multiple endpoints since role permissions vary:
  1. REST loginaudit record list
  2. SuiteQL loginaudit (may be blocked)
  3. REST systemNote (change log)

Env: NS_*
"""
from __future__ import annotations
import os, requests
from netsuite_sync import ns_get, NS_ACCOUNT, make_auth, SUITEQL_URL

TOKEN_KEY = os.environ.get("NS_TOKEN_KEY", "")

def raw_suiteql(query):
    url = f"{SUITEQL_URL}?limit=50"
    r = requests.post(url, headers={
        "Authorization": make_auth("POST", url),
        "Content-Type":  "application/json",
        "Prefer":        "transient",
    }, json={"q": query})
    return r.status_code, r.text[:1000]

def main():
    print("=" * 60)
    print("NetSuite audit log check — looking for token activity")
    print(f"Account: {NS_ACCOUNT}")
    print(f"Token key (first 8): {TOKEN_KEY[:8]}...")
    print("=" * 60)

    # 1. Try REST loginaudit list
    print("\n1. REST loginaudit list")
    r = ns_get("loginaudit?limit=5")
    print(f"   HTTP {r.status_code}")
    if r.status_code == 200:
        items = r.json().get("items", [])
        print(f"   {len(items)} records returned")
        for item in items:
            print(f"   {item}")
    else:
        print(f"   {r.text[:300]}")

    # 2. Try SuiteQL loginaudit
    print("\n2. SuiteQL loginaudit (date >= '2026-02-26')")
    q = ("SELECT date, user, ipaddress, status, requesturi "
         "FROM loginaudit WHERE date >= '2026-02-26' ORDER BY date DESC")
    status, body = raw_suiteql(q)
    print(f"   HTTP {status}")
    print(f"   {body}")

    # 3. Try SuiteQL systemNote for changes via this token
    print("\n3. SuiteQL systemNote — recent changes")
    q2 = ("SELECT recordid, recordtype, date, field, newvalue, name "
          "FROM systemnote WHERE date >= '2026-02-26' ORDER BY date DESC")
    status2, body2 = raw_suiteql(q2)
    print(f"   HTTP {status2}")
    print(f"   {body2}")

    # 4. Try REST for audit trail on a known record
    print("\n4. REST systemNote for customer 2217")
    r2 = ns_get("customer/2217/!systemNotes")
    print(f"   HTTP {r2.status_code}")
    if r2.status_code == 200:
        items = r2.json().get("items", [])[:10]
        for item in items:
            print(f"   {item}")
    else:
        print(f"   {r2.text[:300]}")

    print("\nDone.")

if __name__ == "__main__":
    main()
