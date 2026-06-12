"""
diag_horeb_stragglers4.py — identify what is blocking the rename of 48486/48485
back to their real names.

SuiteQL returned 0 rows even for the temp-named contacts we know exist, so the
integration token's SuiteQL access is unreliable. This script uses:
  1. GET contact/48486 and /48485 → print full JSON including entityid
  2. ns_suiteql with raw status/body logging to confirm whether SuiteQL works
  3. REST search by email: GET contact?q=email IS "timsarb@yahoo.com"
  4. REST search by first name fragment: GET contact?q=firstName IS "Tim"&limit=5
  5. Test PATCH to a definitely-unique name (e.g. "Tim Sarb_TESTONLY") to confirm
     the contact itself is patchable (distinguish "name taken" vs "no permission")

Env: NS_*
"""
from __future__ import annotations
import json, requests
from netsuite_sync import (
    ns_get, ns_patch,
    SUITEQL_URL, make_auth, NS_ACCOUNT,
)

TARGETS = [
    {"fix_cid": "48486", "first": "Tim",   "last": "Sarbacker",
     "temp_last": "SarbackerFIX2217", "email": "timsarb@yahoo.com"},
    {"fix_cid": "48485", "first": "Brett", "last": "Quale",
     "temp_last": "QualeFIX2217",     "email": "coachquale24@gmail.com"},
]


def raw_suiteql(query):
    url = f"{SUITEQL_URL}?limit=200"
    r = requests.post(url, headers={
        "Authorization": make_auth("POST", url),
        "Content-Type": "application/json",
        "Prefer": "transient",
    }, json={"q": query})
    return r.status_code, r.text[:600]


def main():
    # 1. Full GET of known contacts
    print("=" * 60)
    print("1. Full GET of known contacts")
    print("=" * 60)
    for t in TARGETS:
        cid = t["fix_cid"]
        r = ns_get(f"contact/{cid}")
        print(f"\n  contact/{cid}: HTTP {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            keys_of_interest = ["id", "firstName", "lastName", "email",
                                 "entityid", "company", "isInactive", "title"]
            for k in keys_of_interest:
                if k in data:
                    print(f"    {k}: {data[k]}")
        else:
            print(f"    {r.text[:300]}")

    # 2. SuiteQL raw status/body — test with known contact IDs
    print("\n" + "=" * 60)
    print("2. SuiteQL raw status (query by known IDs)")
    print("=" * 60)
    ids = ", ".join(t["fix_cid"] for t in TARGETS)
    q = f"SELECT id, firstName, lastName, entityid, company, isInactive FROM contact WHERE id IN ({ids})"
    status, body = raw_suiteql(q)
    print(f"  HTTP {status}")
    print(f"  {body}")

    # 3. SuiteQL for real names (including inactive)
    print("\n" + "=" * 60)
    print("3. SuiteQL for real names (any company, including inactive)")
    print("=" * 60)
    for t in TARGETS:
        first, last = t["first"], t["last"]
        q = (f"SELECT id, firstName, lastName, entityid, company, isInactive "
             f"FROM contact WHERE firstName = '{first}' AND lastName = '{last}'")
        status, body = raw_suiteql(q)
        print(f"\n  '{first} {last}': HTTP {status}")
        print(f"  {body}")

    # 4. REST search by email
    print("\n" + "=" * 60)
    print("4. REST search by email")
    print("=" * 60)
    for t in TARGETS:
        email = t["email"]
        r = ns_get(f'contact?q=email IS "{email}"&limit=10')
        print(f"\n  email={email}: HTTP {r.status_code}")
        print(f"  {r.text[:400]}")

    # 5. Test PATCH to a definitely-unique name
    print("\n" + "=" * 60)
    print("5. Test rename to real name (no rollback — leaves as-is if fails)")
    print("=" * 60)
    for t in TARGETS:
        cid = t["fix_cid"]
        first, last = t["first"], t["last"]
        print(f"\n  PATCH contact/{cid} → '{first} {last}'")
        r = ns_patch(f"contact/{cid}", {"firstName": first, "lastName": last})
        print(f"  HTTP {r.status_code}")
        if r.status_code != 204:
            print(f"  {r.text[:400]}")
        else:
            print(f"  SUCCESS — {first} {last} renamed at company 2217!")

    print("\nDone.")


if __name__ == "__main__":
    main()
