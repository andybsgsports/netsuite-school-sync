"""
diag_group_audience.py — read-only. Inspect a NetSuite CRM Group record
(the "Groups" list under Lists > Relationships, e.g. "Baseball Coaches",
93697) to see what the REST record API exposes for it: static member list,
or just a pointer to the saved search that computes it dynamically.

This sync has never written to group records — grep confirms no code path
touches `group/`. These groups are built and owned by Andrew Murray in
NetSuite directly. This diagnostic exists to answer one question: does the
2026-09-17 contact dedup (603 duplicates retired, isInactive=True) actually
clean up these "Email Audience" groups too, or do they need separate work?

If the group is DYNAMIC (driven by a saved search), NetSuite computes its
membership live at send time from the search criteria — nothing here can
retire a member directly; whether retired duplicates still show up depends
entirely on whether that saved search filters isinactive = 'F'. If the
group is STATIC, it holds an explicit member list that could itself contain
stale/duplicate contact ids independent of the Contact records.

Env: DIAG_GROUP_ID (default 93697 = Baseball Coaches)
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import requests

from netsuite_sync import ns_get, make_auth, NS_ACCOUNT

GROUP_ID = os.environ.get("DIAG_GROUP_ID", "93697").strip()

# Table/view names to try via SuiteQL — the REST Record API v1 doesn't
# support 'group' at all (confirmed: 404 NONEXISTENT_ID), so this is the
# fallback for seeing the group's own fields (name, dynamic vs static,
# linked saved search) and, if exposed, its computed membership.
SUITEQL_CANDIDATES = [
    f"SELECT * FROM group WHERE id = {GROUP_ID}",
    f"SELECT * FROM contactgroup WHERE id = {GROUP_ID}",
    f"SELECT * FROM groupmember WHERE group = {GROUP_ID}",
    f"SELECT * FROM contactGroupMember WHERE grouped = {GROUP_ID}",
]


SUITEQL_URL = f"https://{NS_ACCOUNT}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"


def raw_suiteql(query):
    """Like netsuite_sync.ns_suiteql, but surfaces the HTTP status and error
    body instead of swallowing a failure into [] — this script needs to
    tell 'table doesn't exist' apart from 'genuinely 0 rows'."""
    url = f"{SUITEQL_URL}?limit=10"
    r = requests.post(url, headers={
        "Authorization": make_auth("POST", url),
        "Content-Type": "application/json",
        "Prefer": "transient",
    }, json={"q": query})
    return r.status_code, (r.json().get("items", []) if r.status_code == 200 else r.text)


def try_suiteql():
    print("\n" + "-" * 70)
    print("SuiteQL fallback (REST Record API has no 'group' record type)")
    print("-" * 70)
    for q in SUITEQL_CANDIDATES:
        status, result = raw_suiteql(q)
        print(f"\n  {q}")
        if status == 200:
            print(f"    -> 200, {len(result)} row(s)"
                  + (f": {json.dumps(result[:3])}" if result else ""))
        else:
            print(f"    -> {status}: {str(result)[:300]}")


def try_metadata_catalog():
    """List every record type the REST Record API actually supports, so we
    can see whether anything group/audience-shaped exists under another
    name instead of guessing endpoint spellings one at a time."""
    print("\n" + "-" * 70)
    print("Metadata catalog — record types matching group/audience/contact")
    print("-" * 70)
    r = requests.get(
        f"https://{NS_ACCOUNT}.suitetalk.api.netsuite.com/services/rest/record/v1/metadata-catalog",
        headers={"Authorization": make_auth("GET", f"https://{NS_ACCOUNT}"
                  ".suitetalk.api.netsuite.com/services/rest/record/v1/metadata-catalog"),
                  "Accept": "application/swagger+json"})
    print(f"GET metadata-catalog -> HTTP {r.status_code}")
    if r.status_code != 200:
        print(r.text[:500])
        return
    try:
        names = [i.get("name", "") for i in r.json().get("items", [])]
    except Exception:
        print("(couldn't parse catalog body)")
        return
    hits = sorted(n for n in names if any(k in n.lower()
                  for k in ("group", "audience", "contact", "campaign")))
    print(f"{len(names)} record types total; matches: {hits}")


def main():
    print("=" * 70)
    print(f"  GROUP AUDIENCE DIAGNOSTIC  |  group {GROUP_ID}  (read-only)")
    print("=" * 70)

    r = ns_get(f"group/{GROUP_ID}")
    print(f"\nGET group/{GROUP_ID} -> HTTP {r.status_code}")
    if r.status_code != 200:
        print(r.text[:1500])
        try_suiteql()
        try_metadata_catalog()
        return
    body = r.json()
    print(json.dumps(body, indent=2)[:4000])

    # Try common sublist names NetSuite might expose for group membership.
    for sub in ("groupMemberList", "memberList", "contactList", "members"):
        if sub in body:
            print(f"\nFound sublist '{sub}' inline: "
                  f"{json.dumps(body[sub], indent=2)[:2000]}")
            continue
        r2 = ns_get(f"group/{GROUP_ID}/{sub}")
        if r2.status_code == 200:
            print(f"\nGET group/{GROUP_ID}/{sub} -> 200")
            print(json.dumps(r2.json(), indent=2)[:2000])
        elif r2.status_code not in (404,):
            print(f"\nGET group/{GROUP_ID}/{sub} -> {r2.status_code} "
                  f"{r2.text[:200]}")


if __name__ == "__main__":
    main()
