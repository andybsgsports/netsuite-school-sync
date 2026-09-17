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

from netsuite_sync import ns_get

GROUP_ID = os.environ.get("DIAG_GROUP_ID", "93697").strip()


def main():
    print("=" * 70)
    print(f"  GROUP AUDIENCE DIAGNOSTIC  |  group {GROUP_ID}  (read-only)")
    print("=" * 70)

    r = ns_get(f"group/{GROUP_ID}")
    print(f"\nGET group/{GROUP_ID} -> HTTP {r.status_code}")
    if r.status_code != 200:
        print(r.text[:1500])
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
