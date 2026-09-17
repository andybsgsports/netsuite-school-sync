"""
fix_group_audiences.py — fix the saved searches behind the coach "Groups"
(Lists > Relationships > Groups: Baseball Coaches, Boys Football Coaches,
Athletic Directors, ...) so they stop showing retired duplicate contacts.

These 13 groups are DYNAMIC: NetSuite recomputes each one's membership live
from its linked saved search, nothing is stored on the group itself. Andy
found "Ryan McKittrick (dup 54939)" (isInactive=Yes) still listed as a
member of "Boys Football Coaches" — the saved search has no
`Inactive = No` filter, so it shows every duplicate this sync has ever
retired, not just the September batch. Adding that one filter fixes it
permanently: nothing further needs to run, ever, since NetSuite always
recomputes from the search.

Also swaps a bare `phone` results column for `company` where present,
since Andy asked for the school name over the phone number in the member
list (screenshot: Boys Football Coaches member table).

This calls suitescript/group_audience_restlet.js — a SEPARATE RESTlet from
the one the nightly sync depends on (attach_contact_restlet.js), so a bug
here can never affect attach/detach. See RESTLET_SETUP.md for the deploy
pattern; use a NEW script record + deployment and set
NS_GROUP_RESTLET_SCRIPT_ID / NS_GROUP_RESTLET_DEPLOY_ID.

DRY RUN by default (reports what would change, saves nothing).
LIVE=1 applies it. GROUP_IDS overrides the built-in list (comma-separated).
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import requests

from netsuite_sync import NS_ACCOUNT, make_auth

LIVE = os.environ.get("LIVE", "").strip() in ("1", "true", "True", "yes")

NS_GROUP_RESTLET_SCRIPT_ID = os.environ.get("NS_GROUP_RESTLET_SCRIPT_ID", "").strip()
NS_GROUP_RESTLET_DEPLOY_ID = os.environ.get("NS_GROUP_RESTLET_DEPLOY_ID", "").strip()
RESTLET_URL = (f"https://{NS_ACCOUNT}.restlets.api.netsuite.com/app/site/hosting/restlet.nl"
               f"?script={NS_GROUP_RESTLET_SCRIPT_ID}&deploy={NS_GROUP_RESTLET_DEPLOY_ID}")

# The 13 coach/AD "Email Audience" groups, from Lists > Relationships >
# Groups (2026-09-17). Override with GROUP_IDS=1,2,3 if the account's ids
# ever change or to test against just one.
DEFAULT_GROUPS = {
    "93702": "Athletic Directors",
    "93697": "Baseball Coaches",
    "93691": "Boys Basketball Coaches",
    "93693": "Boys Football Coaches",
    "93699": "Boys Soccer Coaches",
    "93694": "Cross Country Coaches",
    "93692": "Girls Basketball Coaches",
    "93700": "Girls Soccer Coaches",
    "93690": "Girls Volleyball Coaches",
    "93701": "Gymnastics Coaches",
    "93698": "Softball Coaches",
    "93695": "Track and Field Coaches",
    "93696": "Wrestling Coaches",
}

_override = os.environ.get("GROUP_IDS", "").strip()
GROUP_IDS = [g.strip() for g in _override.split(",") if g.strip()] if _override \
    else list(DEFAULT_GROUPS)


def call_restlet(action, group_ids, dry_run=True):
    if not (NS_ACCOUNT and NS_GROUP_RESTLET_SCRIPT_ID and NS_GROUP_RESTLET_DEPLOY_ID):
        print("ERROR: NS_GROUP_RESTLET_SCRIPT_ID / NS_GROUP_RESTLET_DEPLOY_ID not set — "
              "see RESTLET_SETUP.md for the deploy steps, then add these as GitHub secrets.")
        sys.exit(1)
    body = {"action": action, "groupIds": [int(g) for g in group_ids]}
    if action == "fix":
        body["dryRun"] = dry_run
    r = requests.post(RESTLET_URL, headers={
        "Authorization": make_auth("POST", RESTLET_URL),
        "Content-Type": "application/json",
    }, json=body, timeout=120)
    if r.status_code != 200:
        print(f"ERROR: RESTlet HTTP {r.status_code}: {r.text[:500]}")
        sys.exit(1)
    data = r.json()
    if not data.get("success"):
        print(f"ERROR: RESTlet reported failure: {data.get('error')}")
        sys.exit(1)
    return data.get("results", [])


def main():
    print("=" * 70)
    print(f"  FIX GROUP AUDIENCES  |  LIVE={LIVE}  |  groups: {len(GROUP_IDS)}")
    print("=" * 70)

    results = call_restlet("fix", GROUP_IDS, dry_run=not LIVE)

    changed = would_change = errors = 0
    for r in results:
        name = DEFAULT_GROUPS.get(str(r.get("groupId")), r.get("groupName", "?"))
        print(f"\n{r.get('groupId')}  {name}")
        if r.get("error"):
            print(f"   ERROR: {r['error']}")
            errors += 1
            continue
        print(f"   saved search: {r.get('searchTitle')!r} (id {r.get('savedSearchId')}, "
              f"field {r.get('savedSearchFieldUsed')})")
        print(f"   currently: inactive-filter={r.get('hasInactiveFilter')}  "
              f"phone-column={r.get('hasPhoneColumn')}  company-column={r.get('hasCompanyColumn')}")
        wc = r.get("wouldChange", {})
        if wc.get("addInactiveFilter") or wc.get("swapPhoneForCompanyColumn"):
            would_change += 1
            print(f"   would change: addInactiveFilter={wc.get('addInactiveFilter')}  "
                  f"swapPhoneForCompanyColumn={wc.get('swapPhoneForCompanyColumn')}")
        else:
            print("   already correct — no change needed")
        if r.get("applied"):
            changed += 1
            print("   APPLIED")

    print("\n" + "=" * 70)
    if LIVE:
        print(f"  fixed: {changed}   already correct: "
              f"{len(results) - changed - errors}   errors: {errors}")
    else:
        print(f"  would fix: {would_change}   already correct: "
              f"{len(results) - would_change - errors}   errors: {errors}")
        print("  DRY RUN — nothing changed. Set LIVE=1 to apply.")
    print("=" * 70)


if __name__ == "__main__":
    main()
