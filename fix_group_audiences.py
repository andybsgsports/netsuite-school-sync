"""
fix_group_audiences.py — fix the saved searches behind the coach "Groups"
(Lists > Relationships > Groups: Baseball Coaches, Boys Football Coaches,
Athletic Directors, ...) so they stop showing retired duplicate contacts,
and scope each one to only Andy's own accounts.

These 13 groups are DYNAMIC: NetSuite recomputes each one's membership live
from its linked saved search, nothing is stored on the group itself. Andy
found "Ryan McKittrick (dup 54939)" (isInactive=Yes) still listed as a
member of "Boys Football Coaches" — the saved search has no
`Inactive = No` filter, so it shows every duplicate this sync has ever
retired, not just the September batch. Adding that one filter fixes it
permanently: nothing further needs to run, ever, since NetSuite always
recomputes from the search.

Andy also asked each of the 13 be scoped to only contacts at schools
where HE is the Sales Rep (e.g. McHenry High School) — not every school
company-wide (Boys Football Coaches alone had 317 members). SALES_REP_ID
(NetSuite employee id, default "3" = Andrew Murray, see
netsuite_sync.SALES_REP_MAP) adds a company.salesrep filter alongside the
inactive one. Set SALES_REP_ID="" to skip this and only fix the inactive
filter.

Also adds a `company` results column when missing (unconditionally — not
only when a `phone` column happens to be present). NOTE: confirmed live
(2026-09-17, twice) this does NOT change what the Group's own "Members"
tab displays; that table is a FIXED NetSuite layout
(Name/Phone/Email/Bounced/Inactive/Subscription Status) unrelated to the
search's own Results columns — nothing can change what that specific
screen shows. It only affects the search's own view when run directly
(Lists > Search > Saved Searches) or exported to CSV — the real
workaround for "which school is this contact at".

v1 tried to find each search's id by loading the linked Group record via
SuiteScript — NetSuite rejected that ("The record type [GROUP] is
invalid": CRM Group isn't a SuiteScript-supported record type, same wall
REST hit). v2 needs each of the 13 saved searches' own internal ids
directly instead. Fastest way to get them: NetSuite UI ->
Lists > Search > Saved Searches, filter Type = Contact, find the 13
"<Sport> Coaches - Email Audience" / "Athletic Directors - Email
Audience" rows, read their ID column (a number, or a customsearch_...
script id — either works). Fill in SEARCH_IDS below or pass SEARCH_IDS_JSON.

This calls suitescript/group_audience_restlet.js — a SEPARATE RESTlet from
the one the nightly sync depends on (attach_contact_restlet.js), so a bug
here can never affect attach/detach. See RESTLET_SETUP.md for the deploy
pattern; NS_GROUP_RESTLET_SCRIPT_ID / NS_GROUP_RESTLET_DEPLOY_ID must be set.

DRY RUN by default (reports what would change and the resulting member
count, saves nothing). LIVE=1 applies it.
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import requests

from netsuite_sync import NS_ACCOUNT, make_auth

LIVE = os.environ.get("LIVE", "").strip() in ("1", "true", "True", "yes")
SALES_REP_ID = os.environ.get("SALES_REP_ID", "3").strip() or None

NS_GROUP_RESTLET_SCRIPT_ID = os.environ.get("NS_GROUP_RESTLET_SCRIPT_ID", "").strip()
NS_GROUP_RESTLET_DEPLOY_ID = os.environ.get("NS_GROUP_RESTLET_DEPLOY_ID", "").strip()
RESTLET_URL = (f"https://{NS_ACCOUNT}.restlets.api.netsuite.com/app/site/hosting/restlet.nl"
               f"?script={NS_GROUP_RESTLET_SCRIPT_ID}&deploy={NS_GROUP_RESTLET_DEPLOY_ID}")

# Fill these in from the Saved Searches list (see module docstring), or
# pass SEARCH_IDS_JSON='{"Baseball Coaches": 12345, ...}' as an env var
# without editing this file.
SEARCH_IDS = {
    "Athletic Directors": None,
    "Baseball Coaches": None,
    "Boys Basketball Coaches": None,
    "Boys Football Coaches": None,
    "Boys Soccer Coaches": None,
    "Cross Country Coaches": None,
    "Girls Basketball Coaches": None,
    "Girls Soccer Coaches": None,
    "Girls Volleyball Coaches": None,
    "Gymnastics Coaches": None,
    "Softball Coaches": None,
    "Track and Field Coaches": None,
    "Wrestling Coaches": None,
}

_override = os.environ.get("SEARCH_IDS_JSON", "").strip()
if _override:
    SEARCH_IDS = json.loads(_override)


def call_restlet(action, searches, dry_run=True):
    if not (NS_ACCOUNT and NS_GROUP_RESTLET_SCRIPT_ID and NS_GROUP_RESTLET_DEPLOY_ID):
        print("ERROR: NS_GROUP_RESTLET_SCRIPT_ID / NS_GROUP_RESTLET_DEPLOY_ID not set — "
              "see RESTLET_SETUP.md for the deploy steps, then add these as GitHub secrets.")
        sys.exit(1)
    body = {"action": action, "searches": searches}
    if SALES_REP_ID:
        body["salesRepId"] = SALES_REP_ID
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
    known = {k: v for k, v in SEARCH_IDS.items() if v}
    missing = [k for k, v in SEARCH_IDS.items() if not v]
    if missing:
        print(f"NOTE: no id set for {len(missing)} search(es), skipping: {missing}")
    if not known:
        print("ERROR: no search ids configured. Fill in SEARCH_IDS in this file, or set "
              "SEARCH_IDS_JSON='{\"Baseball Coaches\": 12345, ...}'.")
        sys.exit(1)

    print("=" * 70)
    print(f"  FIX GROUP AUDIENCES  |  LIVE={LIVE}  |  SALES_REP_ID={SALES_REP_ID}  |  "
          f"searches: {len(known)}")
    print("=" * 70)

    results = call_restlet("fix", known, dry_run=not LIVE)

    changed = would_change = errors = 0
    for r in results:
        print(f"\n{r.get('label')}  (search id {r.get('savedSearchId')})")
        if r.get("error"):
            print(f"   ERROR: {r['error']}")
            errors += 1
            continue
        print(f"   title: {r.get('searchTitle')!r}  type: {r.get('searchType')}")
        print(f"   currently: inactive-filter={r.get('hasInactiveFilter')}  "
              f"sales-rep-filter={r.get('hasSalesRepFilter')}  "
              f"company-column={r.get('hasCompanyColumn')}")
        print(f"   current members: {r.get('currentResultCount')}   "
              f"members after this run: {r.get('wouldBeResultCount')}")
        wc = r.get("wouldChange", {})
        if wc.get("addInactiveFilter") or wc.get("addSalesRepFilter") or wc.get("addCompanyColumn"):
            would_change += 1
            print(f"   would change: addInactiveFilter={wc.get('addInactiveFilter')}  "
                  f"addSalesRepFilter={wc.get('addSalesRepFilter')}  "
                  f"addCompanyColumn={wc.get('addCompanyColumn')}")
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
