"""
retire_one_contact.py — retire a single named-by-id NS contact record.

For orphans the normal sweeps can't reach: retire_untracked_contacts.py
only visits customers that are themselves rows on the Schools tab, so a
duplicate sitting on a PARENT customer (a school district, not the school
itself) is invisible to it. First case: Bret St Arnauld, contact 48484,
company 2290 "Mount Horeb School District" (parent of "Mount Horeb High
School" 2217, which the sheet already tracks as his shared card) — a
legacy record from before the district/school split, found via
diag_email_lookup.py after Andy spotted him doubled in the sales-rep-
scoped Boys Football Coaches audience.

Same convention as retire_untracked_contacts.py / merge_coop_contacts.py:
retire = lastName += " (dup {id})" + isInactive=True. Only ever touches
a record stamped "Auto-synced by School Sync" — refuses otherwise, so a
contact Andy made by hand (or the wrong id) is never silently altered.

Env: CONTACT_ID (required). DRY RUN by default; LIVE=1 applies.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from netsuite_sync import ns_get, ns_patch

LIVE = os.environ.get("LIVE", "").strip() in ("1", "true", "True", "yes")
CONTACT_ID = os.environ.get("CONTACT_ID", "").strip()
OWNER_MARK = "Auto-synced by School Sync"


def main():
    print("=" * 70)
    print(f"  RETIRE ONE CONTACT  |  id={CONTACT_ID}  |  LIVE={LIVE}")
    print("=" * 70)
    if not CONTACT_ID.isdigit():
        print("  ERROR: set CONTACT_ID to a numeric NS contact id")
        sys.exit(1)

    r = ns_get(f"contact/{CONTACT_ID}?fields=firstName,lastName,email,company,"
               f"isInactive,comments,externalId")
    if r.status_code != 200:
        print(f"  ERROR: GET contact/{CONTACT_ID} -> {r.status_code} {r.text[:300]}")
        sys.exit(1)
    b = r.json()
    comp = b.get("company") or {}
    name = f"{b.get('firstName', '')} {b.get('lastName', '')}"
    print(f"  {name}  <{b.get('email', '')}>")
    print(f"  company: {comp.get('id', '')}  {comp.get('refName', '')}")
    print(f"  isInactive: {b.get('isInactive')}")
    print(f"  comments: {(b.get('comments') or '')[:200]!r}")

    if b.get("isInactive"):
        print("\n  Already inactive — nothing to do.")
        return

    if OWNER_MARK not in (b.get("comments") or ""):
        print(f"\n  REFUSING: comments do not contain {OWNER_MARK!r} — "
              f"this record wasn't created by the sync, won't touch it.")
        sys.exit(1)

    new_last = f"{(b.get('lastName') or '').strip()} (dup {CONTACT_ID})"[:80]
    print(f"\n  Would rename lastName -> {new_last!r} and set isInactive=True")
    if not LIVE:
        print("  DRY RUN — nothing changed. Set LIVE=1 to apply.")
        return

    pr = ns_patch(f"contact/{CONTACT_ID}", {"lastName": new_last, "isInactive": True})
    if pr.status_code == 204:
        print("  APPLIED")
    else:
        print(f"  ERROR: PATCH -> {pr.status_code} {pr.text[:300]}")
        sys.exit(1)


if __name__ == "__main__":
    main()
