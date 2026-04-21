"""
cleanup_duplicate_contacts.py
-----------------------------
Merge duplicate NetSuite Contact records that were created for the same
person across multiple co-op schools under the old external-ID scheme.

Example: Adam McDonald (amcdonal@waukesha.k12.wi.us) coaches Girls Golf at
Waukesha North + South + West — he used to get a separate NS contact per
school. This script picks one canonical contact per email and inactivates
the rest.

The source of truth is the Google Sheet's Contacts tab — it has one row
per (school, email, role), each with an NS Contact ID. We group rows by
email, and for any email with 2+ distinct NS Contact IDs we have dupes.

Canonical-pick priority (for a given email):
  1. Any contact whose externalId already uses the new EM_<email> format
     (it's been touched by the new sync — that's where the primary link
     should live going forward).
  2. Lowest contact ID (oldest record).

Inactive contacts are skipped entirely — they're already out of play.

By default runs as dry-run. Pass --live to actually inactivate and
rewrite the sheet.

Usage:
  python cleanup_duplicate_contacts.py              # dry run
  python cleanup_duplicate_contacts.py --live       # apply
  python cleanup_duplicate_contacts.py --email x@y  # limit to one email
"""

import argparse
import json
import os
import sys
import time
from collections import defaultdict

import gspread
from google.oauth2.service_account import Credentials

from netsuite_sync import (
    make_contact_external_id,
    ns_get,
    ns_patch,
)

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
CONTACTS_COLUMNS = [
    "School Name", "First", "Last", "Email", "Role", "Type",
    "Sync", "NS Contact ID", "NS Customer ID", "Last Synced",
]


def get_gspread_client():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if creds_json:
        creds = Credentials.from_service_account_info(
            json.loads(creds_json), scopes=GOOGLE_SCOPES
        )
    else:
        creds_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "credentials.json")
        creds = Credentials.from_service_account_file(creds_file, scopes=GOOGLE_SCOPES)
    return gspread.authorize(creds)


def load_contacts_tab(gc, sheet_id):
    wb = gc.open_by_key(sheet_id)
    ws = wb.worksheet("Contacts")
    return wb, ws, ws.get_all_records()


def save_contacts_tab(ws, rows):
    clean = [r for r in rows if str(r.get("School Name", "")).strip()]
    headers = CONTACTS_COLUMNS
    vals = [headers] + [[str(r.get(h, "") or "") for h in headers] for r in clean]
    ws.clear()
    ws.update(range_name="A1", values=vals)


def fetch_contact_details(contact_id):
    """Returns {id, externalId, isInactive, email, firstName, lastName, company_id} or None."""
    r = ns_get(f"contact/{contact_id}")
    if r.status_code != 200:
        return None
    d = r.json()
    company = d.get("company") or {}
    return {
        "id":         str(d.get("id", contact_id)),
        "externalId": d.get("externalId", ""),
        "isInactive": d.get("isInactive", False),
        "email":      d.get("email", ""),
        "firstName":  d.get("firstName", ""),
        "lastName":   d.get("lastName", ""),
        "company_id": str(company.get("id", "")),
    }


def _ext_id_school_slug(ext_id):
    """Parse the school-slug portion of a legacy SCHOOL__email or SCHOOL__ROLE__email ext_id."""
    if not ext_id or ext_id.startswith("EM_"):
        return ""
    parts = (ext_id or "").split("__")
    return parts[0] if parts else ""


def _slug_matches_email_domain(school_slug, email):
    """
    Rough signal: does the school slug contain the email domain's primary token?
    e.g. slug 'MOUNT-HOREB-HIGH-SCHOOL' vs email domain 'mhasd.k12.wi.us' — weak.
    So we invert: check if the email domain appears in NO candidate's school.
    Returns True when slug likely matches the email's domain / primary word.
    """
    if not school_slug or not email:
        return False
    slug = school_slug.upper().replace("-", " ")
    local, _, domain = email.partition("@")
    if not domain:
        return False
    # Pull the "primary" word of the domain (first segment before .k12/.edu/etc)
    primary = domain.split(".")[0].upper()
    # Match if the domain primary appears as a word/prefix in the slug.
    # e.g. primary 'MHASD' vs slug 'MOUNT HOREB...' — doesn't match; this heuristic
    # works best when primary is something like 'WAUKESHA' matching 'WAUKESHA WEST'.
    return primary in slug


def pick_canonical(detail_by_id, email=""):
    """
    Pick the canonical contact ID from a set of duplicates for one email.
    Only active contacts are eligible. Returns None if all are inactive.

    Priority (highest first):
      1. Already migrated to EM_ external-ID format.
      2. Contact whose legacy school-slug matches the email's institutional
         domain — the "home school" contact. e.g. prefer the Mount Horeb
         contact for an @mhasd.k12.wi.us email over the Barneveld co-op copy.
      3. Lowest (oldest) active ID.
    """
    active = {cid: d for cid, d in detail_by_id.items() if not d.get("isInactive")}
    if not active:
        return None

    migrated = [cid for cid, d in active.items()
                if (d.get("externalId") or "").startswith("EM_")]
    if migrated:
        return sorted(migrated, key=lambda x: int(x))[0]

    # Prefer the contact whose school slug matches the email's domain.
    if email:
        domain_matched = [
            cid for cid, d in active.items()
            if _slug_matches_email_domain(_ext_id_school_slug(d.get("externalId", "")), email)
        ]
        if domain_matched:
            return sorted(domain_matched, key=lambda x: int(x))[0]

    return sorted(active.keys(), key=lambda x: int(x))[0]


def merge_dupes_for_email(email, ns_ids, live, verbose=True):
    """
    Resolve a single email's duplicate set.
    ns_ids: list of contact ID strings (already deduped, all non-empty,
            all belong to this email per the sheet).
    Returns (canonical_id_or_None, ids_inactivated_list).
    """
    details = {}
    for cid in ns_ids:
        d = fetch_contact_details(cid)
        if d:
            details[cid] = d
        else:
            if verbose:
                print(f"  [WARN] contact {cid} not fetchable — skipping")
        time.sleep(0.1)
    if not details:
        return None, []

    canonical = pick_canonical(details, email=email)
    if canonical is None:
        if verbose:
            print(f"  All {len(ns_ids)} contacts for {email} are already inactive. Nothing to do.")
        return None, []

    # Anything active other than canonical should be inactivated.
    to_inactivate = [cid for cid, d in details.items()
                     if cid != canonical and not d.get("isInactive")]

    new_ext = make_contact_external_id(email)
    canon_ext = details[canonical].get("externalId", "")
    need_migrate = canon_ext != new_ext

    if verbose:
        print(f"  Canonical: {canonical}  (externalId={canon_ext!r}"
              + (f" -> migrate to {new_ext!r}" if need_migrate else "")
              + ")")
        if to_inactivate:
            print(f"  Inactivate: {to_inactivate}")

    if not live:
        return canonical, to_inactivate

    # LIVE — migrate canonical's external ID, then inactivate the others.
    if need_migrate:
        r = ns_patch(f"contact/{canonical}", {"externalId": new_ext})
        if r.status_code == 204:
            if verbose:
                print(f"    Migrated externalId on {canonical} -> {new_ext}")
        else:
            print(f"    [WARN] migrate externalId {canonical}: {r.status_code} {r.text[:160]}")
        time.sleep(0.2)

    for cid in to_inactivate:
        # Rename the old external ID out of the way so it can't collide
        # with future lookups, then flag isInactive.
        old_ext = details[cid].get("externalId") or ""
        archived_ext = (f"MERGED_{canonical}_{old_ext}" if old_ext
                        else f"MERGED_{canonical}_{cid}")[:150]
        body = {"isInactive": True, "externalId": archived_ext,
                "comments": f"Merged into contact {canonical} ({email})"}
        r = ns_patch(f"contact/{cid}", body)
        if r.status_code == 204:
            if verbose:
                print(f"    Inactivated {cid}, externalId -> {archived_ext}")
        else:
            print(f"    [WARN] inactivate {cid}: {r.status_code} {r.text[:160]}")
        time.sleep(0.2)

    return canonical, to_inactivate


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--live", action="store_true",
                        help="Actually inactivate dupes and rewrite the sheet. "
                             "Default is dry-run.")
    parser.add_argument("--email",
                        help="Only process this one email address (useful for smoke-testing).")
    args = parser.parse_args()

    sheet_id = os.environ.get("GOOGLE_SHEET_ID", "")
    if not sheet_id:
        print("ERROR: GOOGLE_SHEET_ID not set.")
        sys.exit(1)

    print(f"{'=' * 60}")
    print(f"  Contact duplicate cleanup  —  {'LIVE' if args.live else 'DRY RUN'}")
    print(f"{'=' * 60}\n")

    gc = get_gspread_client()
    wb, contacts_ws, contacts = load_contacts_tab(gc, sheet_id)
    print(f"Loaded {len(contacts)} rows from Contacts tab\n")

    # Group by email -> set of distinct non-empty NS Contact IDs
    email_to_ids = defaultdict(set)
    email_to_rows = defaultdict(list)
    for row in contacts:
        email = str(row.get("Email", "")).strip().lower()
        cid = str(row.get("NS Contact ID", "")).strip()
        if not email:
            continue
        email_to_rows[email].append(row)
        if cid and cid not in ("nan", "None", "0", "UNLINKED"):
            email_to_ids[email].add(cid)

    # Only emails with 2+ distinct IDs are duplicates
    dupes = {e: sorted(ids, key=lambda x: int(x) if x.isdigit() else 0)
             for e, ids in email_to_ids.items()
             if len(ids) >= 2}
    if args.email:
        dupes = {args.email.lower(): dupes.get(args.email.lower(), [])}
        dupes = {k: v for k, v in dupes.items() if v}

    print(f"Duplicate sets: {len(dupes)}\n")
    if not dupes:
        print("Nothing to do.")
        return

    total_merged = 0
    total_inactivated = 0
    canonical_by_email = {}
    for email in sorted(dupes):
        ns_ids = dupes[email]
        print(f"[{email}] {len(ns_ids)} contacts: {ns_ids}")
        canonical, inactivated = merge_dupes_for_email(email, ns_ids, live=args.live)
        if canonical:
            canonical_by_email[email] = canonical
            total_merged += 1
            total_inactivated += len(inactivated)
        print()

    # Rewrite sheet so every row for a merged email points at the canonical ID.
    if args.live and canonical_by_email:
        changed = 0
        for row in contacts:
            email = str(row.get("Email", "")).strip().lower()
            canon = canonical_by_email.get(email)
            if not canon:
                continue
            cur = str(row.get("NS Contact ID", "")).strip()
            if cur != canon:
                row["NS Contact ID"] = canon
                changed += 1
        if changed:
            save_contacts_tab(contacts_ws, contacts)
            print(f"Rewrote {changed} sheet row(s) to point at canonical IDs")

    print(f"\n{'=' * 60}")
    verb = "Processed" if args.live else "Would process"
    verb2 = "Inactivated" if args.live else "Would inactivate"
    print(f"  {verb} {total_merged} duplicate set(s)")
    print(f"  {verb2} {total_inactivated} contact(s)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
