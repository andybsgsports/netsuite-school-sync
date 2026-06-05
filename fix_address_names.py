"""
fix_address_names.py — one-time cleanup that updates the `addressee`
on existing NetSuite Ship-To address lines so renamed schools show
the new name (e.g. "Cary (C.-Grove)" -> "Cary-Grove High School").

For every school on the Schools tab that has an NS Customer ID, reads
each addressBook line's addressBookAddress subrecord and, if the
addressee differs from the canonical name (Full Name column, falling
back to School Name), PATCHes just the addressee. Bill-To lines
(defaultBilling) are left alone.

Run after a school rename. Kept out of the nightly push so that the
nightly stays fast (this does a per-line subrecord read that only
matters right after a rename).

Env:
  GOOGLE_SHEET_ID, GOOGLE_CREDENTIALS_JSON, NS_* tokens
  SALES_REP_FILTER  - optional, limit to one rep
  STATE_FILTER      - optional, WI or IL
  SCHOOL_FILTER     - optional, single school
"""
from __future__ import annotations

import os
import sys
import time

from netsuite_sync import ns_get, ns_patch
from school_netsuite_sync import (
    get_gspread_client,
    GOOGLE_SHEET_ID, MASTER_TAB,
    M_NAME, M_NS_ID, M_LOCKED,
)

M_FULL = "Full Name"
SALES_REP_FILTER = os.environ.get("SALES_REP_FILTER", "").strip()
STATE_FILTER     = os.environ.get("STATE_FILTER", "").strip().upper()
SCHOOL_FILTER    = os.environ.get("SCHOOL_FILTER", "").strip()


def fix_school(customer_id, canonical):
    """Update addressee on every non-Bill-To address line that doesn't
    already match `canonical`. Returns (fixed, checked)."""
    fixed = checked = 0
    r = ns_get(f"customer/{customer_id}?expand=addressBook")
    if r.status_code != 200:
        print(f"    WARN: can't read addressBook for {customer_id}: {r.status_code}")
        return 0, 0
    for item in r.json().get("addressBook", {}).get("items", []):
        href = item.get("links", [{}])[0].get("href", "")
        line_id = href.rstrip("/").split("/")[-1] if href else None
        if not line_id:
            continue
        line = ns_get(f"customer/{customer_id}/addressBook/{line_id}")
        if line.status_code != 200:
            continue
        if line.json().get("defaultBilling"):
            continue  # leave Bill-To alone
        sub = ns_get(f"customer/{customer_id}/addressBook/{line_id}/addressBookAddress")
        if sub.status_code != 200:
            continue
        checked += 1
        subj = sub.json()
        if os.environ.get("DEBUG_ADDR") and checked <= 2:
            import json as _j
            print(f"    DEBUG subrecord JSON: {_j.dumps(subj)[:600]}")
        cur = (subj.get("addressee") or "").strip()
        if cur and cur != canonical:
            pr = ns_patch(
                f"customer/{customer_id}/addressBook/{line_id}/addressBookAddress",
                {"addressee": canonical})
            if pr.status_code in (200, 204):
                fixed += 1
            else:
                print(f"    WARN: addressee PATCH failed line {line_id}: "
                      f"{pr.status_code} {pr.text[:120]}")
        time.sleep(0.05)
    return fixed, checked


def main():
    if not GOOGLE_SHEET_ID:
        print("ERROR: GOOGLE_SHEET_ID env var not set.")
        sys.exit(1)

    gc = get_gspread_client()
    wb = gc.open_by_key(GOOGLE_SHEET_ID)
    rows = wb.worksheet(MASTER_TAB).get_all_records()

    if SALES_REP_FILTER: print(f"SALES_REP_FILTER: {SALES_REP_FILTER}")
    if STATE_FILTER:     print(f"STATE_FILTER: {STATE_FILTER}")
    if SCHOOL_FILTER:    print(f"SCHOOL_FILTER: {SCHOOL_FILTER}")

    total_fixed = total_checked = schools = 0
    for r in rows:
        name  = str(r.get(M_NAME, "")).strip()
        full  = str(r.get(M_FULL, "")).strip()
        ns_id = str(r.get(M_NS_ID, "")).strip()
        rep   = str(r.get("Sales Rep", "")).strip()
        state = str(r.get("State", "")).strip().upper()
        if not name or str(r.get(M_LOCKED, "")).strip().upper() == "Y":
            continue
        if ns_id in ("", "nan", "None", "0"):
            continue
        if SALES_REP_FILTER and rep.lower() != SALES_REP_FILTER.lower():
            continue
        if STATE_FILTER and state != STATE_FILTER:
            continue
        if SCHOOL_FILTER and name != SCHOOL_FILTER:
            continue
        canonical = full or name
        f, c = fix_school(ns_id, canonical)
        schools += 1
        total_fixed += f
        total_checked += c
        if f:
            print(f"  {name} (NS {ns_id}): fixed {f}/{c} -> '{canonical}'")

    print(f"\nSchools scanned: {schools}")
    print(f"Address lines checked: {total_checked}")
    print(f"Addressee lines updated: {total_fixed}")


if __name__ == "__main__":
    main()
