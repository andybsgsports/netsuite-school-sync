"""
build_unified_master.py
-----------------------
One-time script. Builds the Schools_Master tab on the School Sync Master
Google Sheet by:
  1. Reading the WI School List- Master sheet (all reps' WI schools)
  2. Reading the IL_Schools tab (IHSA schools)
  3. Reading a NetSuite customer export CSV (CustomersProjects827.csv)
  4. Smart-matching each school to a NetSuite Customer by name
  5. Writing the unified Schools_Master tab with a match-confidence column

After this runs, review "Match Confidence" column values:
  - exact     : normalized name matched a single NS customer
  - high      : fuzzy match >=90 within same state, single best candidate
  - low       : fuzzy match 70-90, manual review recommended
  - ambiguous : two+ candidates tied closely, flagged for manual pick
  - none      : no candidate found — either add to NS or leave blank

The sync scripts will eventually read ONLY from this tab and never
overwrite its editable columns.

Usage:
  python build_unified_master.py [path/to/CustomersProjects827.csv]
  python build_unified_master.py --dry-run    # print match report, don't write
"""

import argparse
import csv
import json
import os
import re
import sys
from collections import defaultdict
from difflib import SequenceMatcher
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
SHEET_MAIN = os.environ.get("GOOGLE_SHEET_ID", "1iWhtasin-gmk3jllDvls7G1eI_pgzMm4yfQUP_qZHEM")
SHEET_REPS = os.environ.get("GOOGLE_SHEET_ID_REPS", "1SlZHbGRvPiO8Qtq7kY2aI0Y9oUsKZ2CxXNXcuw211N0")

MASTER_TAB = "Schools_Master"
MASTER_COLUMNS = [
    "School Name",
    "State",
    "Scraper URL",
    "Sales Rep",
    "NS Customer ID",
    "NS Customer Name",
    "Match Confidence",
    "Locked",
    "Notes",
    "Last Synced",
]

# -- Normalization ----------------------------------------------------------
DROP_WORDS = [
    "high school", "high schools", "school district", "school district's",
    "schools", "school", "area school", "community unit school district",
    "consolidated school district", "union school district", "unit district",
    "h.s.", "hs", "district", "academy", "area", "the ",
]


def norm_name(s):
    """Canonicalize a name for matching: lowercase, strip punctuation, collapse whitespace,
    remove common school noise words, and alias St./Mt./&."""
    s = (s or "").lower().strip()
    s = s.replace("&", "and").replace("'s", "").replace("'", "")
    s = s.replace("st.", "saint").replace("mt.", "mount").replace("ft.", "fort")
    s = re.sub(r"\([^)]*\)", "", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    for w in DROP_WORDS:
        s = re.sub(rf"\b{re.escape(w.strip())}\b", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()


# -- Data loading ----------------------------------------------------------
def get_gspread_client():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if creds_json:
        creds = Credentials.from_service_account_info(
            json.loads(creds_json), scopes=GOOGLE_SCOPES
        )
    else:
        creds_file = Path(__file__).parent / "credentials.json"
        creds = Credentials.from_service_account_file(str(creds_file), scopes=GOOGLE_SCOPES)
    return gspread.authorize(creds)


def load_wi_schools(gc):
    wb = gc.open_by_key(SHEET_REPS)
    ws = wb.sheet1
    out = []
    for r in ws.get_all_records():
        name = str(r.get("Schools", "")).strip()
        url = str(r.get("School Website", "")).strip()
        rep = str(r.get("Sales Rep", "")).strip()
        if name and url:
            out.append({"name": name, "state": "WI", "url": url, "rep": rep})
    return out


def load_il_schools(gc):
    wb = gc.open_by_key(SHEET_MAIN)
    try:
        ws = wb.worksheet("IL_Schools")
    except Exception:
        return []
    out = []
    for r in ws.get_all_records():
        name = str(r.get("Schools", "")).strip()
        url = str(r.get("School Website", "")).strip()
        rep = str(r.get("Sales Rep", "")).strip()
        ns_id = str(r.get("NS Customer ID", "")).strip()
        notes = str(r.get("Notes", "")).strip()
        if name and url:
            out.append({
                "name": name, "state": "IL", "url": url, "rep": rep,
                "ns_id_hint": ns_id, "notes": notes,
            })
    return out


def load_ns_customers(csv_path):
    """Returns list of {id, company_name, sales_rep, state, norm_name}."""
    out = []
    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            nid = (row.get("Internal ID") or "").strip()
            if not nid:
                continue
            name = (row.get("Name") or row.get("Company Name") or "").strip()
            if not name:
                continue
            out.append({
                "id": nid,
                "company_name": name,
                "sales_rep": (row.get("Sales Rep") or "").strip(),
                "state": (row.get("Billing State/Province") or "").strip().upper(),
                "city":  (row.get("Billing City") or "").strip(),
                "norm":  norm_name(name),
            })
    return out


# -- Matching --------------------------------------------------------------
def name_variants(raw):
    """
    Build alternate normalized forms for a school name.
    Handles IL's "School (Detail)" convention — e.g.
      "Cary (C.-Grove)"  -> also try "Cary-Grove", "Cary C-Grove"
      "Fox Lake (Grant)" -> also try "Fox Lake Grant", "Grant"
      "Crystal Lake (Central)" -> also try "Crystal Lake Central", "Central"
    """
    out = {norm_name(raw)}
    m = re.match(r"^([^()]+?)\s*\(([^)]+)\)\s*$", raw.strip())
    if m:
        before = m.group(1).strip()
        inner  = m.group(2).strip()
        out.add(norm_name(f"{before} {inner}"))
        out.add(norm_name(f"{before}-{inner}"))
        out.add(norm_name(inner))
        # "Cary (C.-Grove)" -> the before+inner collapsed: "Cary C-Grove"
        out.add(norm_name(f"{before} {inner.replace('.', '')}"))
    return {v for v in out if v}


def match_school_to_customer(school, ns_customers):
    """
    Return (ns_id, ns_name, confidence) where confidence is one of:
      'exact', 'high', 'low', 'ambiguous', 'none'.
    """
    targets = name_variants(school["name"])
    target  = norm_name(school["name"])
    state   = school["state"]

    # Exact normalized match (any variant) within same state first
    state_candidates = [c for c in ns_customers if c["state"] == state]
    exact = [c for c in state_candidates if c["norm"] in targets]
    if len(exact) == 1:
        return exact[0]["id"], exact[0]["company_name"], "exact"
    if len(exact) > 1:
        best = min(exact, key=lambda c: int(c["id"]) if c["id"].isdigit() else 10**9)
        return best["id"], best["company_name"], "ambiguous"

    # Fuzzy, state-constrained. Score against the best variant.
    scored = []
    for c in state_candidates:
        if not c["norm"]:
            continue
        r = max(similarity(t, c["norm"]) for t in targets)
        if r >= 0.70:
            scored.append((r, c))
    scored.sort(key=lambda x: -x[0])

    if scored:
        top = scored[0]
        if top[0] >= 0.90:
            # High confidence, but check if second is within 0.03 (ambiguous)
            if len(scored) > 1 and scored[1][0] >= top[0] - 0.03:
                return top[1]["id"], top[1]["company_name"], "ambiguous"
            return top[1]["id"], top[1]["company_name"], "high"
        if top[0] >= 0.70:
            if len(scored) > 1 and scored[1][0] >= top[0] - 0.03:
                return top[1]["id"], top[1]["company_name"], "ambiguous"
            return top[1]["id"], top[1]["company_name"], "low"

    return "", "", "none"


# -- Build the rows --------------------------------------------------------
def build_master_rows(wi_schools, il_schools, ns_customers):
    rows = []
    stats = defaultdict(int)
    for sch in wi_schools + il_schools:
        ns_hint = sch.get("ns_id_hint") or ""
        if ns_hint:
            rows.append({
                "School Name": sch["name"], "State": sch["state"],
                "Scraper URL": sch["url"], "Sales Rep": sch["rep"],
                "NS Customer ID": ns_hint, "NS Customer Name": "",
                "Match Confidence": "manual", "Locked": "",
                "Notes": sch.get("notes", ""), "Last Synced": "",
            })
            stats["manual"] += 1
            continue
        ns_id, ns_name, conf = match_school_to_customer(sch, ns_customers)
        # Only auto-fill NS Customer ID for HIGH-confidence matches. For the
        # others put the best-guess in Notes so the user can review and copy
        # across intentionally — prevents silent bad matches from syncing.
        if conf in ("exact", "high"):
            fill_id = ns_id
            fill_name = ns_name
            note = ""
        else:
            fill_id = ""
            fill_name = ""
            note = f"[{conf}] guess: {ns_id} {ns_name}".strip() if ns_id else f"[{conf}] no candidate"
        rows.append({
            "School Name": sch["name"], "State": sch["state"],
            "Scraper URL": sch["url"], "Sales Rep": sch["rep"],
            "NS Customer ID": fill_id, "NS Customer Name": fill_name,
            "Match Confidence": conf, "Locked": "",
            "Notes": note, "Last Synced": "",
        })
        stats[conf] += 1
    return rows, stats


# -- Write to Sheet --------------------------------------------------------
def write_master_tab(gc, rows):
    wb = gc.open_by_key(SHEET_MAIN)
    try:
        ws = wb.worksheet(MASTER_TAB)
        ws.clear()
    except Exception:
        ws = wb.add_worksheet(title=MASTER_TAB, rows=len(rows) + 20, cols=len(MASTER_COLUMNS))
    vals = [MASTER_COLUMNS] + [[str(r.get(h, "") or "") for h in MASTER_COLUMNS] for r in rows]
    ws.update(range_name="A1", values=vals)
    print(f"Wrote {len(rows)} rows to '{MASTER_TAB}' tab")


# -- Main ------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("csv_path", nargs="?",
                        default=r"C:\Users\andre\Downloads\CustomersProjects827.csv",
                        help="NetSuite customer export CSV")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print match stats + ambiguous/none rows; do not write sheet.")
    args = parser.parse_args()

    if not os.path.exists(args.csv_path):
        print(f"ERROR: CSV not found: {args.csv_path}")
        sys.exit(1)

    print("=" * 60)
    print(f"  Build Schools_Master  |  {'DRY RUN' if args.dry_run else 'LIVE'}")
    print("=" * 60)

    gc = get_gspread_client()
    print(f"\nLoading WI schools from master list...")
    wi = load_wi_schools(gc)
    print(f"  {len(wi)} WI schools")
    print(f"Loading IL schools from IL_Schools tab...")
    il = load_il_schools(gc)
    print(f"  {len(il)} IL schools")
    print(f"Loading NetSuite customers from {args.csv_path}...")
    ns = load_ns_customers(args.csv_path)
    print(f"  {len(ns)} NS customers")

    print(f"\nMatching...")
    rows, stats = build_master_rows(wi, il, ns)

    print(f"\n{'=' * 60}\nMatch confidence breakdown:")
    for conf in ("exact", "high", "manual", "low", "ambiguous", "none"):
        print(f"  {conf:<12s}: {stats.get(conf, 0)}")
    print(f"  TOTAL       : {sum(stats.values())}")

    # Show interesting cases
    ambig = [r for r in rows if r["Match Confidence"] == "ambiguous"]
    nones = [r for r in rows if r["Match Confidence"] == "none"]
    lows  = [r for r in rows if r["Match Confidence"] == "low"]
    if ambig:
        print(f"\nAmbiguous ({len(ambig)}) — review these in the sheet:")
        for r in ambig[:25]:
            print(f"  {r['State']}  {r['School Name']:<30s} -> {r['NS Customer ID']:>6}  {r['NS Customer Name']}")
        if len(ambig) > 25:
            print(f"  ... and {len(ambig) - 25} more")
    if lows:
        print(f"\nLow-confidence ({len(lows)}) — verify:")
        for r in lows[:25]:
            print(f"  {r['State']}  {r['School Name']:<30s} -> {r['NS Customer ID']:>6}  {r['NS Customer Name']}")
        if len(lows) > 25:
            print(f"  ... and {len(lows) - 25} more")
    if nones:
        print(f"\nNo match ({len(nones)}) — either create in NS or leave blank:")
        for r in nones[:25]:
            print(f"  {r['State']}  {r['School Name']}")
        if len(nones) > 25:
            print(f"  ... and {len(nones) - 25} more")

    if args.dry_run:
        print(f"\nDRY RUN — no changes written. Remove --dry-run to write the tab.")
        return

    write_master_tab(gc, rows)
    print(f"\nDone. Open the sheet and review: "
          f"https://docs.google.com/spreadsheets/d/{SHEET_MAIN}/edit")


if __name__ == "__main__":
    main()
