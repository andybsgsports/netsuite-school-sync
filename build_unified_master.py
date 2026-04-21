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
    "high school", "high schools", "high sch", "senior high",
    "school district", "school district's", "school dist", "school distr",
    "schools", "school", "sch",
    "community unit school district", "community school district",
    "consolidated school district", "union school district",
    "unit district", "area school district", "area schools",
    "public school", "public schools",
    "h.s.", "hs", "high",   # bare 'High' (NS 'Madison Lafollette High')
    "district", "distr", "dist",
    "the ",
]
# NOTE: "academy" and "area" are INTENTIONALLY NOT stripped — meaningful
# parts of distinct school names ("Brookfield Academy", "Sauk Prairie Area
# Schools"). Stripping them collapsed different schools to the same string.


ABBREV_EXPANSIONS = [
    # Expand NS-style abbreviations BEFORE dropping noise words. Order matters —
    # longer patterns first. All patterns use \b to avoid eating substrings.
    (r"\bsch\.l\b",        "school"),
    (r"\bschls?\.?\b",     "schools"),
    (r"\bschl\.?\b",       "school"),
    (r"\bsch\.\b",         "school"),
    (r"\bdist\.\b",        "district"),
    (r"\bdistr?\.?\b",     "district"),
    (r"\bdis\b",           "district"),    # 'Kickapoo Area School Dis'
    (r"\bdst\.?\b",        "district"),
    (r"\bs\.?d\.?\b",      "school district"),
    (r"\bh\.?s\.?\b",      "high school"),
    (r"\bcomm?\.?\b",      "community"),
    (r"\bco-?op\.?\b",     "cooperative"),
    (r"\bsr\.?\s+high\b",  "senior high"),
    (r"\bjr\.?\s+high\b",  "junior high"),
    (r"\byth\.?\b",        "youth"),
    (r"\belem\.?\b",       "elementary"),
    (r"\bassn\.?\b",       "association"),
    (r"\bassoc\.?\b",      "association"),
    (r"\bdept\.?\b",       "department"),
    (r"\brec\.?\b",        "recreation"),
    # Drop patterns that don't carry identity for school matching. Order: these
    # turn "McHenry Community High School - District 156" / "Pearl City School
    # Cusd #200" / "Warren Township High School" into forms that collapse to
    # just the town/name.
    (r"\bcusd\s*#?\s*\d+\b", ""),
    (r"\busd\s*#?\s*\d+\b",  ""),          # 'Usd #215' (unit school district)
    (r"\bcud\s*#?\s*\d+\b",  ""),
    (r"\bccsd\s*#?\s*\d+\b", ""),
    (r"\bdistrict\s+\d+\b",  ""),          # 'District 156'
    (r"\bd\s*#\s*\d+\b",     ""),          # 'D #156'
    (r"-\s*district\s+\d+\b", ""),         # '- District 156'
    (r"\btownship\b",        ""),
    (r"\bpublic\s+school\s+district\b", "school district"),
    # City-name spelling aliases (NS uses no space, source sheet uses a space).
    (r"\bde\s+pere\b",      "depere"),
    (r"\bla\s+crosse\b",    "lacrosse"),
    (r"\bfond\s+du\s+lac\b", "fonddulac"),
    (r"\beau\s+claire\b",   "eauclaire"),
    (r"\bst\.?\s+francis\b", "saintfrancis"),
]


def norm_name(s):
    """Canonicalize a name for matching: lowercase, strip punctuation, collapse whitespace,
    remove common school noise words, and alias St./Mt./&."""
    s = (s or "").lower().strip()
    s = s.replace("&", "and").replace("'s", "").replace("'", "")
    # Word-boundary-anchored so 'dist.' doesn't get 'st.' turned into 'saint'.
    s = re.sub(r"\bst\.", "saint", s)
    s = re.sub(r"\bmt\.", "mount", s)
    s = re.sub(r"\bft\.", "fort", s)
    s = re.sub(r"\([^)]*\)", "", s)
    # Expand abbreviations (handles NS's "Com. Schl. Dist." and similar).
    for pattern, repl in ABBREV_EXPANSIONS:
        s = re.sub(pattern, repl, s)
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


OMG_PATTERN = re.compile(r"(?i)\bomg\b")


def load_ns_customers(csv_path):
    """Returns list of {id, company_name, sales_rep, state, norm_name}.

    NS records containing 'OMG' in their name are filtered out entirely —
    those are internal BSG grouping records (team/club variants), not the
    primary school customer, and they should never be linked as a match.
    """
    out = []
    skipped_omg = 0
    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            nid = (row.get("Internal ID") or "").strip()
            if not nid:
                continue
            name = (row.get("Name") or row.get("Company Name") or "").strip()
            if not name:
                continue
            if OMG_PATTERN.search(name):
                skipped_omg += 1
                continue
            out.append({
                "id": nid,
                "company_name": name,
                "sales_rep": (row.get("Sales Rep") or "").strip(),
                "state": (row.get("Billing State/Province") or "").strip().upper(),
                "city":  (row.get("Billing City") or "").strip(),
                "norm":  norm_name(name),
            })
    if skipped_omg:
        print(f"  (skipped {skipped_omg} OMG-flagged NS records)")
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


# Multi-school districts whose individual high schools are stored in NS
# WITHOUT the city prefix. e.g. NS has "Edgewood High School" and
# "Lafollette High School" under billing city = Madison. Our source sheet
# lists them as "Madison Edgewood" / "Madison Lafollette", so we need to
# also try the suffix alone.
SPLIT_PREFIX_CITIES = {
    "madison", "milwaukee", "kenosha", "green bay", "la crosse", "lacrosse",
    "appleton", "racine", "beloit", "wausau", "janesville", "sheboygan",
    "oshkosh", "waukesha", "sun prairie", "west bend", "brookfield",
    "eau claire", "fond du lac", "stevens point", "rockford",
    "crystal lake", "grayslake", "lake villa", "cary", "fox lake",
    "gurnee", "belvidere",
}


def base_name_variants(raw):
    """Non-normalized base names to try. Handles IL parens and city-prefix
    patterns where the school is stored in NS without the city."""
    raw = raw.strip()
    out = [raw]
    m = re.match(r"^([^()]+?)\s*\(([^)]+)\)\s*$", raw)
    if m:
        before = m.group(1).strip()
        inner  = m.group(2).strip()
        # e.g. "Cary (C.-Grove)" -> "Cary-Grove", "Cary C-Grove", "Cary", "C-Grove"
        out = [
            f"{before}-{inner.replace('.', '')}",
            f"{before} {inner.replace('.', '')}",
            before,
            inner.replace(".", "").strip(),
        ]
        # And also try the suffix alone if the before-part is a known city.
        if norm_name(before) in SPLIT_PREFIX_CITIES:
            out.append(inner.replace(".", "").strip())
        return out
    # Non-parens: if the name starts with a known city and has >1 token,
    # also try the suffix alone ("Madison Edgewood" -> "Edgewood").
    tokens = raw.split()
    for n_prefix in (3, 2, 1):
        if len(tokens) <= n_prefix:
            continue
        prefix = " ".join(tokens[:n_prefix])
        if norm_name(prefix) in SPLIT_PREFIX_CITIES:
            out.append(" ".join(tokens[n_prefix:]))
            break
    return out


# Each tier: (label, name-builder, regex the NS customer's ORIGINAL name must contain)
# Anchoring by original-name regex stops a 'High School' query from matching
# a 'School District' record that happens to share the same normalized form.
TIER_RULES = [
    ("hs", lambda b: f"{b} High School",
     re.compile(r"(?i)\b(high\s+school|h\.?s\.?|senior\s+high)\b")),
    ("sd", lambda b: f"{b} School District",
     re.compile(r"(?i)\b(school\s+(dist|distr|district|dst)|"
                r"sch\.?\s*(dist|distr|district|dst)|"
                r"schl\.?\s*(dist|distr|district|dst)|"
                r"s\.?\s*d\.?|cusd|usd|ccsd|district\s+\d+)\b")),
    ("area_sd", lambda b: f"{b} Area School District",
     re.compile(r"(?i)\barea\b")),
    ("comm_sd", lambda b: f"{b} Community School District",
     re.compile(r"(?i)\bcomm(unity)?\b")),
    ("bare", lambda b: b, None),  # last resort — no tier filter
]


def exact_match_tiered(school, ns_customers, claimed):
    """
    Try each tier (HS first, then SD-like variants, then bare) against the
    school's base-name variants. Within a tier, a candidate only counts if
    its NORMALIZED name equals our wanted form AND its ORIGINAL name matches
    that tier's keyword regex (e.g. contains 'High School' for the HS tier).

    Within a tier, prefers same-state customers, falling back to empty-state.
    """
    state = school["state"]
    bases = base_name_variants(school["name"])
    for tier_label, builder, tier_regex in TIER_RULES:
        for base in bases:
            wanted = norm_name(builder(base))
            if not wanted:
                continue
            candidates = [
                c for c in ns_customers
                if c["id"] not in claimed and c["norm"] == wanted
            ]
            if tier_regex is not None:
                candidates = [c for c in candidates if tier_regex.search(c["company_name"])]
            if not candidates:
                continue
            same_state = [c for c in candidates if c["state"] == state]
            empty_state = [c for c in candidates if not c["state"]]
            for bucket in (same_state, empty_state):
                if len(bucket) == 1:
                    return bucket[0]["id"], bucket[0]["company_name"], "exact"
                if len(bucket) > 1:
                    # City tiebreak: if the school name carries a city hint
                    # (the 'before' part of an IL parens name, or the bare
                    # name itself) and one candidate's Billing City matches,
                    # prefer that one over the oldest-ID fallback.
                    city_hint = (base_name_variants(school["name"])[0] or "").lower().strip()
                    city_match = [c for c in bucket
                                  if c.get("city", "").lower().strip() == city_hint
                                  or (c.get("city", "").lower().strip()
                                      and c.get("city", "").lower().strip() in city_hint)]
                    if len(city_match) == 1:
                        return city_match[0]["id"], city_match[0]["company_name"], "exact"
                    pool = city_match if city_match else bucket
                    best = min(pool, key=lambda c: int(c["id"]) if c["id"].isdigit() else 10**9)
                    return best["id"], best["company_name"], "ambiguous"
    return None, None, None


def match_fuzzy(school, available_customers):
    """
    Fuzzy-match one school against a pool of (un-claimed) NS customers.
    Called only after exact matches have been resolved and their IDs removed
    from the pool.
    """
    targets = name_variants(school["name"])
    state   = school["state"]
    state_candidates = [c for c in available_customers if c["state"] == state]

    scored = []
    for c in state_candidates:
        if not c["norm"]:
            continue
        r = max(similarity(t, c["norm"]) for t in targets)
        if r >= 0.70:
            scored.append((r, c))
    scored.sort(key=lambda x: -x[0])

    if not scored:
        return "", "", "none"
    top_score, top_c = scored[0]
    second_close = len(scored) > 1 and scored[1][0] >= top_score - 0.03
    if top_score >= 0.90:
        if second_close:
            return top_c["id"], top_c["company_name"], "ambiguous"
        return top_c["id"], top_c["company_name"], "high"
    if top_score >= 0.70:
        if second_close:
            return top_c["id"], top_c["company_name"], "ambiguous"
        return top_c["id"], top_c["company_name"], "low"
    return "", "", "none"


def match_school_to_customer(school, ns_customers):
    """
    Legacy single-pass matcher. Kept for anything that calls it directly;
    build_master_rows uses the two-pass approach instead.
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
    """
    Two-pass match:
      Pass 1: find every EXACT (normalized-name) match in each state and
              claim those NS IDs so they can't be re-used.
      Pass 2: for everything that didn't exact-match, fall back to fuzzy
              against the UN-claimed pool only.
    This is what stops things like 'Brookfield Academy' from being offered
    1037 (Brookfield East, already claimed by the exact match).
    """
    all_schools = wi_schools + il_schools
    claimed = set()   # NS IDs already taken
    per_school_result = {}  # id(sch) -> (ns_id, ns_name, conf)

    # Pass 1 — tiered exact match. Tries 'X High School' first, then
    # 'X School District', then 'X Area School District', then bare 'X'.
    # Within each tier, prefers same-state customers but falls back to
    # empty-state (common for older NS school records).
    for sch in all_schools:
        if sch.get("ns_id_hint"):
            continue
        ns_id, ns_name, conf = exact_match_tiered(sch, ns_customers, claimed)
        if ns_id:
            per_school_result[id(sch)] = (ns_id, ns_name, conf)
            claimed.add(ns_id)

    # Pass 2 — fuzzy, against unclaimed only
    available = [c for c in ns_customers if c["id"] not in claimed]
    for sch in all_schools:
        if sch.get("ns_id_hint") or id(sch) in per_school_result:
            continue
        per_school_result[id(sch)] = match_fuzzy(sch, available)

    rows = []
    stats = defaultdict(int)
    for sch in all_schools:
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
        ns_id, ns_name, conf = per_school_result.get(id(sch), ("", "", "none"))
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


def load_existing_master(gc):
    """Return {(school_name, state): row_dict} from the current tab, empty if absent."""
    wb = gc.open_by_key(SHEET_MAIN)
    try:
        ws = wb.worksheet(MASTER_TAB)
    except Exception:
        return {}
    out = {}
    for r in ws.get_all_records():
        key = (str(r.get("School Name", "")).strip(), str(r.get("State", "")).strip())
        if key[0]:
            out[key] = r
    return out


def merge_with_existing(new_rows, existing):
    """
    Preserve existing manual work:
      - If a row in the sheet already has a non-blank NS Customer ID, keep
        its entire row (honors user's manual edits + any ID already chosen).
      - If Locked = Y, keep the row even if NS Customer ID is blank.
      - Otherwise write the freshly-computed row.
    Returns (merged_rows, preserved_count).
    """
    preserved = 0
    merged = []
    for r in new_rows:
        key = (r["School Name"], r["State"])
        prior = existing.get(key)
        if prior:
            prior_id = str(prior.get("NS Customer ID", "")).strip()
            prior_lock = str(prior.get("Locked", "")).strip().upper() == "Y"
            if prior_id or prior_lock:
                # Trust the prior row. Only refresh the Scraper URL + Sales Rep
                # (these are structural, not the user's match work).
                merged_row = dict(prior)
                merged_row["Scraper URL"] = r["Scraper URL"]
                if not prior_lock:
                    merged_row["Sales Rep"] = r["Sales Rep"]
                merged.append(merged_row)
                preserved += 1
                continue
        merged.append(r)
    return merged, preserved


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

    # Preserve anything the user has already filled in or locked on the sheet.
    existing = load_existing_master(gc)
    if existing:
        rows, preserved = merge_with_existing(rows, existing)
        print(f"\nPreserved {preserved} existing rows (had NS ID set or Locked=Y)")

    if args.dry_run:
        print(f"\nDRY RUN — no changes written. Remove --dry-run to write the tab.")
        return

    write_master_tab(gc, rows)
    print(f"\nDone. Open the sheet and review: "
          f"https://docs.google.com/spreadsheets/d/{SHEET_MAIN}/edit")


if __name__ == "__main__":
    main()
