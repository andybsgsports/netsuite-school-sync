"""
rep_digests.py
--------------
Consolidated replacement for the six per-rep WIAA scraper scripts that used to
live on the Desktop and send Outlook emails via Task Scheduler.

One run:
  1. Reads the "WI School List- Master" Google Sheet (grouped by Sales Rep col)
  2. For each configured rep, scrapes WIAA for their schools
  3. Builds an xlsx with Athletic Admins / Administrators / per-sport coach tabs
  4. Diffs current vs. previous snapshot (stored in snapshots/{rep}.json)
  5. Emails the rep (and BCC andy) via Gmail SMTP if anything changed
  6. Writes the new snapshot back to disk (committed by the workflow)

Env vars required:
  GOOGLE_SHEET_ID_REPS     - ID of "WI School List- Master" sheet
                             (default: 1SlZHbGRvPiO8Qtq7kY2aI0Y9oUsKZ2CxXNXcuw211N0)
  GOOGLE_CREDENTIALS_JSON  - service account JSON (same as daily-sync)
  GMAIL_USER               - e.g. andy@bsgsports.com
  GMAIL_APP_PASSWORD       - Gmail app password (requires 2FA enabled)
  DRY_RUN                  - if "1", send all emails to GMAIL_USER only
  REP_FILTER               - if set, only process that rep name (testing)
"""

import io
import json
import os
import re
import smtplib
import sys
import time
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from netsuite_sync import scrape_wiaa_school_detail
from ihsa_sync import fetch_school_staff, fetch_email, extract_school_id

# -- Config -------------------------------------------------------------------
GOOGLE_SHEET_ID_REPS = os.environ.get(
    "GOOGLE_SHEET_ID_REPS",
    "1SlZHbGRvPiO8Qtq7kY2aI0Y9oUsKZ2CxXNXcuw211N0",
)
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
DELAY_BETWEEN_SCHOOLS = 1.2  # seconds

SNAPSHOT_DIR = Path(__file__).parent / "snapshots"

# Roles that go on the Athletic Admins tab (vs. generic Administrators).
ATHLETIC_AD_ROLES = {
    "Athletic Director",
    "Assistant Principal, Athletic Director",
    "Boys Athletic Director",
    "Girls Athletic Director",
}

# Rep-to-email mapping. "name" must match the value in the sheet's Sales Rep
# column exactly. "cc" is optional.
#
# TODO(andy): confirm the addresses for Howie, JohnV, Tyler, Wedge. These are
# derived from the old per-rep scripts where available and my best guess
# otherwise. Update before flipping DRY_RUN off.
REPS = [
    # Andy also gets IL schools (IHSA API) in his digest — only rep with IL.
    {"name": "Andrew Murray", "email": "andy@bsgsports.com",   "cc": None, "include_il": True},
    {"name": "Jeff Howard",   "email": "howie@bsgsports.com",  "cc": None},
    {"name": "Tyler Fuhrman", "email": "tyler@bsgsports.com",  "cc": None},
    {"name": "Kyle Loughrin", "email": "kylel@bsgsports.com",  "cc": None},
    {"name": "Paul Speth",    "email": "paul@bsgsports.com",   "cc": "julie@bsgsports.com"},
    {"name": "John Viles",    "email": "johnv@bsgsports.com",  "cc": None},
    {"name": "Jeff Wedvick",  "email": "wedge@bsgsports.com",  "cc": None},
]

GOOGLE_SHEET_ID_MAIN = os.environ.get("GOOGLE_SHEET_ID", "")  # for IL_Schools tab

DRY_RUN = os.environ.get("DRY_RUN", "") == "1"
REP_FILTER = os.environ.get("REP_FILTER", "").strip()


# -- Google Sheets -----------------------------------------------------------
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


def load_rep_schools(gc):
    """Returns {rep_name: [(school_name, school_url), ...]}."""
    wb = gc.open_by_key(GOOGLE_SHEET_ID_REPS)
    ws = wb.sheet1  # WI School List- Master has one tab
    rows = ws.get_all_records()
    by_rep = {}
    for row in rows:
        school = str(row.get("Schools", "")).strip()
        url = str(row.get("School Website", "")).strip()
        rep = str(row.get("Sales Rep", "")).strip()
        if not (school and url and rep):
            continue
        by_rep.setdefault(rep, []).append((school, url))
    return by_rep


def load_il_schools(gc):
    """Returns [(school_name, school_website), ...] from IL_Schools tab."""
    if not GOOGLE_SHEET_ID_MAIN:
        return []
    wb = gc.open_by_key(GOOGLE_SHEET_ID_MAIN)
    try:
        ws = wb.worksheet("IL_Schools")
    except Exception:
        return []
    out = []
    for row in ws.get_all_records():
        school = str(row.get("Schools", "")).strip()
        url = str(row.get("School Website", "")).strip()
        if school and url:
            out.append((school, url))
    return out


# -- Scraping helpers --------------------------------------------------------
def scrape_rep(rep_name, schools):
    """Scrape every school assigned to `rep_name`. Returns (admins, coaches)."""
    admins, coaches = [], []
    for i, (school, url) in enumerate(schools, 1):
        print(f"  [{i}/{len(schools)}] {school}")
        try:
            _info, scraped_admins, scraped_coaches = scrape_wiaa_school_detail(url)
        except Exception as exc:
            print(f"    ERROR: {exc}")
            continue
        for a in scraped_admins:
            admins.append({
                "School":     smart_title(school),
                "Role":       canonical_admin_role(a.get("role", "")),
                "First Name": smart_title(a.get("first") or ""),
                "Last Name":  smart_title(a.get("last") or ""),
                "Email":      a.get("email", ""),
                "State":      "WI",
            })
        for c in scraped_coaches:
            coaches.append({
                "School":     smart_title(school),
                "Sport":      c.get("role", ""),  # netsuite_sync returns sport in role
                "First Name": smart_title(c.get("first") or ""),
                "Last Name":  smart_title(c.get("last") or ""),
                "Role":       c.get("type", ""),  # Head Coach / Assistant Coach / Coach
                "Email":      c.get("email", ""),
                "State":      "WI",
            })
        time.sleep(DELAY_BETWEEN_SCHOOLS)
    return dedup_admins(admins), dedup_coaches(coaches)


# IHSA role IDs that belong on the Administrators-style sheets rather than a
# sport-coach sheet. Prefix meanings: A* / B* = Admin, G* = Medical, everything
# else is a coach / activity head.
IL_ADMIN_PREFIXES = ("A", "B", "G")
# Admin role-IDs that specifically belong on the Athletic Admins sheet
IL_ATHLETIC_AD_ROLE_IDS = {"B2-AthDir", "C1-BoysAD", "C1-GirlsAD"}


def scrape_il_schools(il_schools):
    """
    Scrape IL via IHSA API. Returns (admins, coaches) in the same row shape
    as scrape_rep() so they can be merged into Andy's combined xlsx.
    """
    admins, coaches = [], []
    for i, (school, url) in enumerate(il_schools, 1):
        school_id = extract_school_id(url)
        if not school_id:
            print(f"  [IL {i}/{len(il_schools)}] {school}  -- can't parse id, skip")
            continue
        print(f"  [IL {i}/{len(il_schools)}] {school} (id {school_id})")
        try:
            people = fetch_school_staff(school_id)
        except Exception as exc:
            print(f"    ERROR staff2: {exc}")
            continue
        # Resolve emails
        for p in people:
            if p.get("has_email") and p.get("person_id"):
                try:
                    p["email"] = fetch_email(school_id, p["person_id"])
                except Exception:
                    p["email"] = ""
                time.sleep(0.15)
        for p in people:
            if not p.get("email"):
                continue
            role_id = p.get("role_id", "") or ""
            role_name = (p.get("role") or "").strip()
            coach_type = p.get("type", "")
            if not role_name:
                continue
            first = smart_title(p.get("first") or "")
            last  = smart_title(p.get("last") or "")
            if coach_type == "Admin":
                admins.append({
                    "School":     smart_title(school),
                    "Role":       canonical_admin_role(role_name) if role_id in IL_ATHLETIC_AD_ROLE_IDS else smart_title(role_name),
                    "First Name": first,
                    "Last Name":  last,
                    "Email":      p["email"],
                    "State":      "IL",
                })
            else:
                # role_name is the sport already (e.g. "Boys Baseball")
                coaches.append({
                    "School":     smart_title(school),
                    "Sport":      smart_title(role_name),
                    "First Name": first,
                    "Last Name":  last,
                    "Role":       coach_type,   # "Head Coach" / "Assistant Coach" / "Coach"
                    "Email":      p["email"],
                    "State":      "IL",
                })
        time.sleep(0.5)
    return dedup_admins(admins), dedup_coaches(coaches)


def _norm(s):
    return re.sub(r"\s+", " ", ("" if s is None else str(s)).strip())


def smart_title(s):
    """
    Like str.title() but:
      - doesn't capitalize after apostrophes ("Principal's", not "Principal'S")
      - leaves already-mixed-case input alone ("McDonald" stays "McDonald")
    """
    t = str(s or "")
    if not t:
        return t
    # If the string has any mixed case (e.g. "McDonald", "D'Andrea"), preserve it.
    if t != t.lower() and t != t.upper():
        return t
    return re.sub(r"\b[a-zA-Z]+(?:'[a-zA-Z]+)?",
                  lambda m: m.group(0)[0].upper() + m.group(0)[1:].lower(),
                  t)


def canonical_admin_role(role):
    r = _norm(role)
    low = r.lower()
    if "assistant principal" in low and "athletic director" in low:
        return "Assistant Principal, Athletic Director"
    if "assistant athletic director" in low:
        return "Assistant Athletic Director"
    if "activities director" in low:
        return "Activities Director"
    if "supervisor" in low:
        return smart_title(r)
    if "athletic director" in low and "assistant" not in low:
        if "boys" in low:
            return "Boys Athletic Director"
        if "girls" in low:
            return "Girls Athletic Director"
        return "Athletic Director"
    return smart_title(r)


def dedup_admins(admins):
    """Collapse per (School, Email). Combine gendered ADs into plain AD."""
    if not admins:
        return []
    df = pd.DataFrame(admins)
    out = []
    for (_school, _email), group in df.groupby(["School", "Email"]):
        row = group.iloc[0].to_dict()
        roles = set(group["Role"].tolist())
        if {"Boys Athletic Director", "Girls Athletic Director"} <= roles or "Athletic Director" in roles:
            row["Role"] = "Athletic Director"
        elif len(roles) > 1:
            row["Role"] = " & ".join(sorted(roles))
        out.append(row)
    return out


def sport_group_of(sport):
    """Canonical sport group: strip Boys/Girls, separators, collapse whitespace."""
    s = re.sub(r"\b(Boys|Girls)\b", "", str(sport), flags=re.IGNORECASE)
    s = re.sub(r"[-_&]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return smart_title(s)


def dedup_coaches(coaches):
    """
    Dedup per (School, Email, SportGroup) so one coach covering Boys+Girls
    of the same sport becomes a single row, but a coach covering different
    sports (e.g. Basketball + Golf) stays as separate rows — one per sheet.
    """
    if not coaches:
        return []
    df = pd.DataFrame(coaches)
    df["SportGroup"] = df["Sport"].map(sport_group_of)
    out = []
    for (_school, _email, sg), group in df.groupby(["School", "Email", "SportGroup"]):
        row = group.iloc[0].to_dict()
        row["SportGroup"] = sg
        roles = {str(r) for r in group["Role"].tolist()}
        sports = list(dict.fromkeys(group["Sport"].tolist()))
        if "Head Coach" in roles:
            row["Role"] = "Head Coach"
        elif "Assistant Coach" in roles:
            row["Role"] = "Assistant Coach"
        elif "Coach" in roles:
            row["Role"] = "Coach"
        has_boys = any("boys" in s.lower() for s in sports)
        has_girls = any("girls" in s.lower() for s in sports)
        if has_boys and has_girls:
            row["Sport"] = f"Boys & Girls {sg}".strip()
        elif len(sports) == 1:
            row["Sport"] = sports[0]
        else:
            # Same SportGroup, multiple variants (rare) — pick the cleanest.
            row["Sport"] = sports[0]
        out.append(row)
    return out


# -- XLSX output -------------------------------------------------------------
def build_xlsx(admins, coaches, rep_name):
    """Build the per-rep xlsx in memory. Returns (bytes, sheet_summary_dict)."""
    bio = io.BytesIO()
    summary = {}
    df_admins = pd.DataFrame(admins)
    df_coaches = pd.DataFrame(coaches)

    with pd.ExcelWriter(bio, engine="openpyxl") as w:
        if not df_admins.empty:
            df_ath = df_admins[df_admins["Role"].isin(ATHLETIC_AD_ROLES)]
            df_oth = df_admins[~df_admins["Role"].isin(ATHLETIC_AD_ROLES)]
            cols = ["School", "Role", "First Name", "Last Name", "Email", "State"]
            if not df_ath.empty:
                df_ath.reindex(columns=cols).sort_values(["State", "School"]) \
                    .to_excel(w, sheet_name="Athletic Admins", index=False)
                summary["Athletic Admins"] = len(df_ath)
            if not df_oth.empty:
                df_oth.reindex(columns=cols).sort_values(["State", "School"]) \
                    .to_excel(w, sheet_name="Administrators", index=False)
                summary["Administrators"] = len(df_oth)

        if not df_coaches.empty:
            df_coaches = df_coaches.copy()
            if "SportGroup" not in df_coaches.columns:
                df_coaches["SportGroup"] = df_coaches["Sport"].map(sport_group_of)
            cols = ["School", "Sport", "First Name", "Last Name", "Role", "Email", "State"]
            for sport_group, group in df_coaches.groupby("SportGroup", dropna=False):
                sheet = re.sub(r"[\\/*?:[\]]", "", sport_group or "Unknown").strip()[:31] or "Unknown"
                df_group = group.reindex(columns=cols).sort_values(["State", "School"])
                df_group.to_excel(w, sheet_name=sheet, index=False)
                summary[sheet] = len(df_group)

    apply_table_formatting(bio)
    return bio.getvalue(), summary


def apply_table_formatting(bio):
    bio.seek(0)
    wb = load_workbook(bio)
    used = set()
    for ws in wb.worksheets:
        max_row, max_col = ws.max_row, ws.max_column
        if max_row < 2 or max_col < 1:
            continue
        base = re.sub(r"\W+", "", ws.title)[:25] or "Data"
        name = base + "Tbl"
        k = 1
        while name in used:
            name = f"{base}{k}Tbl"
            k += 1
        used.add(name)
        if not ws.tables:
            t = Table(displayName=name, ref=f"A1:{get_column_letter(max_col)}{max_row}")
            t.tableStyleInfo = TableStyleInfo(name="TableStyleMedium9", showRowStripes=True)
            ws.add_table(t)
        for idx in range(1, max_col + 1):
            col = get_column_letter(idx)
            width = max((len(str(c.value)) if c.value else 0) for c in ws[col])
            ws.column_dimensions[col].width = min(max(width + 2, 8), 60)
    bio.seek(0)
    bio.truncate()
    wb.save(bio)


# -- Snapshots + diff --------------------------------------------------------
def snapshot_path(rep_name):
    safe = re.sub(r"[^A-Za-z0-9]+", "_", rep_name).strip("_")
    return SNAPSHOT_DIR / f"{safe}.json"


def contacts_to_records(admins, coaches):
    """
    Returns {(school, email, role, sport): {"first":..., "last":...}}.
    Key is the stable identity used for diffing; value holds display fields
    like First/Last name so we can show them in added/removed email lines
    without making them part of the diff key (which would cause noise when
    a name gets re-cased on some future run).
    """
    recs = {}
    for a in admins:
        key = (a["School"], a["Email"], a["Role"], "")
        recs[key] = {"first": a.get("First Name", ""), "last": a.get("Last Name", "")}
    for c in coaches:
        key = (c["School"], c["Email"], c["Role"], c.get("Sport", ""))
        recs[key] = {"first": c.get("First Name", ""), "last": c.get("Last Name", "")}
    return recs


def load_snapshot(rep_name):
    """Returns (keyset, records_dict) or (None, {})."""
    p = snapshot_path(rep_name)
    if not p.exists():
        return None, {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        # New format: records is a list of {school, email, role, sport, first, last}
        if "records" in data:
            recs = {}
            for r in data["records"]:
                key = (r.get("school", ""), r.get("email", ""),
                       r.get("role", ""), r.get("sport", ""))
                recs[key] = {"first": r.get("first", ""), "last": r.get("last", "")}
            return set(recs.keys()), recs
        # Legacy format: keys-only list of tuples, no name info
        return {tuple(k) for k in data.get("keys", [])}, {}
    except Exception:
        return None, {}


def save_snapshot(rep_name, records):
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    p = snapshot_path(rep_name)
    serializable = [
        {"school": k[0], "email": k[1], "role": k[2], "sport": k[3],
         "first": v.get("first", ""), "last": v.get("last", "")}
        for k, v in sorted(records.items())
    ]
    p.write_text(
        json.dumps(
            {
                "rep": rep_name,
                "updated": datetime.utcnow().isoformat() + "Z",
                "records": serializable,
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def diff_keys(previous, current):
    if previous is None:
        return set(), set(), True  # first run
    added = current - previous
    removed = previous - current
    return added, removed, False


# -- Email -------------------------------------------------------------------
def send_email(rep, subject, body, xlsx_bytes, xlsx_name):
    """
    Recipient logic:
      DIGESTS_OVERRIDE_TO set  -> all emails go there, subject gets [NEW SYS] tag
                                  (shadow mode for parallel validation)
      DRY_RUN=1                -> all emails go to GMAIL_USER, labeled [DRY RUN]
      otherwise (true live)    -> rep's actual email + CC
    """
    gmail_user = os.environ.get("GMAIL_USER", "")
    gmail_pw = os.environ.get("GMAIL_APP_PASSWORD", "")
    override_to = os.environ.get("DIGESTS_OVERRIDE_TO", "").strip()
    if not (gmail_user and gmail_pw):
        print("  WARNING: GMAIL_USER / GMAIL_APP_PASSWORD not set -- skipping send")
        return False

    if override_to:
        to_addr = override_to
        cc_addr = None
        bcc_addr = None  # primary recipient is already the user — skip BCC
        subject = f"[NEW SYS] {subject}"
        body = (
            f"(Shadow-mode email from GitHub Actions; would have gone to {rep['email']}"
            + (f", CC {rep['cc']}" if rep.get("cc") else "")
            + ".)\n\n" + body
        )
    elif DRY_RUN:
        to_addr = gmail_user
        cc_addr = None
        bcc_addr = None  # TO is already GMAIL_USER — skip BCC
        body = (
            f"[DRY RUN — would send to {rep['email']}"
            + (f", CC {rep['cc']}" if rep.get("cc") else "")
            + "]\n\n" + body
        )
    else:
        to_addr = rep["email"]
        cc_addr = rep.get("cc")
        bcc_addr = gmail_user  # true live: BCC andy so he sees each rep's email

    msg = EmailMessage()
    msg["From"] = gmail_user
    msg["To"] = to_addr
    if cc_addr:
        msg["Cc"] = cc_addr
    msg["Subject"] = subject
    msg.set_content(body)
    msg.add_attachment(
        xlsx_bytes,
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=xlsx_name,
    )

    recipients = [to_addr]
    if cc_addr:
        recipients.append(cc_addr)
    if bcc_addr and bcc_addr not in recipients:
        recipients.append(bcc_addr)

    with smtplib.SMTP("smtp.gmail.com", 587) as s:
        s.starttls()
        s.login(gmail_user, gmail_pw)
        s.send_message(msg, to_addrs=recipients)
    print(f"  Email sent to {to_addr}" + (f" (CC {cc_addr})" if cc_addr else ""))
    return True


# -- Main --------------------------------------------------------------------
def main():
    print("=" * 60)
    print(f"  Rep Digests  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}  |  DRY_RUN={DRY_RUN}")
    print("=" * 60)

    gc = get_gspread_client()
    by_rep = load_rep_schools(gc)
    print(f"\nReps in sheet: {sorted(by_rep.keys())}")

    # Pre-load IL schools once (used by any rep with include_il=True)
    il_schools = []
    if any(r.get("include_il") for r in REPS):
        il_schools = load_il_schools(gc)
        print(f"IL schools available: {len(il_schools)}")

    rep_name_to_config = {r["name"]: r for r in REPS}
    results = []

    for rep in REPS:
        if REP_FILTER and rep["name"] != REP_FILTER:
            continue
        schools = by_rep.get(rep["name"], [])
        if not schools:
            print(f"\n[{rep['name']}] No schools in sheet — skipping")
            continue

        print(f"\n{'-' * 60}")
        print(f"[{rep['name']}] {len(schools)} schools")
        print("-" * 60)

        admins, coaches = scrape_rep(rep["name"], schools)

        # Merge IL schools into this rep's digest if configured (Andy only)
        il_count = 0
        if rep.get("include_il") and il_schools:
            print(f"  Pulling {len(il_schools)} IL schools via IHSA API...")
            il_admins, il_coaches = scrape_il_schools(il_schools)
            admins = admins + il_admins
            coaches = coaches + il_coaches
            il_count = len(il_schools)

        current_records = contacts_to_records(admins, coaches)
        current_keys = set(current_records.keys())
        previous_keys, previous_records = load_snapshot(rep["name"])
        added, removed, first_run = diff_keys(previous_keys, current_keys)

        xlsx_bytes, sheet_summary = build_xlsx(admins, coaches, rep["name"])
        digest_label = "WI+IL" if rep.get("include_il") else "WI"
        xlsx_name = f"{rep['name'].replace(' ', '_')}-{digest_label}_School_Admins_Coaches.xlsx"

        def render(prefix, key, records):
            # key = (school, email, role, sport)
            rec = records.get(key, {})
            name = (f"{rec.get('first','')} {rec.get('last','')}").strip() or "(name unknown)"
            school, email, role, sport = key
            tail = f"  [{sport}]" if sport else ""
            return f"  {prefix} {name}  {email}  {role}  ({school}){tail}"

        body_lines = [f"{digest_label} school contact digest for {rep['name']}", ""]
        if first_run:
            body_lines.append("Initial snapshot — no previous version to diff against.")
        else:
            body_lines.append(f"Changes since last run: +{len(added)} / -{len(removed)}")
            if added:
                body_lines.append("\nAdded:")
                for k in sorted(added):
                    body_lines.append(render("+", k, current_records))
            if removed:
                body_lines.append("\nRemoved:")
                for k in sorted(removed):
                    body_lines.append(render("-", k, previous_records))
        body_lines += ["", "Sheet counts:"] + [f"  {k}: {v}" for k, v in sorted(sheet_summary.items())]
        body = "\n".join(body_lines)

        should_send = first_run or added or removed
        if should_send:
            subject = f"{rep['name']} - Updated {digest_label} School Admins and Coaches"
            sent = send_email(rep, subject, body, xlsx_bytes, xlsx_name)
        else:
            print(f"  No changes — no email sent.")
            sent = False

        save_snapshot(rep["name"], current_records)
        results.append({
            "rep": rep["name"],
            "schools": len(schools),
            "added": len(added),
            "removed": len(removed),
            "sent": sent,
        })

    print("\n" + "=" * 60)
    print("Summary:")
    for r in results:
        print(f"  {r['rep']:<20}  schools={r['schools']:<3}  +{r['added']:<3} -{r['removed']:<3}  sent={r['sent']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
