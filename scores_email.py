"""
scores_email.py
---------------
Weekly sports scores digest for BSG Sports customer schools.

Runs every Monday morning: reads scores_schools.csv, checks the WIAA
schedule page for each team, collects all games from the prior week
(Monday through Sunday), and emails one HTML digest. Off-season teams
have empty schedules and naturally drop out — only teams that played
last week appear in the email.

CSV required columns:
  School Name, State, Sport, TeamID

One email per sales rep, covering that rep's schools only (rep assignment
comes from the master sheet's Schools tab; rep email addresses reuse the
REPS list in rep_digests.py). Sends via Gmail SMTP.

SAFETY: until SCORES_LIVE=1, every rep's email is redirected to GMAIL_USER
(Andy) with a "[TEST → rep@...]" subject prefix — reps receive nothing.

Env vars:
  GMAIL_USER               sender + test-mode recipient
  GMAIL_APP_PASSWORD       Gmail app password (16-char, 2FA required)
  GOOGLE_SHEET_ID          master sheet (Schools tab, for rep assignment)
  GOOGLE_CREDENTIALS_JSON  service account JSON (same as other workflows)
  SCORES_LIVE              "1" → actually send to each rep (BCC Andy)
  SCORES_CSV               path to CSV  (default: scores_schools.csv)
  SCHOOL_FILTER            substring filter on school name (testing)
  SEND_EMPTY               "1" → send Andy an email even with no games
  DRY_RUN                  "1" → print instead of send
  DUMP_HTML                "1" → write raw schedule HTML + parse diagnostics
"""

import csv
import os
import re
import smtplib
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────
GMAIL_USER         = os.environ.get("GMAIL_USER", "")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
SCORES_CSV         = os.environ.get("SCORES_CSV", "").strip() or "scores_schools.csv"
SCORES_RECIPIENT   = os.environ.get("SCORES_RECIPIENT", "").strip() or GMAIL_USER
DRY_RUN            = os.environ.get("DRY_RUN", "0") == "1"
DUMP_HTML          = os.environ.get("DUMP_HTML", "0") == "1"
SCHOOL_FILTER      = os.environ.get("SCHOOL_FILTER", "").strip().lower()
SEND_EMPTY         = os.environ.get("SEND_EMPTY", "0") == "1"
SCORES_LIVE        = os.environ.get("SCORES_LIVE", "0") == "1"
WEEK_OF            = os.environ.get("WEEK_OF", "").strip()   # YYYY-MM-DD: report that week
WORKERS            = int(os.environ.get("WORKERS", "6") or "6")

# Same headers the existing WIAA scraper uses — already bypass bot protection
WIAA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer":         "https://schools.wiaawi.org/Directory/School/List",
}

WIAA_SCHEDULE_BASE = "https://schools.wiaawi.org/Directory/Schedule/Index"
DELAY = 0.6  # seconds between requests


# ── WIAA date parser ──────────────────────────────────────────────────────────
def _parse_date(text):
    """Parse WIAA schedule date strings. Returns date or None.

    Real cells look like '20260407 04/07/2026 7:00PM (C)' — a yyyymmdd sort
    key plus m/d/yyyy plus time. Extract the date portion wherever it sits."""
    text = re.sub(r"\s+", " ", text.strip())
    m = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", text)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        except ValueError:
            pass
    m = re.match(r"^(\d{4})(\d{2})(\d{2})\b", text)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass
    return None


def _col(headers, *keywords):
    """Return first column index whose header matches any keyword (case-insensitive)."""
    for i, h in enumerate(headers):
        hl = h.lower().strip()
        if any(k in hl for k in keywords):
            return i
    return None


_GENERIC_SCHOOL_WORDS = {
    "school", "district", "high", "hs", "the", "of", "academy", "community",
    "area", "co-op", "coop", "senior", "junior", "jr", "sr", "public",
}


def _school_tokens(school_name):
    """Distinctive lowercase tokens from a school name for matching team
    cells, e.g. 'Mount Horeb School District' -> {'mount', 'horeb'}."""
    toks = {t for t in re.split(r"[^a-z0-9]+", school_name.lower()) if t}
    return toks - _GENERIC_SCHOOL_WORDS or toks


def _team_matches(team_cell, school_tokens):
    if not (team_cell and school_tokens):
        return False
    cell_toks = {t for t in re.split(r"[^a-z0-9]+", team_cell.lower()) if t}
    return school_tokens <= cell_toks


# ── WIAA schedule scraper ─────────────────────────────────────────────────────
def fetch_wiaa_schedule(team_id, school_name=""):
    """
    Fetch the WIAA team schedule page for `team_id` and return a list of game dicts:
      date, opponent, location, is_home, result ('W'/'L'/'T'/''), score ('8-3'/''), played, level

    Returns [] on any error (or for off-season teams with no schedule table).
    """
    url = f"{WIAA_SCHEDULE_BASE}?TeamID={team_id}"
    try:
        resp = requests.get(url, headers=WIAA_HEADERS, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        print(f"  [WARN] TeamID {team_id}: request failed — {e}")
        return []

    if DUMP_HTML:
        dump_path = Path(f"dump_team_{team_id}.html")
        dump_path.write_text(resp.text, encoding="utf-8")
        print(f"  [DUMP] Wrote {dump_path}")

    soup = BeautifulSoup(resp.text, "html.parser")

    # Find the first <table> that has at least one data row
    table = None
    for t in soup.find_all("table"):
        if len(t.find_all("tr")) >= 2:
            table = t
            break
    if not table:
        # Off-season teams have no schedule table — normal, not an error
        return []

    rows = table.find_all("tr")
    headers = [c.get_text(separator=" ", strip=True) for c in rows[0].find_all(["th", "td"])]

    # WIAA ScoreCenter format (confirmed via live dump 2026-06):
    #   Date | Date | Home | Away | Location | Result | ContestID | ContestType
    ci_date   = _col(headers, "date")
    ci_home   = _col(headers, "home")
    ci_away   = _col(headers, "away")
    ci_result = _col(headers, "result", "score", "final", "w/l", "outcome")
    ci_level  = _col(headers, "contesttype", "contest type", "level", "class")

    if ci_date is None or ci_home is None or ci_away is None:
        print(f"  [WARN] TeamID {team_id}: can't identify date/home/away columns.")
        print(f"         Headers found: {headers}")
        return []

    school_tokens = _school_tokens(school_name)

    games  = []
    sample = 0
    for row in rows[1:]:
        cells = row.find_all(["td", "th"])
        if not cells:
            continue

        def cell(idx):
            if idx is None or idx >= len(cells):
                return ""
            return cells[idx].get_text(separator=" ", strip=True)

        if DUMP_HTML and sample < 3:
            print(f"  [SAMPLE] cells={[cell(i) for i in range(len(cells))]}")
            if sample == 0:
                # Raw row HTML: shows whether home/away cells carry TeamID
                # links we could use to look up opponent records.
                print(f"  [ROWHTML] {str(row)[:1200]}")
                # Any standings/record/conference breadcrumbs on the page
                for a in soup.find_all("a", href=True)[:40]:
                    href = a["href"]
                    if re.search(r"conference|standing|record", href, re.I):
                        print(f"  [PAGELINK] {a.get_text(' ', strip=True)!r} -> {href[:160]}")
                for el in soup.find_all(string=re.compile(r"(Record|Conference|Overall)", re.I))[:8]:
                    parent_text = el.parent.get_text(" ", strip=True)[:160]
                    print(f"  [PAGETEXT] {parent_text}")
            sample += 1

        game_date = _parse_date(cell(ci_date))
        if not game_date:
            continue

        home_team = cell(ci_home).strip()
        away_team = cell(ci_away).strip()
        if not (home_team or away_team):
            continue

        # Which side is our school? Token match against the school name.
        is_home = _team_matches(home_team, school_tokens)
        if not is_home and not _team_matches(away_team, school_tokens):
            # Neither side matches (multi-team meet row etc.) — assume home
            is_home = True
        opponent = away_team if is_home else home_team

        raw_result = cell(ci_result)

        # Result: "W 8-3", "L 2-5", "Tie 0-0", "W 16-2 (5)",
        # "W 3-0 (25-8,25-9,25-14)", etc. W/L/T is from this team's perspective.
        played = False
        result = ""
        score  = ""
        if raw_result and raw_result not in ("-", "–", "TBD", "Upcoming", "Scheduled"):
            wlt = re.search(r"\b([WL]|T(?:ie)?)\b", raw_result, re.I)
            sc  = re.search(r"(\d+)\s*[-–]\s*(\d+)", raw_result)
            if not sc:
                nums = re.findall(r"\b(\d{1,3})\b", raw_result)
                if len(nums) >= 2:
                    score = f"{nums[0]}-{nums[1]}"
            if wlt or sc or score:
                played = True
                result = wlt.group(1)[0].upper() if wlt else ""
                if sc:
                    score = f"{sc.group(1)}-{sc.group(2)}"

        games.append({
            "date":     game_date,
            "opponent": opponent,
            "location": cell(_col(headers, "location", "site")),
            "is_home":  is_home,
            "result":   result,
            "score":    score,
            "played":   played,
            "level":    cell(ci_level),
        })

    if DUMP_HTML:
        if games:
            print(f"  [PARSE] {len(games)} game(s): "
                  f"{games[0]['date']} … {games[-1]['date']}; "
                  f"e.g. {'vs.' if games[0]['is_home'] else '@'} {games[0]['opponent']}"
                  f" {games[0]['result']} {games[0]['score']}".rstrip())
        else:
            print(f"  [PARSE] 0 games parsed from {len(rows)-1} table row(s)")

    return games


def prior_week_range(today=None):
    """Return (monday, sunday) of the week to report.

    Default: the week before `today` — run on a Monday that's the
    immediately preceding Mon–Sun; run mid-week it's the last full week.
    WEEK_OF=YYYY-MM-DD overrides: report the Mon–Sun week containing that
    date (for testing against a historical week, e.g. 2026-04-20)."""
    if WEEK_OF:
        d = datetime.strptime(WEEK_OF, "%Y-%m-%d").date()
        monday = d - timedelta(days=d.weekday())
        return monday, monday + timedelta(days=6)
    if today is None:
        today = date.today()
    this_monday = today - timedelta(days=today.weekday())
    last_monday = this_monday - timedelta(days=7)
    last_sunday = this_monday - timedelta(days=1)
    return last_monday, last_sunday


def record_through(games, end_date):
    """Season W-L(-T) record from played games on or before end_date,
    e.g. '15-5' or '15-5-1'. Empty string if nothing played."""
    w = l = t = 0
    for g in games:
        if not g["played"] or g["date"] > end_date:
            continue
        if g["result"] == "W":
            w += 1
        elif g["result"] == "L":
            l += 1
        elif g["result"] == "T":
            t += 1
    if not (w or l or t):
        return ""
    return f"{w}-{l}-{t}" if t else f"{w}-{l}"


def games_in_range(games, start, end):
    """Filter game list to those with start <= date <= end."""
    return [g for g in games if start <= g["date"] <= end]


# ── HTML email ────────────────────────────────────────────────────────────────
# Sports that only one gender plays — drop the Boys/Girls prefix in headers
# ("Boys Baseball" → "BASEBALL"). Everything else keeps its prefix so
# GIRLS SOCCER and BOYS SOCCER stay separate sections.
_SINGLE_GENDER_SPORTS = {
    "baseball", "softball", "football", "football 8-player", "gymnastics",
}


def sport_section(sport):
    """Section header for a sport: 'Boys Baseball' → 'BASEBALL',
    'Girls Soccer' → 'GIRLS SOCCER'."""
    s = re.sub(r"\s+", " ", str(sport or "").strip())
    low = s.lower()
    for prefix in ("boys ", "girls "):
        if low.startswith(prefix) and low[len(prefix):] in _SINGLE_GENDER_SPORTS:
            s = s[len(prefix):]
            break
    return s.upper() or "OTHER"


def build_html(school_results, week_start, week_end):
    """
    school_results: list of {"school": str, "sport": str, "games": [game_dict, ...]}
    Returns an HTML string covering the week, grouped into one section per
    sport (BASEBALL, SOFTBALL, ...) with that sport's games listed under it.
    """
    date_str = (f"Week of {week_start.strftime('%B %d')} – "
                f"{week_end.strftime('%B %d, %Y')}")

    # Flatten to (section, school_label, game) and group by section.
    # school_label carries the season record: "Barneveld High School (15-5)"
    sections = {}
    for item in school_results:
        section = sport_section(item["sport"])
        label = item["school"]
        if item.get("record"):
            label += f' <span style="font-weight:400;color:#666">({item["record"]})</span>'
        for g in item["games"]:
            sections.setdefault(section, []).append((label, g))

    blocks = []
    for section in sorted(sections):
        rows = []
        for school, g in sorted(sections[section],
                                key=lambda x: (x[0], x[1]["date"])):
            ha = "vs." if g["is_home"] else "@"

            if g["played"]:
                color = {"W": "#2e7d32", "L": "#c62828", "T": "#555"}.get(g["result"], "#555")
                label = g["result"] or "Final"
                sc    = f"&nbsp;&nbsp;{g['score']}" if g["score"] else ""
                result_html = f'<strong style="color:{color}">{label}{sc}</strong>'
            else:
                result_html = '<span style="color:#888;font-style:italic">No score reported</span>'

            rows.append(f"""
        <tr>
          <td style="padding:8px 14px;border-bottom:1px solid #eee;white-space:nowrap;color:#777;font-size:13px">
            {g["date"].strftime("%a %m/%d")}
          </td>
          <td style="padding:8px 14px;border-bottom:1px solid #eee;font-weight:600">
            {school}
          </td>
          <td style="padding:8px 14px;border-bottom:1px solid #eee">
            {ha} {g["opponent"]}
          </td>
          <td style="padding:8px 14px;border-bottom:1px solid #eee;text-align:center;white-space:nowrap">
            {result_html}
          </td>
        </tr>""")

        blocks.append(f"""
      <div style="background:#1a237e;color:#fff;padding:9px 16px;margin:26px 0 0;
                  border-radius:6px 6px 0 0;font-size:15px;font-weight:700;
                  letter-spacing:1px">{section}</div>
      <table width="100%" cellpadding="0" cellspacing="0"
             style="border-collapse:collapse;font-size:14px;border:1px solid #eee;
                    border-top:none">
        <thead>
          <tr style="background:#f5f5f5">
            <th style="padding:8px 14px;text-align:left;border-bottom:2px solid #ddd;
                       font-weight:600;font-size:12px;color:#666">Date</th>
            <th style="padding:8px 14px;text-align:left;border-bottom:2px solid #ddd;
                       font-weight:600;font-size:12px;color:#666">School</th>
            <th style="padding:8px 14px;text-align:left;border-bottom:2px solid #ddd;
                       font-weight:600;font-size:12px;color:#666">Opponent</th>
            <th style="padding:8px 14px;text-align:center;border-bottom:2px solid #ddd;
                       font-weight:600;font-size:12px;color:#666">Result</th>
          </tr>
        </thead>
        <tbody>{"".join(rows)}
        </tbody>
      </table>""")

    body_html = "".join(blocks) if blocks else """
      <p style="padding:20px;text-align:center;color:#999">
        No games found last week.
      </p>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<body style="margin:0;padding:24px;background:#f0f2f5;font-family:Arial,Helvetica,sans-serif">
  <div style="max-width:680px;margin:0 auto;background:#fff;border-radius:8px;
              box-shadow:0 2px 10px rgba(0,0,0,.10);overflow:hidden">

    <div style="background:#1a237e;padding:22px 28px;color:#fff">
      <div style="font-size:11px;letter-spacing:1px;text-transform:uppercase;
                  opacity:.7;margin-bottom:4px">BSG Sports</div>
      <h1 style="margin:0;font-size:22px;font-weight:700">Customer School Scores</h1>
      <p style="margin:5px 0 0;opacity:.8;font-size:14px">{date_str}</p>
    </div>

    <div style="padding:4px 28px 24px">{body_html}
    </div>

    <div style="padding:14px 28px;background:#f9f9f9;border-top:1px solid #eee;
                font-size:12px;color:#aaa">
      Automated digest · Scores via WIAA (wiaawi.org) · BSG Sports
    </div>
  </div>
</body>
</html>"""


# ── Rep assignment ───────────────────────────────────────────────────────────
def load_rep_config():
    """Rep name -> {email, cc} from rep_digests.REPS (single source of truth
    for rep addresses). Falls back to empty dict if unavailable."""
    try:
        from rep_digests import REPS
        return {r["name"]: r for r in REPS}
    except Exception as e:
        print(f"[WARN] Could not load REPS from rep_digests: {e}")
        return {}


def load_school_reps():
    """School Name -> Sales Rep from the master sheet's Schools tab.
    Live lookup so rep reassignments take effect immediately. Returns {}
    (everything routes to Andy) if the sheet is unreachable."""
    sheet_id = os.environ.get("GOOGLE_SHEET_ID", "").strip()
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if not (sheet_id and creds_json):
        print("[WARN] GOOGLE_SHEET_ID / GOOGLE_CREDENTIALS_JSON not set — "
              "all schools route to Andy")
        return {}
    try:
        import json as _json
        import gspread
        from google.oauth2.service_account import Credentials
        creds = Credentials.from_service_account_info(
            _json.loads(creds_json),
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"],
        )
        ws = gspread.authorize(creds).open_by_key(sheet_id).worksheet("Schools")
        out = {}
        for rec in ws.get_all_records():
            name = str(rec.get("School Name", "")).strip()
            rep  = str(rec.get("Sales Rep", "")).strip()
            if name:
                out[name] = rep
        return out
    except Exception as e:
        print(f"[WARN] Schools tab lookup failed ({e}) — all schools route to Andy")
        return {}


# ── Email sender ──────────────────────────────────────────────────────────────
def send_email(subject, html_body, to_addr, cc_addr=None, bcc_addr=None):
    """Send HTML email via Gmail SMTP (TLS). Returns True on success."""
    if not (GMAIL_USER and GMAIL_APP_PASSWORD):
        print("  [WARN] GMAIL_USER / GMAIL_APP_PASSWORD not set — skipping send")
        return False
    msg = EmailMessage()
    msg["From"]    = GMAIL_USER
    msg["To"]      = to_addr
    if cc_addr:
        msg["Cc"] = cc_addr
    if bcc_addr and bcc_addr != to_addr:
        msg["Bcc"] = bcc_addr
    msg["Subject"] = subject
    msg.set_content("Please open this email in an HTML-capable client.")
    msg.add_alternative(html_body, subtype="html")
    with smtplib.SMTP("smtp.gmail.com", 587) as s:
        s.starttls()
        s.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        s.send_message(msg)
    return True


# ── CSV loader ────────────────────────────────────────────────────────────────
def load_csv(path):
    """
    Load scores_schools.csv. Returns list of dicts with keys:
      school, state, sport, team_id
    Rows missing TeamID are skipped.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(
            f"CSV not found: {p}\n"
            "Run the 'Discover Team IDs' workflow to generate it, or create it "
            "with columns: School Name, State, Sport, TeamID"
        )
    schools = []
    with open(p, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            team_id = str(row.get("TeamID", "")).strip()
            if not team_id or team_id == "0":
                continue
            schools.append({
                "school":  str(row.get("School Name", "")).strip(),
                "state":   str(row.get("State", "WI")).strip().upper(),
                "sport":   str(row.get("Sport", "")).strip(),
                "team_id": team_id,
            })
    return schools


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    today = date.today()
    week_start, week_end = prior_week_range(today)
    print(f"\n{'='*60}")
    print(f"  scores_email  |  week {week_start} – {week_end}  |  DRY_RUN={DRY_RUN}")
    print(f"{'='*60}\n")

    schools = load_csv(SCORES_CSV)
    if SCHOOL_FILTER:
        schools = [s for s in schools if SCHOOL_FILTER in s["school"].lower()]
        print(f"SCHOOL_FILTER='{SCHOOL_FILTER}' → {len(schools)} team(s)")
    print(f"Loaded {len(schools)} team(s) from {SCORES_CSV}\n")

    def check_team(entry):
        """Fetch one team's schedule; return (week games, season record)."""
        if entry["state"] == "WI":
            all_games = fetch_wiaa_schedule(entry["team_id"], entry["school"])
        else:
            # IL/IHSA schedule scraping — coming soon
            all_games = []
        time.sleep(DELAY)
        return (games_in_range(all_games, week_start, week_end),
                record_through(all_games, week_end))

    print(f"Checking schedules with {WORKERS} parallel workers...")
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        results_per_team = list(ex.map(check_team, schools))

    school_results = []
    for entry, (week_games, record) in zip(schools, results_per_team):
        if not week_games:
            continue
        rec = f" ({record})" if record else ""
        print(f"[{entry['state']}] {entry['school']}{rec} — {entry['sport']}: "
              f"{len(week_games)} game(s)")
        for g in week_games:
            ha = "vs." if g["is_home"] else " @"
            sc = f"  {g['result']} {g['score']}" if g["played"] else "  (no score)"
            print(f"       {g['date']} {ha} {g['opponent']}{sc}")
        school_results.append({
            "school": entry["school"],
            "sport":  entry["sport"],
            "record": record,
            "games":  week_games,
        })

    print()
    if not school_results and not SEND_EMPTY:
        print("[OK] No games last week for any tracked school — no email sent.\n")
        return

    played_count = sum(
        1 for r in school_results for g in r["games"] if g["played"]
    )
    print(f"Games last week: {sum(len(r['games']) for r in school_results)} total, "
          f"{played_count} with scores")

    # ── Group results by sales rep ────────────────────────────────────────────
    rep_config  = load_rep_config()
    school_reps = load_school_reps()

    by_rep = {}
    for r in school_results:
        rep = school_reps.get(r["school"], "")
        if rep not in rep_config:
            rep = ""  # unknown/unassigned rep → Andy's copy
        by_rep.setdefault(rep, []).append(r)

    # SEND_EMPTY test mode with no games at all: one empty email to Andy
    if not by_rep and SEND_EMPTY:
        by_rep = {"": []}

    week_label = (f"Week of {week_start.strftime('%b %d')}"
                  f" – {week_end.strftime('%b %d, %Y')}")

    print(f"\nEmails to build: {len(by_rep)} "
          f"(reps: {[rep or 'Andy/unassigned' for rep in by_rep]})")
    if not SCORES_LIVE:
        print("TEST MODE (SCORES_LIVE unset) — every email goes to "
              f"{GMAIL_USER} instead of the rep\n")

    for rep, results in sorted(by_rep.items()):
        cfg     = rep_config.get(rep, {})
        html    = build_html(results, week_start, week_end)
        subject = (f"{rep + ' — ' if rep else ''}School Scores — {week_label}")

        if rep and cfg.get("email"):
            intended_to = cfg["email"]
            intended_cc = cfg.get("cc")
        else:
            intended_to = GMAIL_USER
            intended_cc = None

        if SCORES_LIVE:
            to_addr, cc_addr = intended_to, intended_cc
            bcc_addr = GMAIL_USER  # Andy sees every rep's email
        else:
            to_addr, cc_addr, bcc_addr = GMAIL_USER, None, None
            if intended_to != GMAIL_USER:
                subject = f"[TEST → {intended_to}] {subject}"

        if DRY_RUN:
            print(f"[DRY RUN] {subject}  →  {to_addr}"
                  + (f" (cc {cc_addr})" if cc_addr else ""))
            for r in results:
                print(f"    • {r['school']} ({r['sport']}): {len(r['games'])} game(s)")
            fname = f"dry_run_email_{(rep or 'andy').replace(' ', '_')}.html"
            Path(fname).write_text(html, encoding="utf-8")
            print(f"[DRY RUN] wrote {fname}")
        else:
            ok = send_email(subject, html, to_addr, cc_addr, bcc_addr)
            print(f"[{'OK' if ok else 'WARN'}] {subject}  →  {to_addr}")


if __name__ == "__main__":
    main()
