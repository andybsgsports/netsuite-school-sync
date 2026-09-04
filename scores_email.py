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

SAFETY: until SCORES_LIVE=1, every rep's email is redirected to
SCORES_RECIPIENT (andy@bsgsports.com) with a "[TEST → rep@...]" subject
prefix — reps receive nothing. Once live, Andy is BCC'd on every rep email.

Env vars:
  GMAIL_USER               Gmail sender account
  SCORES_RECIPIENT         Andy's address for test-mode delivery + live BCC
                           (default: GMAIL_USER)
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

TEAMID_RE = re.compile(r"TeamID=(\d+)", re.I)


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
    ci_date    = _col(headers, "date")
    ci_home    = _col(headers, "home")
    ci_away    = _col(headers, "away")
    ci_result  = _col(headers, "result", "score", "final", "w/l", "outcome")
    ci_level   = _col(headers, "contesttype", "contest type", "level", "class")
    ci_contest = _col(headers, "contestid", "contest id")

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

        # TeamIDs from the links in the home/away cells
        def cell_team_id(idx):
            if idx is None or idx >= len(cells):
                return ""
            a = cells[idx].find("a", href=True)
            m = TEAMID_RE.search(a["href"]) if a else None
            return m.group(1) if m else ""

        home_id = cell_team_id(ci_home)
        away_id = cell_team_id(ci_away)

        # Which side is our team? TeamID match is authoritative; fall back
        # to name-token matching when the cell has no link.
        if team_id and team_id in (home_id, away_id):
            is_home = (home_id == team_id)
        else:
            is_home = _team_matches(home_team, school_tokens)
            if not is_home and not _team_matches(away_team, school_tokens):
                is_home = True
        opponent = away_team if is_home else home_team
        opp_team_id = away_id if is_home else home_id

        # Conference game flag: WIAA marks these with "(C)" on the date
        is_conf = "(c)" in cell(ci_date).lower()

        # Start time (for ordering doubleheader games within a day)
        tm = re.search(r"(\d{1,2}):(\d{2})\s*(AM|PM)", cell(ci_date), re.I)
        time_min = None
        if tm:
            hh = int(tm.group(1)) % 12
            if tm.group(3).upper() == "PM":
                hh += 12
            time_min = hh * 60 + int(tm.group(2))

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
            "date":         game_date,
            "time_min":     time_min,
            "contest_id":   cell(ci_contest).strip(),
            "opponent":     opponent,
            "opp_team_id":  opp_team_id,
            "opp_record":   "",   # filled in later from opponent schedule
            "home_team_id": home_id,
            "away_team_id": away_id,
            "is_conf":      is_conf,
            "location":     cell(_col(headers, "location", "site")),
            "is_home":      is_home,
            "result":       result,
            "score":        score,
            "played":       played,
            "level":        cell(ci_level),
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


def _win_pct(games, end_date):
    """Winning pct of played games through end_date (ties = half win).
    None when no games have been played."""
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
    n = w + l + t
    return (w + 0.5 * t) / n if n else None


def _opp_id(g, self_tid):
    """Opponent TeamID in game g relative to team self_tid."""
    if g["home_team_id"] == self_tid:
        return g["away_team_id"]
    if g["away_team_id"] == self_tid:
        return g["home_team_id"]
    return g.get("opp_team_id", "")


def _ordinal(n):
    if 10 <= n % 100 <= 20:
        return f"{n}th"
    return f"{n}{ {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th') }"


def conf_member_ids(tid, cache):
    """Direct conference rivals of team tid = opponents in its (C) games."""
    return {m for m in (_opp_id(g, tid) for g in cache.get(tid, [])
                        if g["is_conf"]) if m}


def conf_component(tid, cache):
    """Full conference membership: transitive closure of (C)-game opponents
    (early in a season a team hasn't played every rival yet, but rivals of
    rivals are still in the same conference). Excludes tid itself."""
    seen = {tid}
    frontier = [tid]
    while frontier:
        nxt = []
        for t in frontier:
            for m in conf_member_ids(t, cache):
                if m not in seen:
                    seen.add(m)
                    nxt.append(m)
        frontier = nxt
    seen.discard(tid)
    return seen


def _fmt_rec(w, l, t):
    if not (w or l or t):
        return ""
    return f"{w}-{l}-{t}" if t else f"{w}-{l}"


def running_records(tid, cache, store):
    """Per-game running record for team tid: iterate the season in true
    chronological order (date, then start time — so doubleheader game 1
    counts before game 2) and record the team's overall record, conference
    record, and conference win pct AFTER each game. Keyed by the game
    object's identity and by ContestID (so the same contest can be looked
    up from the opponent's copy of the schedule)."""
    if tid in store:
        return store[tid]
    games = cache.get(tid, [])
    order = sorted(range(len(games)),
                   key=lambda i: (games[i]["date"],
                                  games[i]["time_min"] if games[i]["time_min"] is not None else 1441,
                                  i))
    w = l = t = cw = cl = ct = 0
    m = {}
    for i in order:
        g = games[i]
        if g["played"]:
            if g["result"] == "W":
                w += 1
                cw += 1 if g["is_conf"] else 0
            elif g["result"] == "L":
                l += 1
                cl += 1 if g["is_conf"] else 0
            elif g["result"] == "T":
                t += 1
                ct += 1 if g["is_conf"] else 0
        n = cw + cl + ct
        entry = (_fmt_rec(w, l, t), _fmt_rec(cw, cl, ct),
                 (cw + 0.5 * ct) / n if n else None)
        m[id(g)] = entry
        if g["contest_id"]:
            m[("c", g["contest_id"])] = entry
    store[tid] = m
    return m


def game_label(tid, g, cache, store):
    """'8-1, 4-1 Conf, 2nd' as of AFTER this specific game — doubleheader
    game 1 and game 2 get different records. Standings rank this team's
    post-game conference pct against rivals' pct through the same date."""
    recs = running_records(tid, cache, store)
    entry = recs.get(id(g))
    if entry is None and g["contest_id"]:
        entry = recs.get(("c", g["contest_id"]))
    if entry is None:
        # Game not found in this team's schedule copy — date-based fallback
        return team_label_stats(tid, g["date"], cache)
    overall, confrec, my_pct = entry

    place = ""
    if my_pct is not None:
        rival_pcts = []
        for m in conf_component(tid, cache):
            p = _win_pct([x for x in cache.get(m, []) if x["is_conf"]], g["date"])
            if p is not None:
                rival_pcts.append(p)
        if rival_pcts:
            better = sum(1 for p in rival_pcts if p > my_pct + 1e-9)
            tied   = sum(1 for p in rival_pcts if abs(p - my_pct) <= 1e-9)
            place = ("T-" if tied else "") + _ordinal(better + 1)

    parts = [overall]
    if confrec:
        parts.append(f"{confrec} Conf")
    if place:
        parts.append(place)
    return ", ".join(p for p in parts if p)


def team_label_stats(tid, end_date, cache):
    """'15-5, 8-2 Conf, 1st' — overall record, conference record, and
    conference standing computed by ranking conference-rival win pcts.
    Parts that can't be computed are omitted."""
    games = cache.get(tid, [])
    conf_games = [g for g in games if g["is_conf"]]
    overall = record_through(games, end_date)
    confrec = record_through(conf_games, end_date)

    place = ""
    my_pct = _win_pct(conf_games, end_date)
    if my_pct is not None:
        rival_pcts = []
        for m in conf_component(tid, cache):
            p = _win_pct([g for g in cache.get(m, []) if g["is_conf"]], end_date)
            if p is not None:
                rival_pcts.append(p)
        if rival_pcts:
            better = sum(1 for p in rival_pcts if p > my_pct + 1e-9)
            tied   = sum(1 for p in rival_pcts if abs(p - my_pct) <= 1e-9)
            place = ("T-" if tied else "") + _ordinal(better + 1)

    parts = [overall]
    if confrec:
        parts.append(f"{confrec} Conf")
    if place:
        parts.append(place)
    return ", ".join(p for p in parts if p)


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
    Returns an HTML string covering the week. Condensed layout: one section
    per sport (BASEBALL, SOFTBALL, ...), and inside it each school is a
    slim sub-header with its games in tight rows underneath.
    """
    date_str = (f"Week of {week_start.strftime('%B %d')} – "
                f"{week_end.strftime('%B %d, %Y')}")

    # section -> school -> [games]
    sections = {}
    for item in school_results:
        section = sport_section(item["sport"])
        bucket = sections.setdefault(section, {}).setdefault(item["school"], [])
        bucket.extend(item["games"])

    cell = "padding:4px 10px;border-bottom:1px solid #f0f0f0;font-size:13px"

    blocks = []
    for section in sorted(sections):
        rows = []
        for school in sorted(sections[section]):
            games = sorted(
                sections[section][school],
                key=lambda g: (g["date"],
                               g.get("time_min") if g.get("time_min") is not None else 1441))

            # Slim school sub-header spanning the table
            rows.append(f"""
        <tr>
          <td colspan="4" style="padding:6px 10px 4px;background:#eef0f7;
              border-bottom:1px solid #dde;font-size:13px;font-weight:700;
              color:#1a237e">{school}</td>
        </tr>""")

            for g in games:
                ha = "vs." if g["is_home"] else "@"
                if g["played"]:
                    color = {"W": "#2e7d32", "L": "#c62828", "T": "#555"}.get(g["result"], "#555")
                    sc = f"&nbsp;{g['score']}" if g["score"] else ""
                    result_html = f'<strong style="color:{color}">{g["result"] or "F"}{sc}</strong>'
                else:
                    result_html = '<span style="color:#999;font-style:italic">no score</span>'

                opp = (f'{ha} {g["opponent"]}'
                       + (f' <span style="color:#888;font-size:12px">({g["opp_record"]})</span>'
                          if g.get("opp_record") else ""))
                rec = (f'<span style="color:#555;font-size:12px">{g["self_record"]}</span>'
                       if g.get("self_record") else "")

                rows.append(f"""
        <tr>
          <td style="{cell};white-space:nowrap;color:#888;width:62px">
            {g["date"].strftime("%a %m/%d")}
          </td>
          <td style="{cell}">{opp}</td>
          <td style="{cell};text-align:center;white-space:nowrap;width:70px">{result_html}</td>
          <td style="{cell};white-space:nowrap;text-align:right">{rec}</td>
        </tr>""")

        blocks.append(f"""
      <div style="background:#1a237e;color:#fff;padding:7px 12px;margin:18px 0 0;
                  border-radius:6px 6px 0 0;font-size:14px;font-weight:700;
                  letter-spacing:1px">{section}</div>
      <table width="100%" cellpadding="0" cellspacing="0"
             style="border-collapse:collapse;border:1px solid #eee;border-top:none">
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


def _norm_school(name):
    """Normalized school-name key: case/space/punct-insensitive so minor
    drift between the sheet and the TeamID CSV ('Southern door' vs
    'Southern Door') still routes to the right rep."""
    return re.sub(r"[^a-z0-9]+", " ", str(name or "").lower()).strip()


def load_school_reps():
    """Normalized school name -> Sales Rep from the master sheet's Schools
    tab. Live lookup so rep reassignments take effect immediately. Returns
    {} (everything routes to Andy) if the sheet is unreachable."""
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
                out[_norm_school(name)] = rep
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

    # Schedule cache: TeamID -> full season game list. Filled in three passes:
    #   A) our tracked teams   B) their week opponents
    #   C) conference rivals of everyone displayed (for standings ranking)
    schedule_cache = {}

    def fetch_batch(pairs, label):
        pairs = [(t, n) for t, n in pairs if t and t not in schedule_cache]
        if not pairs:
            return
        print(f"Fetching {len(pairs)} schedule(s) ({label})...")

        def one(pair):
            tid, name = pair
            games = fetch_wiaa_schedule(tid, name)
            time.sleep(DELAY)
            return tid, games

        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            schedule_cache.update(dict(ex.map(one, pairs)))

    print(f"Checking schedules with {WORKERS} parallel workers...")
    fetch_batch([(e["team_id"], e["school"]) for e in schools
                 if e["state"] == "WI"], "tracked teams")

    week_by_entry = [
        games_in_range(schedule_cache.get(e["team_id"], []), week_start, week_end)
        for e in schools
    ]

    # Pass B: week-game opponents (their record + standing appear in rows)
    display_opps = {g["opp_team_id"]
                    for wg in week_by_entry for g in wg if g["opp_team_id"]}
    fetch_batch([(t, "") for t in sorted(display_opps)], "week opponents")

    # Pass C: conference rivals of every displayed team — needed to rank
    # conference standings from (C)-game win percentages. Expands
    # iteratively because a rival's rivals (not yet played) are also
    # conference members whose records shape the standings.
    display_ids = ({e["team_id"] for e, wg in zip(schools, week_by_entry) if wg}
                   | display_opps)
    for round_no in range(1, 4):
        rivals = set()
        for tid in display_ids:
            rivals |= conf_component(tid, schedule_cache)
        new = sorted(rivals - set(schedule_cache))
        if not new:
            break
        fetch_batch([(t, "") for t in new], f"conference rivals round {round_no}")

    running_store = {}

    school_results = []
    for entry, week_games in zip(schools, week_by_entry):
        if not week_games:
            continue
        for g in week_games:
            g["self_record"] = game_label(entry["team_id"], g,
                                          schedule_cache, running_store)
            g["opp_record"] = (game_label(g["opp_team_id"], g,
                                          schedule_cache, running_store)
                               if g["opp_team_id"] else "")
        print(f"[{entry['state']}] {entry['school']} — {entry['sport']}: "
              f"{len(week_games)} game(s)")
        for g in week_games:
            ha = "vs." if g["is_home"] else " @"
            srec = f" ({g['self_record']})" if g["self_record"] else ""
            orec = f" ({g['opp_record']})" if g["opp_record"] else ""
            sc = f"  {g['result']} {g['score']}" if g["played"] else "  (no score)"
            print(f"       {g['date']}{srec} {ha} {g['opponent']}{orec}{sc}")
        school_results.append({
            "school": entry["school"],
            "sport":  entry["sport"],
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
        rep = school_reps.get(_norm_school(r["school"]), "")
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
              f"{SCORES_RECIPIENT} instead of the rep\n")

    for rep, results in sorted(by_rep.items()):
        cfg     = rep_config.get(rep, {})
        html    = build_html(results, week_start, week_end)
        # Schools with a blank/unrecognized Sales Rep on the Schools tab
        # land in a clearly-labeled catch-all that only Andy receives.
        # (With SEND_EMPTY and no games at all, it's a plain "no games" notice.)
        if rep:
            who = rep
        elif results:
            who = "Unassigned Schools (no Sales Rep on sheet)"
        else:
            who = "No games last week"
        subject = f"{who} — School Scores — {week_label}"

        if rep and cfg.get("email"):
            intended_to = cfg["email"]
            intended_cc = cfg.get("cc")
        else:
            intended_to = SCORES_RECIPIENT
            intended_cc = None

        if SCORES_LIVE:
            to_addr, cc_addr = intended_to, intended_cc
            bcc_addr = SCORES_RECIPIENT  # Andy sees every rep's email
        else:
            # Test mode: everything to Andy's address (SCORES_RECIPIENT,
            # e.g. andy@bsgsports.com), still sent from the Gmail account.
            to_addr, cc_addr, bcc_addr = SCORES_RECIPIENT, None, None
            if intended_to != SCORES_RECIPIENT:
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
