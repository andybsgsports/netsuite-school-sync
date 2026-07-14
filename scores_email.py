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

Sending: prefers Microsoft Graph (mail goes out genuinely FROM the
andy@bsgsports.com M365 mailbox) when OUTLOOK_* secrets are present and the
token has Mail.Send; otherwise falls back to Gmail SMTP.

Env vars:
  OUTLOOK_CLIENT_ID    Azure app id      (same as outlook contacts sync)
  OUTLOOK_TENANT_ID    Azure tenant id   (same as outlook contacts sync)
  OUTLOOK_TOKEN_CACHE  MSAL token cache JSON — must include Mail.Send scope
                       (re-run outlook_auth_setup.py once to add it)
  GMAIL_USER           fallback sender + default recipient
  GMAIL_APP_PASSWORD   Gmail app password (16-char, 2FA required)
  SCORES_CSV           path to CSV  (default: scores_schools.csv)
  SCORES_RECIPIENT     override recipient for the whole run
  SCHOOL_FILTER        substring filter on school name (testing)
  DRY_RUN              "1" → print instead of send
  DUMP_HTML            "1" → write raw schedule HTML + parse diagnostics
"""

import csv
import os
import re
import smtplib
import time
from datetime import date, timedelta
from email.message import EmailMessage
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────
GMAIL_USER         = os.environ.get("GMAIL_USER", "")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
SCORES_CSV         = os.environ.get("SCORES_CSV", "scores_schools.csv")
SCORES_RECIPIENT   = os.environ.get("SCORES_RECIPIENT", "").strip() or GMAIL_USER
DRY_RUN            = os.environ.get("DRY_RUN", "0") == "1"
DUMP_HTML          = os.environ.get("DUMP_HTML", "0") == "1"
SCHOOL_FILTER      = os.environ.get("SCHOOL_FILTER", "").strip().lower()

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
    """Return (monday, sunday) of the week before `today`. When run on a
    Monday this is the immediately preceding Mon–Sun; run mid-week it still
    reports the last full completed week."""
    if today is None:
        today = date.today()
    this_monday = today - timedelta(days=today.weekday())
    last_monday = this_monday - timedelta(days=7)
    last_sunday = this_monday - timedelta(days=1)
    return last_monday, last_sunday


def games_in_range(games, start, end):
    """Filter game list to those with start <= date <= end."""
    return [g for g in games if start <= g["date"] <= end]


# ── HTML email ────────────────────────────────────────────────────────────────
def build_html(school_results, week_start, week_end):
    """
    school_results: list of {"school": str, "sport": str, "games": [game_dict, ...]}
    Returns an HTML string covering the week week_start–week_end.
    """
    date_str = (f"Week of {week_start.strftime('%B %d')} – "
                f"{week_end.strftime('%B %d, %Y')}")

    rows = []
    for item in school_results:
        for g in sorted(item["games"], key=lambda x: x["date"]):
            ha = "vs." if g["is_home"] else "@"

            if g["played"]:
                color = {"W": "#2e7d32", "L": "#c62828", "T": "#555"}.get(g["result"], "#555")
                label = g["result"] or "Final"
                sc    = f"&nbsp;&nbsp;{g['score']}" if g["score"] else ""
                result_html = f'<strong style="color:{color}">{label}{sc}</strong>'
            else:
                result_html = '<span style="color:#888;font-style:italic">No score reported</span>'

            lvl = g.get("level", "")
            lvl_html = (f' <span style="font-size:11px;color:#888">({lvl})</span>'
                        if lvl and lvl.lower() not in ("", "varsity", "1") else "")

            rows.append(f"""
        <tr>
          <td style="padding:9px 14px;border-bottom:1px solid #eee;white-space:nowrap;color:#555">
            {g["date"].strftime("%a %m/%d")}
          </td>
          <td style="padding:9px 14px;border-bottom:1px solid #eee;white-space:nowrap">
            {item["school"]}{lvl_html}
          </td>
          <td style="padding:9px 14px;border-bottom:1px solid #eee;color:#555">
            {item["sport"]}
          </td>
          <td style="padding:9px 14px;border-bottom:1px solid #eee">
            {ha} {g["opponent"]}
          </td>
          <td style="padding:9px 14px;border-bottom:1px solid #eee;text-align:center">
            {result_html}
          </td>
        </tr>""")

    rows_html = "".join(rows) if rows else """
        <tr>
          <td colspan="5" style="padding:20px;text-align:center;color:#999">
            No games found last week.
          </td>
        </tr>"""

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

    <div style="padding:24px 28px">
      <table width="100%" cellpadding="0" cellspacing="0"
             style="border-collapse:collapse;font-size:14px">
        <thead>
          <tr style="background:#f5f5f5">
            <th style="padding:10px 14px;text-align:left;border-bottom:2px solid #ddd;
                       font-weight:600">Date</th>
            <th style="padding:10px 14px;text-align:left;border-bottom:2px solid #ddd;
                       font-weight:600">School</th>
            <th style="padding:10px 14px;text-align:left;border-bottom:2px solid #ddd;
                       font-weight:600">Sport</th>
            <th style="padding:10px 14px;text-align:left;border-bottom:2px solid #ddd;
                       font-weight:600">Opponent</th>
            <th style="padding:10px 14px;text-align:center;border-bottom:2px solid #ddd;
                       font-weight:600">Result</th>
          </tr>
        </thead>
        <tbody>{rows_html}
        </tbody>
      </table>
    </div>

    <div style="padding:14px 28px;background:#f9f9f9;border-top:1px solid #eee;
                font-size:12px;color:#aaa">
      Automated digest · Scores via WIAA (wiaawi.org) · BSG Sports
    </div>
  </div>
</body>
</html>"""


# ── Email sender ──────────────────────────────────────────────────────────────
OUTLOOK_CLIENT_ID   = os.environ.get("OUTLOOK_CLIENT_ID", "").strip()
OUTLOOK_TENANT_ID   = os.environ.get("OUTLOOK_TENANT_ID", "").strip()
OUTLOOK_TOKEN_CACHE = os.environ.get("OUTLOOK_TOKEN_CACHE", "").strip()


def _graph_token():
    """Acquire a Graph access token with Mail.Send via the saved MSAL cache
    (same cache the Outlook contacts sync uses). Returns token or None."""
    if not (OUTLOOK_CLIENT_ID and OUTLOOK_TENANT_ID and OUTLOOK_TOKEN_CACHE):
        return None
    try:
        from msal import PublicClientApplication, SerializableTokenCache
        cache = SerializableTokenCache()
        cache.deserialize(OUTLOOK_TOKEN_CACHE)
        app = PublicClientApplication(
            OUTLOOK_CLIENT_ID,
            authority=f"https://login.microsoftonline.com/{OUTLOOK_TENANT_ID}",
            token_cache=cache,
        )
        accounts = app.get_accounts()
        if not accounts:
            return None
        result = app.acquire_token_silent(["Mail.Send"], account=accounts[0])
        if result and "access_token" in result:
            return result["access_token"]
    except Exception as e:
        print(f"  [WARN] Graph auth failed: {e}")
    return None


def send_via_graph(subject, html_body, recipient, token):
    """Send from the signed-in M365 mailbox (andy@bsgsports.com) via Graph."""
    payload = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": html_body},
            "toRecipients": [{"emailAddress": {"address": recipient}}],
        },
        "saveToSentItems": True,
    }
    r = requests.post(
        "https://graph.microsoft.com/v1.0/me/sendMail",
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )
    if r.status_code == 202:
        return True
    print(f"  [WARN] Graph sendMail failed: {r.status_code} {r.text[:200]}")
    return False


def send_via_gmail(subject, html_body, recipient):
    """Fallback: send HTML email via Gmail SMTP (TLS)."""
    if not (GMAIL_USER and GMAIL_APP_PASSWORD):
        print("  [WARN] GMAIL_USER / GMAIL_APP_PASSWORD not set — skipping send")
        return False
    msg = EmailMessage()
    msg["From"]    = GMAIL_USER
    msg["To"]      = recipient
    msg["Subject"] = subject
    msg.set_content("Please open this email in an HTML-capable client.")
    msg.add_alternative(html_body, subtype="html")
    with smtplib.SMTP("smtp.gmail.com", 587) as s:
        s.starttls()
        s.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        s.send_message(msg)
    return True


def send_email(subject, html_body, recipient):
    """Prefer Graph (true From: andy@bsgsports.com); fall back to Gmail."""
    token = _graph_token()
    if token:
        print("  [MAIL] Sending via Microsoft Graph (from bsgsports mailbox)")
        if send_via_graph(subject, html_body, recipient, token):
            return True
        print("  [MAIL] Graph failed — falling back to Gmail SMTP")
    else:
        print("  [MAIL] Graph not configured (or token lacks Mail.Send) — using Gmail SMTP")
    return send_via_gmail(subject, html_body, recipient)


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

    school_results = []

    for entry in schools:
        school  = entry["school"]
        sport   = entry["sport"]
        team_id = entry["team_id"]
        state   = entry["state"]

        print(f"[{state}] {school} — {sport}  (TeamID={team_id})")

        if state == "WI":
            all_games = fetch_wiaa_schedule(team_id, school)
        elif state == "IL":
            # IHSA schedule scraping — coming soon. TeamID column should hold
            # the IHSA identifier once the IL schedule URL pattern is confirmed.
            print(f"  [TODO] IHSA schedule not yet implemented for {school}")
            all_games = []
        else:
            print(f"  [SKIP] Unknown state '{state}' — skipping")
            all_games = []

        week_games = games_in_range(all_games, week_start, week_end)
        if week_games:
            print(f"  → {len(week_games)} game(s) last week")
            for g in week_games:
                ha = "vs." if g["is_home"] else " @"
                sc = f"  {g['result']} {g['score']}" if g["played"] else "  (no score)"
                print(f"       {g['date']} {ha} {g['opponent']}{sc}")
            school_results.append({
                "school": school,
                "sport":  sport,
                "games":  week_games,
            })
        else:
            print(f"  → no games last week")

        time.sleep(DELAY)

    print()
    if not school_results:
        print("[OK] No games last week for any tracked school — no email sent.\n")
        return

    played_count = sum(
        1 for r in school_results for g in r["games"] if g["played"]
    )
    print(f"Games last week: {sum(len(r['games']) for r in school_results)} total, "
          f"{played_count} with scores")

    html      = build_html(school_results, week_start, week_end)
    subject   = (f"BSG Sports Scores — Week of {week_start.strftime('%b %d')}"
                 f" – {week_end.strftime('%b %d, %Y')}")
    recipient = SCORES_RECIPIENT

    if DRY_RUN:
        print(f"\n[DRY RUN] Subject: {subject}")
        print(f"[DRY RUN] Recipient: {recipient}")
        print(f"[DRY RUN] Schools with games last week:")
        for r in school_results:
            print(f"  • {r['school']} ({r['sport']})")
        Path("dry_run_email.html").write_text(html, encoding="utf-8")
        print("[DRY RUN] Email HTML written to dry_run_email.html")
    else:
        ok = send_email(subject, html, recipient)
        if ok:
            print(f"\n[OK] Sent scores email to {recipient}")
        else:
            print(f"\n[WARN] Email not sent (check credentials)")


if __name__ == "__main__":
    main()
