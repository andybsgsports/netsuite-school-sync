# ihsa_batch_runner.py
# ------------------------------------------------------------
# Batch IHSA "details" scraper — applies your Antioch logic to all schools.
# Output (no Website column): Job Title | First Name | Last Name | Email | School | State
# ------------------------------------------------------------

import re, csv, sys, time
from pathlib import Path
from typing import Dict, Tuple, List, Optional
import pandas as pd

# ---------- CONFIG ----------
INPUT_XLSX = Path(r"C:\Users\andre\OneDrive - Badger Sporting Goods\Desktop\Illinois Contact List\Andy-Illinois List of Schools.xlsx")
OUT_DIR    = Path(r"C:\Users\andre\OneDrive - Badger Sporting Goods\Desktop\Illinois Contact List\IHSA-Batch-Output")
WRITE_PER_SCHOOL = False  # set True to also write per-school files in OUT_DIR\schools

# Optional helper CSVs (if you maintain overrides/patterns):
EXCEPTIONS_CSV   = Path(r"C:\Users\andre\OneDrive - Badger Sporting Goods\Desktop\Illinois Contact List\name_exceptions.csv")  # Email,First Name,Last Name
DOMAIN_RULES_CSV = Path(r"C:\Users\andre\OneDrive - Badger Sporting Goods\Desktop\Illinois Contact List\domain_rules.csv")    # domain,pattern

# Built-in domain rules (extendable via DOMAIN_RULES_CSV)
BUILTIN_DOMAIN_RULES: Dict[str, str] = {
    "chsd117.org": "first.last",
}

SAFE_COLS = ["Job Title","First Name","Last Name","Email","School","State"]

# ---------- Selenium ----------
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def make_driver() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1440,2400")
    opts.add_argument("--log-level=3")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    service = Service(log_path="NUL")  # suppress chromedriver logs on Windows
    return webdriver.Chrome(options=opts, service=service)

# ---------- Helpers ----------
EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$", re.I)
HONORIFICS = ("mr.","mr","mrs.","mrs","ms.","ms","dr.","dr","coach")
PHONE_RE = re.compile(r"\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b", re.I)
ROLE_COUNT_SUFFIX = re.compile(r"\s*\(\s*\d+\s*\)\s*$")
DETAILS_RE = re.compile(r"/schools/details/(\d+)", re.I)
ID_RE = re.compile(r"^\d{3,5}$")  # 3–5 digits (e.g., 0114)

def norm(s: str) -> str:
    return re.sub(r"\s+"," ", ("" if s is None else str(s)).replace("\xa0"," ").strip())

def is_valid_email(e: str) -> bool:
    return bool(EMAIL_RE.match(norm(e))) if isinstance(e,str) else False

def strip_honorifics(name: str) -> str:
    t = norm(name); tl = t.lower()
    for h in HONORIFICS:
        if tl.startswith(h + " "):
            return t[len(h)+1:].strip()
    return t

def split_first_space(name: str) -> Tuple[str,str]:
    name = strip_honorifics(name)
    if not name: return "",""
    parts = name.split()
    return (parts[0].title(), " ".join(parts[1:]).title()) if len(parts) > 1 else (parts[0].title(), "")

def load_exceptions(path: Path) -> Dict[str, Tuple[str,str]]:
    out = {}
    if path.exists():
        with path.open(newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                e = norm(r.get("Email","")).lower()
                fn = norm(r.get("First Name","")); ln = norm(r.get("Last Name",""))
                if e and fn and ln: out[e] = (fn, ln)
    return out

def load_domain_rules(csv_path: Path) -> Dict[str,str]:
    rules = dict(BUILTIN_DOMAIN_RULES)
    if csv_path.exists():
        with csv_path.open(newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                d = norm(r.get("domain","")).lower()
                p = norm(r.get("pattern","")).lower()
                if d and p: rules[d] = p
    return rules

def apply_rule(rule: str, local: str) -> Tuple[str,str]:
    loc = re.sub(r"\d+$","", local)
    def split2():
        t = re.split(r"[._-]+", loc)
        return (t[0], t[1]) if len(t) >= 2 else (None, None)
    if rule == "first.last":
        a,b = split2();  return (a.title(), b.title()) if a and b else ("","")
    if rule == "last.first":
        a,b = split2();  return (b.title(), a.title()) if a and b else ("","")
    if rule == "f.last":
        a,b = split2();  return (a.upper()+".", b.title()) if a and b and len(a)==1 else ("","")
    if rule == "first.l":
        a,b = split2();  return (a.title(), b.upper()+".") if a and b and len(b)==1 else ("","")
    if rule == "flast":
        m = re.match(r"^([a-z])([a-z]+)", loc);  return (m.group(1).upper()+".", m.group(2).title()) if m else ("","")
    if rule == "firstl":
        m = re.match(r"^([a-z]+)([a-z])$", loc); return (m.group(1).title(), m.group(2).upper()+".") if m else ("","")
    if rule == "lastf":
        m = re.match(r"^([a-z]+)([a-z])$", loc); return (m.group(2).upper()+".", m.group(1).title()) if m else ("","")
    if rule == "first": return (loc.title(), "")
    if rule == "last":  return ("", loc.title())
    return "",""

def infer_from_email(email: str, domain_rules: Dict[str,str], exceptions: Dict[str,Tuple[str,str]]) -> Tuple[str,str]:
    e = norm(email).lower()
    if not is_valid_email(e): return "",""
    if e in exceptions: return exceptions[e]
    local, domain = e.split("@",1)
    rule = domain_rules.get(domain)
    if rule:
        fn, ln = apply_rule(rule, local)
        if fn or ln: return fn, ln
    parts = [p for p in re.split(r"[._-]+", re.sub(r"[^a-z0-9._-]","", local)) if p]
    if len(parts) >= 2 and len(parts[0])>1 and len(parts[1])>1:
        return (parts[0].title(), parts[1].title())
    m = re.match(r"^([a-z])([a-z]{2,})$", re.sub(r"\d+$","", local))
    return (m.group(1).upper()+".", m.group(2).title()) if m else ("","")

def clean_role(role: str) -> str:
    return ROLE_COUNT_SUFFIX.sub("", norm(role))

def keep_role(role: str) -> bool:
    """Map role text to your requested sections."""
    r = clean_role(role).lower()
    # Exclude assistant COACH (unless explicitly head)
    if "assistant" in r and "coach" in r and "head" not in r:
        return False

    # Administration
    admin_terms = (
        "other administrator","athletic director","athletic supervisor",
        "principal","superintendent","activities director","dean of students"
    )
    if any(t in r for t in admin_terms):
        return True

    # Athletic Medical Staff
    if any(t in r for t in ("athletic trainer","head athletic trainer","athletic training","athletic medical")):
        return True

    # Boys/Girls Athletics — Head Coaches only
    if "head coach" in r and ("boys" in r or "girls" in r):
        return True

    # Activities & Non-Competitive Activities — Head Coaches / Advisors / Directors
    if "head coach" in r:  # generic head coach (no boys/girls)
        return True
    if any(k in r for k in ("advisor","adviser","director","band","choir","orchestra","theatre","debate","esports")):
        return True

    return False

def ihsa_url_from_id_or_url(id_or_url: str) -> str:
    s = norm(id_or_url)
    return s if s.startswith("http") else f"https://www.ihsa.org/schools/details/{s.zfill(4)}"

# ---------- Extraction ----------
def extract_card_from_mailto(a_tag) -> dict:
    """Parse a single contact where we have a mailto <a>."""
    try:
        card = a_tag.find_element(By.XPATH, "ancestor::li[1]")
    except Exception:
        try:
            card = a_tag.find_element(By.XPATH, "ancestor::div[1]")
        except Exception:
            return {}
    email = (a_tag.get_attribute("href") or "").replace("mailto:", "").strip()
    lines = [norm(x) for x in card.text.split("\n") if norm(x)]

    # name-first pass
    name_idx = None
    for i, t in enumerate(lines):
        if "@" in t or PHONE_RE.search(t): continue
        if len(strip_honorifics(t).split()) >= 2 and len(t) <= 60:
            name_idx = i; break
    name = strip_honorifics(lines[name_idx]) if name_idx is not None else ""

    # role after name
    role = ""
    if name_idx is not None:
        for j in range(name_idx+1, min(name_idx+6, len(lines))):
            t = lines[j]
            if "@" in t or PHONE_RE.search(t): continue
            role = clean_role(t); break

    # fallback: role-first then name
    if not role or not name:
        role_idx = None
        for i,t in enumerate(lines):
            if "@" in t or PHONE_RE.search(t): continue
            lt = t.lower()
            if any(k in lt for k in ("director","coach","advisor","adviser","principal","superintendent","administrator","trainer")):
                role_idx = i; break
        if role_idx is not None and not role:
            role = clean_role(lines[role_idx])
        if role_idx is not None and not name:
            for j in range(role_idx+1, min(role_idx+6, len(lines))):
                t = lines[j]
                if "@" in t or PHONE_RE.search(t): continue
                nm = strip_honorifics(t)
                if len(nm) >= 2:
                    name = nm; break

    return {"Name": name, "Role": role, "Email": email}

def extract_people(driver: webdriver.Chrome) -> List[dict]:
    people = []

    # Strategy 1: mailto anchors
    mail_links = driver.find_elements(By.XPATH, "//a[starts-with(translate(@href,'MAILTO','mailto'),'mailto:')]")
    for a in mail_links:
        row = extract_card_from_mailto(a)
        if row and row["Email"] and row["Role"] and keep_role(row["Role"]):
            people.append(row)

    # Strategy 2: plaintext emails anywhere
    candidates = driver.find_elements(By.XPATH, "//*[contains(text(),'@')]")
    seen_emails = set(p["Email"].lower() for p in people if p.get("Email"))
    for el in candidates:
        txt = (el.text or "").strip()
        for m in re.finditer(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", txt):
            email = m.group(0)
            if email.lower() in seen_emails:
                continue
            # climb to a reasonable container and parse similarly
            card = None
            try:
                card = el.find_element(By.XPATH, "ancestor::li[1]")
            except Exception:
                try:
                    card = el.find_element(By.XPATH, "ancestor::div[1]")
                except Exception:
                    continue
            lines = [norm(x) for x in card.text.split("\n") if norm(x)]

            # parse name/role with both orders
            def parse_name_role(lines):
                # name-first
                name_idx = None
                for i,t in enumerate(lines):
                    if "@" in t or PHONE_RE.search(t): continue
                    if len(strip_honorifics(t).split()) >= 2 and len(t) <= 60:
                        name_idx = i; break
                if name_idx is not None:
                    for j in range(name_idx+1, min(name_idx+6, len(lines))):
                        t = lines[j]
                        if "@" in t or PHONE_RE.search(t): continue
                        return strip_honorifics(lines[name_idx]), clean_role(t)
                # role-first
                role_idx = None
                for i,t in enumerate(lines):
                    if "@" in t or PHONE_RE.search(t): continue
                    lt = t.lower()
                    if any(k in lt for k in ("director","coach","advisor","adviser","principal","superintendent","administrator","trainer")):
                        role_idx = i; break
                if role_idx is not None:
                    role = clean_role(lines[role_idx])
                    for j in range(role_idx+1, min(role_idx+6, len(lines))):
                        t = lines[j]
                        if "@" in t or PHONE_RE.search(t): continue
                        nm = strip_honorifics(t)
                        if len(nm) >= 2:
                            return nm, role
                return "", ""
            name, role = parse_name_role(lines)

            row = {"Name": name, "Role": role, "Email": email}
            if row["Email"] and row["Role"] and keep_role(row["Role"]):
                people.append(row)
                seen_emails.add(email.lower())

    # de-dupe
    uniq = {}
    for p in people:
        key = (p.get("Email","").lower(), p.get("Role","").lower(), p.get("Name","").lower())
        if key not in uniq:
            uniq[key] = p
    return list(uniq.values())

# ---------- Build & Write ----------
def build_dataframe(people: List[dict], school: str, state: str,
                    domain_rules: Dict[str,str], exceptions: Dict[str,Tuple[str,str]]) -> pd.DataFrame:
    rows=[]
    for p in people:
        name = norm(p.get("Name","")); role=norm(p.get("Role","")); email=norm(p.get("Email",""))
        fn, ln = split_first_space(name)          # page name wins
        if is_valid_email(email):                 # fill blanks only from email
            ef, el = infer_from_email(email, domain_rules, exceptions)
            fn = fn or ef; ln = ln or el
        rows.append({
            "Job Title": role,
            "First Name": fn.title() if fn else "",
            "Last Name":  ln.title() if ln else "",
            "Email": email,
            "School": school or "",
            "State":  state or "IL",
        })
    df = pd.DataFrame(rows, columns=SAFE_COLS).fillna("")
    return df.drop_duplicates(subset=["Job Title","First Name","Last Name","Email","School"])

def write_xlsx(df: pd.DataFrame, path: Path, sheet_name: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="xlsxwriter") as w:
        df.to_excel(w, index=False, sheet_name=sheet_name[:31])
        ws = w.sheets[sheet_name[:31]]
        if not df.empty:
            ws.autofilter(0,0,len(df), len(df.columns)-1)

# ---------- Input loader (robust + tolerant) ----------
def _open_excel_robust(path: Path) -> pd.ExcelFile:
    last_err = None
    for attempt in range(12):  # ~18 seconds total to survive locks
        try:
            return pd.ExcelFile(path)
        except PermissionError as e:
            last_err = e
            time.sleep(1.5)
    raise SystemExit(
        f"[ERROR] Could not open {path} (file is locked). "
        f"Close Excel, turn off Explorer Preview Pane (Alt+P), or pause OneDrive sync and retry.\n{last_err}"
    )

def _resolve(colnames, candidates):
    lower = {c.lower(): c for c in colnames}
    for key in candidates:
        if key in lower: return lower[key]
    for c in colnames:
        lc = c.lower()
        if any(key in lc for key in candidates): return c
    return None

def load_batch(path: Path) -> pd.DataFrame:
    xls = _open_excel_robust(path)
    # Use first non-empty sheet
    for sh in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sh, dtype=str)
        if len(df): break
    df = df.fillna("")
    df.columns = [str(c).strip() for c in df.columns]

    school_col = _resolve(df.columns, {"school","school name","name","high school"})
    state_col  = _resolve(df.columns, {"state"})

    # Detect columns that contain IHSA URLs or numeric IDs
    url_cols, id_cols = [], []
    for c in df.columns:
        s = df[c].astype(str)
        if s.str.contains(r"ihsa\.org/schools/details/\d+", case=False, regex=True).any():
            url_cols.append(c)
        if s.str.fullmatch(ID_RE).any():
            id_cols.append(c)

    rows = []
    for _, r in df.iterrows():
        school = norm(r[school_col]) if school_col else ""
        state  = norm(r[state_col]) if state_col else "IL"

        ihsa_url = ""
        ihsa_id  = ""
        for c in url_cols:
            val = norm(str(r[c]))
            m = DETAILS_RE.search(val)
            if m:
                ihsa_url = val
                ihsa_id = m.group(1)
                break
        if not ihsa_id:
            for c in id_cols:
                val = norm(str(r[c]))
                if ID_RE.fullmatch(val):
                    ihsa_id = val
                    break

        if ihsa_url or ihsa_id:
            rows.append({"School": school, "IHSA_URL": ihsa_url, "IHSA_ID": ihsa_id, "State": state})

    out = pd.DataFrame(rows)
    if out.empty:
        raise SystemExit("[ERROR] No rows with IHSA URLs or IDs found in the input workbook.")
    return out.reset_index(drop=True)

# ---------- Main ----------
def ihsa_url_or_id_to_key(row) -> str:
    return row["IHSA_URL"] or row["IHSA_ID"]

def sanitize_filename(name: str) -> str:
    bad = r'<>:"/\|?*'
    t = "".join(("_" if ch in bad else ch) for ch in name)
    return t.strip().rstrip(".")

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "schools").mkdir(parents=True, exist_ok=True)

    batch = load_batch(INPUT_XLSX)
    exceptions   = load_exceptions(EXCEPTIONS_CSV)
    domain_rules = load_domain_rules(DOMAIN_RULES_CSV)

    combined_parts = []
    for i, row in batch.iterrows():
        school = row["School"]
        state  = row["State"] or "IL"
        ihsa_key = ihsa_url_or_id_to_key(row)

        print(f"[RUN] {school or '(no school name)'}  |  {ihsa_key}")
        driver = make_driver()
        try:
            driver.get(ihsa_url_from_id_or_url(ihsa_key))
            # Wait for page to load (don't depend on mailto existing)
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

            # Fallback: if School was blank in the list, grab the page header
            if not school:
                try:
                    hdr = driver.find_element(By.XPATH, "//h1|//h2")
                    school = norm(hdr.text)
                except Exception:
                    pass

            people = extract_people(driver)
        finally:
            try: driver.quit()
            except Exception: pass

        df = build_dataframe(people, school=school, state=state, domain_rules=domain_rules, exceptions=exceptions)
        combined_parts.append(df)

        if WRITE_PER_SCHOOL:
            per_path = OUT_DIR / "schools" / f"{sanitize_filename(school or row['IHSA_ID'] or 'school')}.xlsx"
            write_xlsx(df, per_path, sheet_name="IHSA")

        time.sleep(0.6)  # polite delay

    combined = pd.concat(combined_parts, ignore_index=True) if combined_parts else pd.DataFrame(columns=SAFE_COLS)
    combined_path = OUT_DIR / "IHSA-Batch-Combined.xlsx"
    combined_path.parent.mkdir(parents=True, exist_ok=True)  # ensure folder exists
    with pd.ExcelWriter(combined_path, engine="xlsxwriter") as w:
        combined.to_excel(w, index=False, sheet_name="All")
        ws = w.sheets["All"]
        if not combined.empty:
            ws.autofilter(0,0,len(combined), len(combined.columns)-1)
    print(f"[DONE] Combined → {combined_path}")

if __name__ == "__main__":
    main()
