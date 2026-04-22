"""
netsuite_sync.py
----------------
Syncs school data from WIAA/IHSA into NetSuite Customer and Contact records.
Called by the master sync script.
"""

import os
import requests
import time
import random
import string
import hmac
import hashlib
import base64
import re
from urllib.parse import quote, urlparse
from bs4 import BeautifulSoup

# ============================================================
# CREDENTIALS - loaded from env vars or .env file
# ============================================================
def _load_dotenv():
    """Load .env file from the script's directory if it exists."""
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    os.environ.setdefault(key.strip(), val.strip())

_load_dotenv()

NS_ACCOUNT      = os.environ.get("NS_ACCOUNT",      "")
NS_CONSUMER_KEY = os.environ.get("NS_CONSUMER_KEY",  "")
NS_CONSUMER_SEC = os.environ.get("NS_CONSUMER_SEC",  "")
NS_TOKEN_KEY    = os.environ.get("NS_TOKEN_KEY",     "")
NS_TOKEN_SEC    = os.environ.get("NS_TOKEN_SEC",     "")
BASE_URL        = f"https://{NS_ACCOUNT}.suitetalk.api.netsuite.com/services/rest/record/v1"

# Custom field IDs
CF_LEVEL      = "custentity_school_level"
CF_NICKNAME   = "custentity_school_nickname"
CF_COLORS     = "custentity_school_colors"
CF_CONFERENCE = "custentity_school_conference"
CF_DISTRICT   = "custentity_wiaa_district"
CF_SIZE       = "custentity_school_size"
CF_ENROLLMENT = "custentity_school_enrollment"
CF_STATE      = "custentity_school_state"
CF_CLASS      = "custentity_school_class"  # internal ID: 4776

# Sales rep name -> NetSuite employee internal ID mapping
SALES_REP_MAP = {
    "Andrew Murray": "3",
}

# WIAA nav h5s to skip when parsing
NAV_H5S = {
    "Schools", "Contests", "General", "Tournaments", "Conferences",
    "School", "Conference", "Officials", "All Sports", "Football",
    "Golf", "Soccer",
}

SKIP_SITES = {
    "wiaawi.org", "google.com", "facebook.com", "twitter.com",
    "instagram.com", "youtube.com", "officials.wiaawi", "halftime.wiaawi",
}

WIAA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://schools.wiaawi.org/Directory/School/List",
}

# ============================================================
# AUTH
# ============================================================
def make_auth(method, full_url):
    parsed   = urlparse(full_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    nonce    = "".join(random.choices(string.ascii_letters + string.digits, k=11))
    ts       = str(int(time.time()))
    op = {
        "oauth_consumer_key":     NS_CONSUMER_KEY,
        "oauth_nonce":            nonce,
        "oauth_signature_method": "HMAC-SHA256",
        "oauth_timestamp":        ts,
        "oauth_token":            NS_TOKEN_KEY,
        "oauth_version":          "1.0",
    }
    all_p = dict(op)
    if parsed.query:
        for part in parsed.query.split("&"):
            if "=" in part:
                k, v = part.split("=", 1)
                all_p[k] = v
    sp = "&".join(f"{quote(k,safe='')}={quote(v,safe='')}"
                  for k, v in sorted(all_p.items()))
    bs = "&".join([method.upper(), quote(base_url, safe=''), quote(sp, safe='')])
    sk = f"{quote(NS_CONSUMER_SEC,safe='')}&{quote(NS_TOKEN_SEC,safe='')}"
    sig = base64.b64encode(
        hmac.new(sk.encode(), bs.encode(), hashlib.sha256).digest()).decode()
    return (f'OAuth realm="{NS_ACCOUNT}",oauth_consumer_key="{NS_CONSUMER_KEY}",'
            f'oauth_token="{NS_TOKEN_KEY}",oauth_signature_method="HMAC-SHA256",'
            f'oauth_timestamp="{ts}",oauth_nonce="{nonce}",oauth_version="1.0",'
            f'oauth_signature="{quote(sig,safe="")}"')

def ns_get(path):
    url = f"{BASE_URL}/{path}"
    return requests.get(url, headers={
        "Authorization": make_auth("GET", url),
        "Content-Type": "application/json"})

def ns_post(path, body):
    url = f"{BASE_URL}/{path}"
    return requests.post(url, headers={
        "Authorization": make_auth("POST", url),
        "Content-Type": "application/json"}, json=body)

def ns_patch(path, body):
    url = f"{BASE_URL}/{path}"
    return requests.patch(url, headers={
        "Authorization": make_auth("PATCH", url),
        "Content-Type": "application/json"}, json=body)

def ns_delete(path):
    url = f"{BASE_URL}/{path}"
    return requests.delete(url, headers={
        "Authorization": make_auth("DELETE", url),
        "Content-Type": "application/json"})

SUITEQL_URL = f"https://{NS_ACCOUNT}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"

def ns_suiteql(query, limit=1000):
    """Run a SuiteQL query and return the list of result rows."""
    url = f"{SUITEQL_URL}?limit={limit}"
    r = requests.post(url, headers={
        "Authorization": make_auth("POST", url),
        "Content-Type": "application/json",
        "Prefer": "transient",
    }, json={"q": query})
    if r.status_code == 200:
        return r.json().get("items", [])
    return []

# ============================================================
# HELPERS
# ============================================================
def smart_title(s):
    """
    Proper-case a name without the bugs of str.title():
      - doesn't capitalize the letter after an apostrophe
        ('d'andrea' -> 'D'Andrea', not 'D'Andrea')  -- wait actually we want D'Andrea
        Actually: "o'brien" -> "O'Brien"  (cap after apostrophe IS wanted for names)
      - leaves already-mixed-case input alone ('McDonald' stays 'McDonald')
      - fixes all-caps ('JAMES HOVORKA' -> 'James Hovorka')
      - fixes all-lowercase ('james hovorka' -> 'James Hovorka')
    """
    t = str(s or "")
    if not t:
        return t
    if t != t.lower() and t != t.upper():
        return t  # mixed case already — trust it
    # Capitalize each word AND the letter after each apostrophe.
    return re.sub(
        r"[a-zA-Z]+",
        lambda m: m.group(0)[0].upper() + m.group(0)[1:].lower(),
        t,
    )


def slugify(name):
    s = name.upper().strip()
    s = re.sub(r"[^A-Z0-9]+", "-", s)
    return s.strip("-")[:50]

def extract_id_from_location(resp):
    loc = resp.headers.get("Location", "")
    m = re.search(r"/(\d+)$", loc)
    return m.group(1) if m else None

def decode_cf_email(encoded):
    """Decode Cloudflare-obfuscated email addresses."""
    try:
        r = int(encoded[:2], 16)
        return "".join(chr(int(encoded[i:i+2], 16) ^ r)
                       for i in range(2, len(encoded), 2))
    except:
        return ""

# ============================================================
# WIAA SCRAPER
# ============================================================
def scrape_wiaa_school_detail(wiaa_url):
    """
    Scrape a WIAA school detail page and return:
      - school info dict
      - list of admin contact dicts
      - list of coach contact dicts
    """
    info     = {}
    admins   = []
    coaches  = []

    try:
        resp = requests.get(wiaa_url, headers=WIAA_HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # ---- School info from h5 tags ----
        content_h5s = [h.get_text(strip=True) for h in soup.find_all("h5")
                       if h.get_text(strip=True) and h.get_text(strip=True) not in NAV_H5S]

        def h5(idx, default=""):
            return content_h5s[idx] if idx < len(content_h5s) else default

        info["level"]        = h5(0)
        info["school_class"] = h5(1)   # Public / Private
        info["wiaa_district"] = h5(6).replace("\xa0", " ").strip()
        info["school_size"]  = h5(7)

        # Dynamically find where the address block starts.
        # Strategy: find the h5 that contains "WI" or "IL" (state) then work backwards.
        # Address block is always: addr1, [addr2/building], city, state, zip, phone, fax
        # State is always a 2-letter abbreviation like "WI" or "IL"
        state_idx = None
        for _idx in range(8, len(content_h5s)):
            _val = content_h5s[_idx].strip()
            if _val in ("WI", "IL", "MN", "IA", "MI", "IN"):
                state_idx = _idx
                break

        if state_idx is not None:
            # Work backwards from state:
            # state-1 = city
            # state-2 = PO Box OR address1 (if no PO Box)
            # state-3 = address1 (if PO Box exists) OR conference
            city_val  = h5(state_idx - 1)
            prev2_val = h5(state_idx - 2)
            prev3_val = h5(state_idx - 3)

            if "BOX" in prev2_val.upper():
                # prev2 = PO Box, prev3 = address1
                info["address1"] = prev3_val
                info["address2"] = prev2_val
                conf_idx = state_idx - 4
            elif re.match(r"^[A-Za-z\s]+$", prev2_val) and not re.search(r"\d", prev2_val):
                # prev2 has no digits - could be building name (like "Patriots Hall")
                # Check prev3 for a street address
                if re.search(r"\d", prev3_val):
                    info["address1"] = prev3_val
                    info["address2"] = prev2_val
                    conf_idx = state_idx - 4
                else:
                    info["address1"] = prev2_val
                    info["address2"] = ""
                    conf_idx = state_idx - 3
            else:
                # prev2 is the street address
                info["address1"] = prev2_val
                info["address2"] = ""
                conf_idx = state_idx - 3

            info["city"]       = city_val
            info["state"]      = h5(state_idx)
            info["zip"]        = h5(state_idx + 1)
            info["phone"]      = h5(state_idx + 2)
            info["conference"] = h5(conf_idx)
            info["nickname"]   = h5(conf_idx - 1)
            info["colors"]     = h5(conf_idx - 2)
        else:
            # Fallback
            info["colors"]     = h5(9)
            info["nickname"]   = h5(10)
            info["conference"] = h5(11)
            info["address1"]   = h5(12)
            info["address2"]   = ""
            info["city"]       = h5(13)
            info["state"]      = h5(14)
            info["zip"]        = h5(15)
            info["phone"]      = h5(16)

        # Enrollment
        full_text = soup.get_text(" ")
        m = re.search(r"School:\s*(\d+)", full_text)
        info["enrollment"] = int(m.group(1)) if m else None

        # Website
        info["website"] = ""
        for a in soup.find_all("a", href=True):
            href = a["href"]
            txt  = a.get_text(strip=True)
            if txt == "Website" and href.startswith("http"):
                info["website"] = href
                break
        if not info["website"]:
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.startswith("http") and not any(s in href for s in SKIP_SITES):
                    info["website"] = href
                    break

        # ---- Admins ----
        admin_table = soup.find("table", {"id": "tblAdminList"})
        if admin_table:
            for row in admin_table.find_all("tr")[1:]:
                cols = row.find_all("td")
                if len(cols) >= 4:
                    role = cols[1].get_text(strip=True)
                    name = cols[2].get_text(" ", strip=True).strip()
                    etag = cols[3].find("span", {"class": "__cf_email__"})
                    email = decode_cf_email(etag["data-cfemail"]) if etag \
                            else cols[3].get_text(strip=True)
                    parts = name.split()
                    admins.append({
                        "first": parts[0] if parts else "",
                        "last":  " ".join(parts[1:]) if len(parts) > 1 else "",
                        "email": email,
                        "role":  role,
                        "type":  "Admin",
                    })

        # ---- Coaches ----
        coach_table = soup.find("table", {"id": "tblCoachList"})
        if coach_table:
            for row in coach_table.find_all("tr")[1:]:
                cols = row.find_all("td")
                if len(cols) >= 5:
                    sport = cols[1].get_text(strip=True)
                    name  = cols[2].get_text(" ", strip=True).strip()
                    role  = cols[3].get_text(strip=True)
                    etag  = cols[4].find("span", {"class": "__cf_email__"})
                    email = decode_cf_email(etag["data-cfemail"]) if etag \
                            else cols[4].get_text(strip=True)
                    parts = name.split()
                    # Clean up role: strip "Head Coach", "Assistant Coach" etc.
                    # Type becomes the coaching level, Role becomes the sport
                    role_clean = role.replace("Head Coach", "").replace("Assistant Coach", "").strip(" -")
                    coach_type = "Head Coach" if "Head" in role else "Assistant Coach" if "Assistant" in role else "Coach"
                    coaches.append({
                        "first": parts[0] if parts else "",
                        "last":  " ".join(parts[1:]) if len(parts) > 1 else "",
                        "email": email,
                        "role":  sport,
                        "type":  coach_type,
                    })

    except Exception as e:
        print(f"  [WIAA scrape warning] {wiaa_url}: {e}")

    return info, admins, coaches

# ============================================================
# CUSTOMER (SCHOOL) FUNCTIONS
# ============================================================
def get_customer_by_external_id(external_id):
    resp = ns_get(f"customer/eid:{external_id}")
    if resp.status_code == 200:
        return resp.json().get("id")
    return None

def build_address_items(school_info, contacts, school_name=""):
    """
    Build addressbook items list.
    Only creates one Ship-To per contact (Attn: First Last).
    Sets addressee explicitly to school_name so NetSuite doesn't
    prepend the customer number (e.g. "1669 Mount Horeb High School").
    """
    addr1 = school_info.get("address1", "")
    city  = school_info.get("city", "")
    st    = school_info.get("state", "")
    zp    = school_info.get("zip", "")
    items = []

    if addr1 and city:
        seen_names = set()
        for c in contacts:
            full_name = f"{c.get('first','')} {c.get('last','')}".strip()
            if not full_name or full_name in seen_names:
                continue
            seen_names.add(full_name)
            items.append({
                "defaultShipping": False,
                "defaultBilling":  False,
                "label":           full_name,
                "addressBookAddress": {
                    "addressee": school_name or "",
                    "attention": full_name,
                    "addr1":     addr1,
                    "city":      city,
                    "state":     st,
                    "zip":       zp,
                    "country":   {"id": "US"},
                }
            })

    return items

def sync_address_book(customer_id, school_info, contacts, school_name=""):
    """
    Sync Ship-To addresses for active contacts. Adds one Ship-To per
    contact that doesn't already have one (matched by label = contact name).

    NetSuite REST API PATCH always adds to addressBook — it cannot
    replace or clear. So we only add missing entries.
    Removals are handled by remove_contact_ship_to() when contacts depart.
    """
    addr1 = school_info.get("address1", "")
    city  = school_info.get("city", "")
    st    = school_info.get("state", "")
    zp    = school_info.get("zip", "")
    if not addr1 or not city:
        return

    # Deduplicated contact names
    seen = set()
    contact_names = []
    for c in contacts:
        name = f"{c.get('first','')} {c.get('last','')}".strip()
        if name and name not in seen:
            seen.add(name)
            contact_names.append(name)

    if not contact_names:
        return

    # Build the set of existing labels by reading each addressBook line.
    # expand=addressBook returns only links, not labels, so every line must
    # be fetched individually. We do NOT short-circuit on a sampled entry —
    # that was unreliable when the sample happened to be a Bill-To or a
    # pre-existing Ship-To, and it caused the same contacts to be re-added
    # on every run (80+ duplicate Ship-Tos on Barneveld as of Apr 2026).
    existing_labels = set()
    r = ns_get(f"customer/{customer_id}?expand=addressBook")
    if r.status_code == 200:
        items = r.json().get("addressBook", {}).get("items", [])
        for item in items:
            href = item.get("links", [{}])[0].get("href", "")
            line_id = href.rstrip("/").split("/")[-1] if href else None
            if not line_id:
                continue
            r2 = ns_get(f"customer/{customer_id}/addressBook/{line_id}")
            if r2.status_code == 200:
                lbl = (r2.json().get("label") or "").strip()
                if lbl:
                    existing_labels.add(lbl.lower())

    new_items = []
    for name in contact_names:
        if name.lower() not in existing_labels:
            new_items.append({
                "defaultShipping": False,
                "defaultBilling":  False,
                "label":           name,
                "addressBookAddress": {
                    "addressee": school_name or "",
                    "attention": name,
                    "addr1":     addr1,
                    "city":      city,
                    "state":     st,
                    "zip":       zp,
                    "country":   {"id": "US"},
                }
            })

    if new_items:
        r = ns_patch(f"customer/{customer_id}",
                     {"addressBook": {"items": new_items}})
        if r.status_code == 204:
            print(f"  [NS] Added {len(new_items)} new Ship-To address(es) "
                  f"({len(existing_labels)} already on record)")
        else:
            print(f"  [NS] WARN: address add failed: {r.status_code} {r.text[:150]}")
    else:
        print(f"  [NS] Ship-To addresses up to date "
              f"({len(contact_names)} contacts, {len(existing_labels)} on record)")

def _set_sales_team(customer_id, team_item):
    """
    Set the sales team on an existing customer.
    If a salesTeam line already exists, PATCH it in-place (changing employee).
    If no salesTeam exists, add via customer-level PATCH.
    """
    emp_name = team_item.get("employee", {}).get("id", "?")

    # Check for existing salesTeam items
    r = ns_get(f"customer/{customer_id}?expand=salesTeam")
    if r.status_code != 200:
        print(f"  [NS] WARN: could not read salesTeam for customer {customer_id}")
        return

    existing = r.json().get("salesTeam", {}).get("items", [])

    if existing:
        # PATCH the first existing line item in-place
        href = existing[0].get("links", [{}])[0].get("href", "")
        if "/v1/" in href:
            path = href.split("/v1/")[1]
            r2 = ns_patch(path, team_item)
            if r2.status_code == 204:
                print(f"  [NS] Updated Sales Team on customer {customer_id}")
            else:
                print(f"  [NS] WARN: salesTeam patch failed: {r2.status_code} {r2.text[:150]}")
    else:
        # No existing team — add via customer body
        r2 = ns_patch(f"customer/{customer_id}", {
            "salesTeam": {"items": [team_item]}
        })
        if r2.status_code == 204:
            print(f"  [NS] Added Sales Team on customer {customer_id}")
        else:
            print(f"  [NS] WARN: salesTeam add failed: {r2.status_code} {r2.text[:150]}")

def build_customer_body(school_name, state, school_info, contacts=None, sales_rep=None):
    """Build the full Customer record body."""
    level     = school_info.get("level", "")
    # Only append level if it's not already present in the school name
    if level and level.lower() not in school_name.lower():
        full_name = f"{school_name} {level}".strip()
    else:
        full_name = school_name
    external_id = slugify(school_name)
    st          = school_info.get("state", state)
    zp          = school_info.get("zip", "")
    school_class = school_info.get("school_class", "")

    body = {
        "companyName":  full_name,
        "externalId":   external_id,
        "isPerson":     False,
        "phone":        school_info.get("phone", ""),
        "url":          school_info.get("website", ""),
        CF_LEVEL:       level,
        CF_NICKNAME:    school_info.get("nickname", ""),
        CF_COLORS:      school_info.get("colors", ""),
        CF_CONFERENCE:  school_info.get("conference", ""),
        CF_DISTRICT:    school_info.get("wiaa_district", ""),
        CF_SIZE:        school_info.get("school_size", ""),
        CF_STATE:       st,
        CF_CLASS:       school_class,
    }
    if school_info.get("enrollment"):
        body[CF_ENROLLMENT] = school_info["enrollment"]

    # Sales rep (via salesTeam sublist — the salesRep field is ignored on this form)
    if sales_rep:
        emp_id = SALES_REP_MAP.get(sales_rep)
        if emp_id:
            body["salesTeam"] = {
                "items": [{
                    "employee": {"id": emp_id},
                    "salesRole": {"id": "-2"},  # -2 = "Sales Rep" role
                    "contribution": 100.0,
                    "isPrimary": True,
                }]
            }

    # Build addressbook with school addresses + per-contact Ship-Tos
    addr_items = build_address_items(school_info, contacts or [], school_name=full_name)
    if addr_items:
        body["addressBook"] = {"items": addr_items}

    return body

def sync_customer(school_name, state, school_info, contacts=None, ns_customer_id=None,
                  sales_rep=None):
    """
    Update or create a Customer record.

    If ns_customer_id is provided (from the NS Customer ID column in the Excel),
    it is used directly — no name lookup, no external ID lookup. This is the
    safe path for the 40 existing schools where we already know the correct ID.

    If ns_customer_id is blank, a new Customer is created and the new ID returned.
    The caller (school_netsuite_sync.py) is responsible for writing the new ID
    back to the Excel so future runs use the direct-ID path.

    Returns (ns_id, created_bool).
    """
    body = build_customer_body(school_name, state, school_info, contacts, sales_rep=sales_rep)

    if ns_customer_id:
        # ── Direct PATCH — bypass all name/externalId matching ──────────────
        # Handle salesTeam separately (can't add if one already exists)
        sales_team_data = body.pop("salesTeam", None)

        r = ns_patch(f"customer/{ns_customer_id}", body)
        if r.status_code == 204:
            print(f"  [NS] Updated Customer: {body['companyName']} (ID: {ns_customer_id})")
        else:
            print(f"  [NS] ERROR updating customer: {r.status_code} {r.text[:200]}")

        # Set sales team: PATCH existing line or add new one
        if sales_team_data:
            _set_sales_team(ns_customer_id, sales_team_data["items"][0])

        return ns_customer_id, False
    else:
        # ── No ID yet — create new Customer ─────────────────────────────────
        r = ns_post("customer", body)
        if r.status_code == 204:
            new_id = extract_id_from_location(r)
            print(f"  [NS] Created Customer: {body['companyName']} (ID: {new_id})")
            return new_id, True
        else:
            print(f"  [NS] ERROR creating customer: {r.status_code} {r.text[:200]}")
            return None, False

# ============================================================
# CONTACT FUNCTIONS
# ============================================================
def get_contact_by_external_id(external_id):
    resp = ns_get(f"contact/eid:{external_id}")
    if resp.status_code == 200:
        data = resp.json()
        return data.get("id"), data.get("isInactive", False)
    return None, None

def make_contact_external_id(email, school_name=None, *_ignored):
    """
    Contact external ID.

    Per-school keying when a school is supplied: `{school_slug}__{email}`. This
    gives co-op coaches (same email, multiple schools) their own NS contact
    record per school, so each school's Contacts tab shows them. NetSuite's
    REST API doesn't allow sharing one contact across customers via
    contactRoles — per-school records are the working alternative.

    When called without a school (e.g. cleanup tooling that dedupes by
    email), falls back to the email-only format `EM_{email}`.
    """
    email_clean = re.sub(r"[^a-z0-9@._-]", "", (email or "").lower())[:80]
    if school_name:
        return f"{slugify(school_name)}__{email_clean}"[:150]
    return f"EM_{email_clean}"[:150]


def _make_legacy_school_ext_id(school_name, email):
    """Prior format: SCHOOL__email (per-school dedup). Used for fallback lookup."""
    school_slug = slugify(school_name)
    email_clean = re.sub(r"[^a-z0-9@._-]", "", (email or "").lower())[:50]
    return f"{school_slug}__{email_clean}"[:150]


def _make_legacy_ext_id(school_name, email, role):
    """Oldest format: SCHOOL__ROLE__email. Used for fallback lookup."""
    school_slug = slugify(school_name)
    role_slug   = re.sub(r"[^A-Z0-9]+", "-", (role or "").upper().strip())[:30]
    email_clean = re.sub(r"[^a-z0-9@._-]", "", (email or "").lower())[:50]
    return f"{school_slug}__{role_slug}__{email_clean}"[:150]


FREE_EMAIL_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "msn.com",
    "aol.com", "icloud.com", "att.net", "comcast.net", "sbcglobal.net",
    "verizon.net", "mail.com", "live.com", "me.com", "ymail.com",
    "cox.net", "charter.net", "bellsouth.net", "earthlink.net",
    "protonmail.com", "pm.me", "proton.me", "duck.com", "fastmail.com",
}


def extract_email_domain(email):
    """'john@barneveld.k12.wi.us' -> 'barneveld.k12.wi.us'. Empty string if no @."""
    s = (email or "").strip().lower()
    if "@" not in s:
        return ""
    return s.split("@", 1)[1]


def compute_school_domain(contacts):
    """
    Given the contacts for one school, return that school's institutional
    email domain (the most common non-free domain), or "" if none clear.
    Used to decide whether a contact's email identifies *this* school as
    the person's home school (primary company link).
    """
    counts = {}
    for c in contacts or []:
        dom = extract_email_domain(c.get("email", ""))
        if not dom or dom in FREE_EMAIL_DOMAINS:
            continue
        counts[dom] = counts.get(dom, 0) + 1
    if not counts:
        return ""
    return max(counts.items(), key=lambda kv: kv[1])[0]


def find_contact_any_format(school_name, email, role="", customer_id=None):
    """
    Locate an existing NS contact by trying each external-ID format in order:
      1. canonical: {SCHOOL}__{email}          (per-school — current scheme)
      2. legacy:    EM_{email}                 (email-only era)  *claimed only
         if that contact's `company` already points at customer_id, so we
         don't steal a co-op coach's home-school record for a second school
      3. oldest:    {SCHOOL}__{ROLE}__{email}
    Returns (contact_id, is_inactive, found_via). None values if nothing found.
    """
    cid, inactive = get_contact_by_external_id(make_contact_external_id(email, school_name))
    if cid:
        return cid, inactive, "school_email"

    cid, inactive = get_contact_by_external_id(make_contact_external_id(email))  # EM_ legacy
    if cid:
        if customer_id is None:
            return cid, inactive, "email"
        r = ns_get(f"contact/{cid}?fields=company")
        if r.status_code == 200:
            company = (r.json().get("company") or {}).get("id", "")
            if str(company) == str(customer_id):
                return cid, inactive, "email"

    if role:
        cid, inactive = get_contact_by_external_id(_make_legacy_ext_id(school_name, email, role))
        if cid:
            return cid, inactive, "legacy_school_role"
    return None, None, None

def _find_contact_for_customer(customer_id, email):
    """Search for a contact under a customer by email using the contacts sublist.

    Note: SuiteQL and REST search are both blocked by the current role's
    permissions, so this falls back to the contactList expand. Many schools
    have empty contactList (contacts linked via company field but not the
    sublist), so this often returns None.
    """
    resp = ns_get(f"customer/{customer_id}?expand=contactList")
    if resp.status_code != 200:
        return None
    data = resp.json()
    contact_list = data.get("contactList", {}).get("items", [])
    for item in contact_list:
        c = item.get("fields", item)
        c_email = c.get("email", "")
        if c_email and c_email.lower() == email.lower():
            return c.get("contact", {}).get("id") or c.get("id")
    return None

def sync_contact(customer_id, school_name, contact_row, school_info):
    """
    Create or update a Contact linked to the Customer.
    contact_row: dict with first, last, email, role, type
    Returns contact NS internal ID.
    """
    # Normalize casing so ALL-CAPS rows from WIAA/IHSA get fixed in NS on
    # every sync ("JAMES HOVORKA" -> "James Hovorka").
    first = smart_title(contact_row.get("first", ""))
    last  = smart_title(contact_row.get("last", ""))
    email = contact_row.get("email", "")
    role  = contact_row.get("role", "")
    state = school_info.get("state", "")

    # One NS contact per (school, email). Co-op coaches get a distinct
    # contact record at each school they serve, so every school's Contacts
    # tab shows them (NS REST doesn't allow sharing via contactRoles).
    ext_id = make_contact_external_id(email, school_name)
    contact_id, is_inactive, found_via = find_contact_any_format(
        school_name, email, role, customer_id=customer_id)

    body_create = {
        "externalId": ext_id,
        "firstName":  first,
        "lastName":   last,
        "email":      email,
        "title":      role,
        "company":    {"id": customer_id},
        "comments":   f"{state} | Auto-synced by School Sync",
    }

    if contact_id and is_inactive:
        body_update = dict(body_create)
        body_update["isInactive"] = False
        r = ns_patch(f"contact/{contact_id}", body_update)
        if r.status_code == 204:
            print(f"  [NS] Reactivated Contact: {first} {last} (ID: {contact_id})")
        return contact_id

    elif contact_id:
        # Per-school contact record: always set company to this school's customer.
        body_update = dict(body_create)
        r = ns_patch(f"contact/{contact_id}", body_update)
        if r.status_code == 204:
            if found_via == "school_email":
                print(f"  [NS] Updated Contact: {first} {last} (ID: {contact_id})")
            else:
                print(f"  [NS] Updated Contact (migrated from {found_via}): {first} {last} (ID: {contact_id})")
        return contact_id

    else:
        # Create fresh — include company as the primary link.
        r = ns_post("contact", body_create)
        body = body_create  # kept for the error-path recovery block below
        if r.status_code == 204:
            new_id = extract_id_from_location(r)
            print(f"  [NS] Created Contact: {first} {last} - {role} (ID: {new_id})")
            return new_id
        elif r.status_code == 400 and "already exists" in r.text:
            # Contact exists but external ID mismatch — find by customer contact list
            found_id = _find_contact_for_customer(customer_id, email)
            if found_id:
                r2 = ns_patch(f"contact/{found_id}", body)
                if r2.status_code == 204:
                    print(f"  [NS] Updated Contact (recovered): {first} {last} (ID: {found_id})")
                return found_id
            print(f"  [NS] WARN: contact {first} {last} exists but could not find ID")
            return None
        else:
            print(f"  [NS] ERROR creating contact {first} {last}: "
                  f"{r.status_code} {r.text[:200]}")
            return None

def inactivate_contact(contact_id, name):
    """Soft-delete a contact and note that their Ship-To should be removed."""
    r = ns_patch(f"contact/{contact_id}", {"isInactive": True})
    if r.status_code == 204:
        print(f"  [NS] Inactivated Contact: {name} (ID: {contact_id})")
    else:
        print(f"  [NS] ERROR inactivating {contact_id}: {r.status_code} {r.text[:100]}")

def remove_contact_ship_to(customer_id, contact_name):
    """
    Remove a specific contact's Ship-To address from the Customer addressbook.

    NetSuite REST API PATCH always ADDS to addressBook — it cannot delete entries.
    So we find the matching address line and PATCH it individually to mark it as
    removed (changing the label so it won't match future contacts).
    """
    target = contact_name.strip().lower()
    r = ns_get(f"customer/{customer_id}?expand=addressBook")
    if r.status_code != 200:
        return

    items = r.json().get("addressBook", {}).get("items", [])
    for item in items:
        href = item.get("links", [{}])[0].get("href", "")
        line_id = href.rstrip("/").split("/")[-1] if href else None
        if line_id:
            r2 = ns_get(f"customer/{customer_id}/addressBook/{line_id}")
            if r2.status_code == 200:
                lbl = r2.json().get("label", "").strip()
                if lbl.lower() == target:
                    r3 = ns_patch(f"customer/{customer_id}/addressBook/{line_id}", {
                        "label": f"(Removed) {contact_name}",
                        "defaultShipping": False,
                        "defaultBilling": False,
                    })
                    if r3.status_code == 204:
                        print(f"  [NS] Cleared Ship-To for: {contact_name}")
                    return

# ============================================================
# MAIN SYNC FUNCTION
# ============================================================
def sync_school(school_name, school_url, state, sync_contacts, sales_rep=None,
                ns_customer_id=None):
    """
    Full sync for one school:
    - Scrapes WIAA page
    - Creates/updates Customer with all fields + address + per-contact Ship-Tos
    - Creates/updates Contacts where sync=True
    - Returns (ns_customer_id, school_info, all_contacts_found, created_bool)

    ns_customer_id: NS Internal ID from the Excel NS Customer ID column.
                    If provided, used directly for PATCH (no name matching).
                    If blank, a new Customer is created.
    sync_contacts:  list of dicts with keys first, last, email, role, type, sync(bool)
    """
    print(f"\n[SYNC] {school_name}" + (f" (NS ID: {ns_customer_id})" if ns_customer_id else " [NEW — will create]"))

    # Scrape
    school_info, scraped_admins, scraped_coaches = scrape_wiaa_school_detail(school_url)

    # Only pass contacts marked for sync to the address builder
    contacts_to_sync = [c for c in sync_contacts if c.get("sync")]

    # Sync Customer — pass through the known ID (or None to trigger creation)
    ns_id, created = sync_customer(school_name, state, school_info,
                                   contacts_to_sync, ns_customer_id=ns_customer_id,
                                   sales_rep=sales_rep)
    if not ns_id:
        return None, school_info, scraped_admins + scraped_coaches, False

    # Sync Contacts
    for c in contacts_to_sync:
        sync_contact(ns_id, school_name, c, school_info)
        time.sleep(0.3)

    all_found = scraped_admins + scraped_coaches
    return ns_id, school_info, all_found, created


# ============================================================
# PARENT RECORD SYNC
# ============================================================
def sync_parent_record(parent_id, school_info, contacts_to_sync, school_name=""):
    """
    Update a Parent (District) customer record:
    - ONLY updates the addressbook (per-contact Ship-Tos)
    - ONLY syncs the contacts linked to the parent record
    - Does NOT overwrite companyName, phone, url, or custom fields on the district
    """
    print(f"  [PARENT] Updating parent record (ID: {parent_id}) - contacts & addresses only")

    addr_items = build_address_items(school_info, contacts_to_sync, school_name=school_name)
    if addr_items:
        r = ns_patch(f"customer/{parent_id}", {"addressBook": {"items": addr_items}})
        if r.status_code == 204:
            print(f"  [PARENT] Updated addresses on parent (ID: {parent_id})")
        else:
            print(f"  [PARENT] ERROR updating addresses: {r.status_code} {r.text[:200]}")

    for c in contacts_to_sync:
        sync_contact(parent_id, f"PARENT-{parent_id}", c, school_info)
        time.sleep(0.3)


# ============================================================
# DIFF-BASED SYNC (used by Andy-WIAA Script.py)
# ============================================================
def sync_changes_to_netsuite(added_rows, removed_rows, columns):
    """
    Push diffs to NetSuite.
    added_rows:   list of tuples (one per new contact found)
    removed_rows: list of tuples (one per contact that disappeared)
    columns:      list of column names matching the tuple positions
    """
    col_map = {name: idx for idx, name in enumerate(columns)}

    def _get(row, key, default=""):
        idx = col_map.get(key, -1)
        return str(row[idx]).strip() if idx >= 0 and idx < len(row) else default

    # Process additions
    for row in added_rows:
        school = _get(row, "School")
        first  = _get(row, "First Name")
        last   = _get(row, "Last Name")
        email  = _get(row, "Email")
        role   = _get(row, "Role")
        state  = _get(row, "State", "WI")

        if not email or not school:
            continue

        ext_id = slugify(school)
        customer_id = get_customer_by_external_id(ext_id)
        if not customer_id:
            print(f"  [SKIP] No NS customer for '{school}' (ext: {ext_id})")
            continue

        contact_row = {"first": first, "last": last, "email": email, "role": role}
        school_info = {"state": state}
        sync_contact(customer_id, school, contact_row, school_info)
        time.sleep(0.3)

    # Process removals
    for row in removed_rows:
        school = _get(row, "School")
        first  = _get(row, "First Name")
        last   = _get(row, "Last Name")
        email  = _get(row, "Email")
        role   = _get(row, "Role")

        if not email or not school:
            continue

        ext_id = slugify(school)
        customer_id = get_customer_by_external_id(ext_id)
        if not customer_id:
            continue

        contact_id, is_inactive, _ = find_contact_any_format(school, email, role)
        if contact_id and not is_inactive:
            inactivate_contact(contact_id, f"{first} {last}")
            remove_contact_ship_to(customer_id, f"{first} {last}")
            time.sleep(0.3)


# ============================================================
# BACKWARD-COMPAT ALIASES (used by debug scripts)
# ============================================================
_h = lambda method, url: {
    "Authorization": make_auth(method, url),
    "Content-Type": "application/json",
}
scrape_wiaa_school = scrape_wiaa_school_detail
