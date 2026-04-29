"""
outlook_contacts_sync.py
------------------------
Daily sync: reads the Contacts tab from the Google master sheet and
mirrors it into Andy's Outlook contacts (andy@bsgsports.com) via Microsoft
Graph.

Folder structure: nested. Top folder per State, sub-folder per
normalized Role (sport / admin title):

    WI/
        Athletic Director/
        Boys Football/
        Boys Basketball/
        ...
    IL/
        Boys Baseball/
        ...

Each PERSON appears ONCE (deduped globally by email). They live in their
primary state's hierarchy under their primary sub-folder:
  - state is alphabetically-first if multiple
  - sub-folder is Athletic Director / Activities Director if applicable,
    else the most-common sport (alphabetical tiebreak)

Optional rep filter: set OUTLOOK_ONLY_REP env var (or hardcode in ONLY_REP)
to mirror only contacts at schools assigned to that Sales Rep. Default is
"Andrew Murray".

The contact's Categories list every sport / admin role they have, so a
cross-rep filter ("show me all wrestlers") still works via Outlook's
category filter.

Match key: email. Adds new, updates changed, moves to correct folder,
deletes departed and any duplicate copies left over from prior runs.

Auth: MSAL public-client refresh-token flow. Token cache lives in the
OUTLOOK_TOKEN_CACHE env var (whole JSON pasted in as a GitHub Secret).
"""

import os
import sys
import json
import time
import re
from collections import Counter, defaultdict
from datetime import datetime
from typing import Dict, List, Tuple, Set

import requests
import gspread
from google.oauth2.service_account import Credentials
from msal import PublicClientApplication, SerializableTokenCache

# -- Config ------------------------------------------------------------------
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "")
GOOGLE_SCOPES   = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

CLIENT_ID  = os.environ.get("OUTLOOK_CLIENT_ID", "15c37b48-585f-437b-8da6-7301d993399e")
TENANT_ID  = os.environ.get("OUTLOOK_TENANT_ID", "72bd9a57-7017-4871-88c0-2ea274e11fd9")
AUTHORITY  = f"https://login.microsoftonline.com/{TENANT_ID}"
GRAPH_SCOPES = ["Contacts.ReadWrite", "User.Read"]
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# Sheet column names (must match school_netsuite_sync.py)
C_SCHOOL = "School Name"
C_FIRST  = "First"
C_LAST   = "Last"
C_EMAIL  = "Email"
C_ROLE   = "Role"
C_TYPE   = "Type"
C_SYNC   = "Sync"

S_NAME  = "School Name"
S_SALES = "Sales Rep"
S_STATE = "State"

# Tag we put on Categories so we only touch contacts WE created
SYNC_TAG = "WIAA-Sync"

# Only sync rows where the school's Sales Rep equals this exact value.
# Empty string = no filter (sync everyone in the sheet).
# Override with env var OUTLOOK_ONLY_REP if you want to flip filters later.
ONLY_REP = os.environ.get("OUTLOOK_ONLY_REP", "Andrew Murray")

# -- Role filter ------------------------------------------------------------
# A role is allowed if it contains any ALLOW_PATTERNS substring AND no
# BLOCK_PATTERNS substring. Block always wins.
ALLOW_PATTERNS = [
    "Athletic Director", "Activities Director",
    "Baseball", "Basketball", "Football", "Softball", "Volleyball",
    "Soccer", "Cross Country", "Track and Field", "Track & Field",
    "Wrestling", "Tennis", "Golf", "Hockey", "Swimming", "Gymnastics",
    "Lacrosse",
]

BLOCK_PATTERNS = [
    "Band", "Bowling", "Bass Fishing", "Cheer", "Chess", "Dance",
    "Esports", "Flag Football", "Field Hockey",
    "Assistant", "Asst", "Admin Assistant", "Supervisor", "Trainer",
    "Principal", "Superintendent", "IHSA Official",
]


def is_role_allowed(role: str) -> bool:
    if not role or not role.strip():
        return False
    rl = role.lower()
    for blk in BLOCK_PATTERNS:
        if blk.lower() in rl:
            return False
    for allow in ALLOW_PATTERNS:
        if allow.lower() in rl:
            return True
    return False


# -- Folder name normalization ---------------------------------------------
# Strip coach-level suffixes so "Boys Baseball Head Coach" and "Boys Baseball"
# collapse into the same folder. Type column already preserves the level.
_NORMALIZE_SUFFIXES = [
    " Head Coach", " Assistant Coach", " Coach",
    "'s Assistant", "'s Head Coach",
]
# Spelling/synonym normalization to consolidate equivalent roles.
_NORMALIZE_REPLACEMENTS = [
    ("Track and Field", "Track & Field"),
]

def normalize_role_for_folder(role: str) -> str:
    name = role.strip()
    for s in _NORMALIZE_SUFFIXES:
        if name.endswith(s):
            name = name[: -len(s)].strip()
    for src, dst in _NORMALIZE_REPLACEMENTS:
        name = name.replace(src, dst)
    # Consolidate Swimming variants
    if "Swimming" in name and "Diving" not in name:
        name = name.replace("Swimming", "Swimming & Diving")
    return name

# -- Google Sheets helpers ---------------------------------------------------
def get_gspread_client():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if creds_json:
        info = json.loads(creds_json)
        creds = Credentials.from_service_account_info(info, scopes=GOOGLE_SCOPES)
    else:
        creds_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "credentials.json")
        creds = Credentials.from_service_account_file(creds_file, scopes=GOOGLE_SCOPES)
    return gspread.authorize(creds)


def load_contacts_and_schools(gc):
    wb = gc.open_by_key(GOOGLE_SHEET_ID)
    contacts_ws = wb.worksheet("Contacts")
    schools_ws  = wb.worksheet("Schools")
    contacts = contacts_ws.get_all_records()
    schools  = schools_ws.get_all_records()

    # School Name -> Sales Rep + State maps
    rep_by_school: Dict[str, str] = {}
    state_by_school: Dict[str, str] = {}
    for s in schools:
        name = str(s.get(S_NAME, "")).strip()
        rep  = str(s.get(S_SALES, "")).strip()
        state = str(s.get(S_STATE, "")).strip().upper()
        if name:
            rep_by_school[name.lower()] = rep
            state_by_school[name.lower()] = state

    return contacts, rep_by_school, state_by_school


# -- Auth --------------------------------------------------------------------
def get_access_token() -> str:
    """Load token cache from env, refresh, return access token."""
    cache_json = os.environ.get("OUTLOOK_TOKEN_CACHE", "")
    if not cache_json:
        # Fallback: read from local file (for local debugging)
        local = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             "outlook_token_cache.json")
        if os.path.exists(local):
            cache_json = open(local).read()
        else:
            raise RuntimeError(
                "No OUTLOOK_TOKEN_CACHE env var set and no local "
                "outlook_token_cache.json found. Run outlook_auth_setup.py first."
            )

    cache = SerializableTokenCache()
    cache.deserialize(cache_json)

    app = PublicClientApplication(CLIENT_ID, authority=AUTHORITY, token_cache=cache)
    accounts = app.get_accounts()
    if not accounts:
        raise RuntimeError("No accounts in token cache. Re-run outlook_auth_setup.py.")

    result = app.acquire_token_silent(GRAPH_SCOPES, account=accounts[0])
    if not result or "access_token" not in result:
        raise RuntimeError(
            "Silent token refresh failed. Re-run outlook_auth_setup.py to "
            "re-auth and update the OUTLOOK_TOKEN_CACHE secret."
        )
    return result["access_token"]


# -- Graph API helpers -------------------------------------------------------
class Graph:
    def __init__(self, token: str):
        self.s = requests.Session()
        self.s.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
            "Accept":        "application/json",
        })

    def _req(self, method: str, url: str, **kw):
        if not url.startswith("http"):
            url = GRAPH_BASE + url
        for attempt in range(3):
            r = self.s.request(method, url, timeout=30, **kw)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", "5"))
                print(f"  [GRAPH] 429 throttled, sleeping {wait}s")
                time.sleep(wait)
                continue
            if r.status_code >= 500 and attempt < 2:
                time.sleep(2 ** attempt)
                continue
            return r
        return r

    def get_all(self, url: str) -> List[dict]:
        """GET with @odata.nextLink pagination."""
        out = []
        next_url = url
        while next_url:
            r = self._req("GET", next_url)
            r.raise_for_status()
            data = r.json()
            out.extend(data.get("value", []))
            next_url = data.get("@odata.nextLink")
        return out

    def post(self, url: str, body: dict) -> dict:
        r = self._req("POST", url, data=json.dumps(body))
        if r.status_code not in (200, 201):
            raise RuntimeError(f"POST {url} -> {r.status_code}: {r.text[:300]}")
        return r.json()

    def patch(self, url: str, body: dict) -> dict:
        r = self._req("PATCH", url, data=json.dumps(body))
        if r.status_code not in (200, 204):
            raise RuntimeError(f"PATCH {url} -> {r.status_code}: {r.text[:300]}")
        return r.json() if r.text else {}

    def delete(self, url: str):
        r = self._req("DELETE", url)
        if r.status_code not in (200, 204):
            raise RuntimeError(f"DELETE {url} -> {r.status_code}: {r.text[:300]}")


# -- Folder management -------------------------------------------------------
def safe_folder_name(role: str) -> str:
    """Clean a Role string for use as an Outlook folder display name."""
    name = re.sub(r"\s+", " ", role).strip()
    # Outlook folder names can't contain certain chars in some clients
    name = name.replace("/", "-").replace("\\", "-")
    return name[:64] or "Other"


def ensure_top_folder(g: Graph, name: str,
                      cache: Dict[str, str]) -> str:
    """Ensure a top-level contact folder exists and return its id."""
    fname = safe_folder_name(name)
    if fname in cache:
        return cache[fname]
    print(f"  [REP-FOLDER] creating: {fname}")
    r = g.post("/me/contactFolders", {"displayName": fname})
    cache[fname] = r["id"]
    return r["id"]


def ensure_sub_folder(g: Graph, parent_id: str, name: str,
                      cache: Dict[Tuple[str, str], str]) -> str:
    """Ensure a child contact folder exists under parent_id, return its id."""
    fname = safe_folder_name(name)
    key = (parent_id, fname)
    if key in cache:
        return cache[key]
    # Refresh children list for this parent (in case other code created some)
    children = g.get_all(f"/me/contactFolders/{parent_id}/childFolders")
    for ch in children:
        cache[(parent_id, ch["displayName"])] = ch["id"]
    if key in cache:
        return cache[key]
    print(f"  [SUB-FOLDER] creating: .../{fname}")
    r = g.post(f"/me/contactFolders/{parent_id}/childFolders",
               {"displayName": fname})
    cache[key] = r["id"]
    return r["id"]


# -- Primary-folder selection -----------------------------------------------
# When a person has multiple roles, decide which sub-folder owns the canonical
# contact. Priority: Athletic Director > Activities Director > most-common
# sport (alphabetical tiebreak).
_ADMIN_PRIORITY = ["Athletic Director", "Activities Director"]


def pick_primary_subfolder(roles_normalized: List[str]) -> str:
    if not roles_normalized:
        return "Other"
    for prio in _ADMIN_PRIORITY:
        matches = sorted({r for r in roles_normalized if prio in r})
        if matches:
            return matches[0]
    counter = Counter(roles_normalized)
    most_common_count = max(counter.values())
    candidates = sorted([r for r, c in counter.items() if c == most_common_count])
    return candidates[0]


def pick_primary_rep(reps: List[str]) -> str:
    """If a person spans multiple reps, pick alphabetically first non-empty."""
    cleaned = sorted({r for r in reps if r})
    return cleaned[0] if cleaned else "Unassigned"


def pick_primary_state(states: List[str]) -> str:
    """If a person spans multiple states, pick alphabetically first non-empty."""
    cleaned = sorted({s for s in states if s})
    return cleaned[0] if cleaned else "Unknown"


# -- Recursive folder walking -----------------------------------------------
def list_all_folders_recursive(g: Graph) -> List[dict]:
    """Return list of all contact folders (top + nested children).
    Each item gets extra 'fullPath' / 'parentDisplayName' / 'parentFolderId'
    keys so the caller knows whether it's a top-level or child folder
    (and how to address it for DELETE)."""
    out = []
    top = g.get_all("/me/contactFolders")
    for t in top:
        t["fullPath"] = t["displayName"]
        # Top-level folders have no parent we care about for deletion
        out.append(t)
        children = g.get_all(f"/me/contactFolders/{t['id']}/childFolders")
        for c in children:
            c["fullPath"] = f"{t['displayName']}/{c['displayName']}"
            c["parentDisplayName"] = t["displayName"]
            c["parentFolderId"] = t["id"]
            out.append(c)
    return out


def delete_contact_folder(g: Graph, fld: dict) -> None:
    """Delete a contact folder using the correct endpoint based on whether
    it's a top-level folder or a child folder. Microsoft Graph returns
    403 ErrorCannotDeleteObject when you try to delete a child folder via
    /me/contactFolders/{id} -- you must go through the parent.
    """
    fid = fld["id"]
    parent_id = fld.get("parentFolderId")
    if parent_id:
        g.delete(f"/me/contactFolders/{parent_id}/childFolders/{fid}")
    else:
        g.delete(f"/me/contactFolders/{fid}")


def cleanup_master_categories(g: Graph,
                              keep_categories: Set[str],
                              known_rep_names: Set[str],
                              known_role_names: Set[str]) -> int:
    """Delete ghost master-categories that match our naming patterns but
    are no longer in `keep_categories`. Only deletes categories that
    look like ours (role name with optional "(Rep Name)" suffix, or a
    bare rep name) so we don't trash unrelated user categories.

    Returns count of deleted categories.
    """
    deleted = 0
    keep_lower = {c.lower() for c in keep_categories}
    rep_lower = {r.lower() for r in known_rep_names if r}
    role_lower = {r.lower() for r in known_role_names if r}

    rep_suffix_re = re.compile(r"^(.+?)\s*\(([^()]+)\)\s*$")

    cats = g.get_all("/me/outlook/masterCategories")
    print(f"  [CATEGORIES] {len(cats)} master categories on the account")
    for cat in cats:
        name = cat.get("displayName", "")
        cid = cat.get("id")
        if not name or not cid:
            continue
        if name.lower() in keep_lower:
            continue

        looks_like_ours = False
        # Pattern: bare rep name (e.g. "Andrew Murray")
        if name.lower() in rep_lower:
            looks_like_ours = True
        else:
            # Pattern: "Role (Rep Name)" e.g. "Athletic Director (Jeff Howard)"
            m = rep_suffix_re.match(name)
            if m:
                base, suffix = m.group(1).strip(), m.group(2).strip()
                if (suffix.lower() in rep_lower
                        and base.lower() in role_lower):
                    looks_like_ours = True

        if not looks_like_ours:
            continue

        try:
            g.delete(f"/me/outlook/masterCategories/{cid}")
            deleted += 1
            print(f"  [CATEGORY] removed ghost: {name}")
        except Exception as e:
            print(f"    ERROR delete category {name}: {e}")
    return deleted


def rename_contact_folder(g: Graph, fld: dict, new_name: str) -> None:
    """PATCH a folder's displayName. Used as a fallback when Graph refuses
    to actually delete the folder."""
    fid = fld["id"]
    parent_id = fld.get("parentFolderId")
    if parent_id:
        g.patch(f"/me/contactFolders/{parent_id}/childFolders/{fid}",
                {"displayName": new_name})
    else:
        g.patch(f"/me/contactFolders/{fid}",
                {"displayName": new_name})


# Prefix used to mark folders that should have been deleted but Graph
# refused (ErrorCannotDeleteObject). User can find + bulk-delete these
# via Outlook desktop.
ARCHIVED_PREFIX = "ZZZ_OLD_"



# -- Contact aggregation & payload ------------------------------------------
def aggregate_people(syncable_rows: List[dict],
                     rep_by_school: Dict[str, str],
                     state_by_school: Dict[str, str]) -> Dict[str, dict]:
    """Group sheet rows by email and return per-person state dict.

    Each value contains: first, last, email, schools (set), reps (set),
    states (set), raw_roles (list), normalized_roles (list of folder names),
    types (set), assignments (list of (type, normalized_role, school)
    tuples preserving pairing for human-readable jobTitle formatting).
    """
    by_email: Dict[str, dict] = {}
    for row in syncable_rows:
        email = str(row.get(C_EMAIL, "")).strip().lower()
        if not email:
            continue
        school = str(row.get(C_SCHOOL, "")).strip()
        rep = rep_by_school.get(school.lower(), "")
        state = state_by_school.get(school.lower(), "")
        role = str(row.get(C_ROLE, "")).strip()
        ctype = str(row.get(C_TYPE, "")).strip()
        first = str(row.get(C_FIRST, "")).strip()
        last  = str(row.get(C_LAST, "")).strip()
        norm_role = normalize_role_for_folder(role) if role else ""

        person = by_email.setdefault(email, {
            "email":   email,
            "first":   first,
            "last":    last,
            "schools": set(),
            "reps":    set(),
            "states":  set(),
            "raw_roles": [],
            "normalized_roles": [],
            "types":   set(),
            "assignments": [],
        })
        # Prefer a non-empty first/last seen first
        if first and not person["first"]:
            person["first"] = first
        if last and not person["last"]:
            person["last"] = last
        if school:
            person["schools"].add(school)
        if rep:
            person["reps"].add(rep)
        if state:
            person["states"].add(state)
        if role:
            person["raw_roles"].append(role)
            person["normalized_roles"].append(norm_role)
        if ctype:
            person["types"].add(ctype)
        if ctype or norm_role:
            person["assignments"].append((ctype, norm_role, school))
    return by_email


def format_job_title(assignments: List[Tuple[str, str, str]]) -> str:
    """Build a readable jobTitle from (type, normalized_role, school) tuples.

    Examples:
      [(Admin, Athletic Director, ...)]                      -> "Athletic Director"
      [(Head Coach, Boys Football, ...),
       (Assistant Coach, Boys Basketball, ...)]              -> "Head Coach - Boys Football; Assistant Coach - Boys Basketball"
      [(Head Coach, Boys Football, ...),
       (Head Coach, Boys Football, ...)]                     -> "Head Coach - Boys Football"  (deduped)
    """
    seen = set()
    pieces: List[str] = []
    for ctype, role, _school in assignments:
        key = (ctype.lower().strip(), role.lower().strip())
        if key in seen:
            continue
        seen.add(key)
        if ctype and role and ctype.lower() not in ("admin",):
            pieces.append(f"{ctype} - {role}")
        elif role:
            pieces.append(role)
        elif ctype:
            pieces.append(ctype)
    return "; ".join(pieces)[:255]


def build_contact_payload(person: dict) -> dict:
    """Single Outlook contact payload aggregating all of a person's rows."""
    first = person["first"]
    last  = person["last"]
    email = person["email"]
    schools = sorted(person["schools"])
    types = sorted(person["types"])
    norm_roles = sorted(set(person["normalized_roles"]))
    assignments = person.get("assignments", [])
    person_name = f"{first} {last}".strip() or email
    # primary_school = the school they hold their primary role at, falling
    # back to first alphabetically. Helps multi-school people show their
    # most-relevant school in the Company field.
    primary_school = ""
    if assignments:
        # Prefer school of an Athletic/Activities Director assignment
        for prio in _ADMIN_PRIORITY:
            for _t, r, s in assignments:
                if prio in r and s:
                    primary_school = s
                    break
            if primary_school:
                break
    if not primary_school:
        primary_school = schools[0] if schools else ""

    primary_rep = pick_primary_rep(list(person["reps"]))
    job_title = format_job_title(assignments)

    # displayName: "School (Coach Name, Title, Sport)"
    # so the list view in each rep/sport folder reads like an at-a-glance
    # roster. Falls back to plain "First Last" when school data is missing.
    primary_sub = pick_primary_subfolder(person["normalized_roles"]) \
        if norm_roles else ""
    primary_type = ""
    for ctype, role, _s in assignments:
        if role == primary_sub:
            primary_type = ctype
            break
    inner_parts = [person_name]
    if primary_type and primary_type.strip().lower() != "admin":
        inner_parts.append(primary_type)
    if primary_sub:
        inner_parts.append(primary_sub)
    if primary_school:
        display = f"{primary_school} ({', '.join(inner_parts)})"
    else:
        display = person_name

    # Categories: SYNC_TAG + types + every normalized role + school(s)
    # School in categories so users can filter by school across folders.
    cats = [SYNC_TAG] + types + norm_roles + schools
    cats = list(dict.fromkeys([c for c in cats if c]))

    return {
        "givenName":   first,
        "surname":     last,
        "displayName": display,
        "companyName": primary_school,
        "jobTitle":    job_title,
        "department":  primary_rep,
        # Stuff full assignment list + every school into the body so it
        # always gets saved with the contact even if the title is truncated.
        "personalNotes": _build_notes(assignments, schools, primary_rep),
        "emailAddresses": [{
            "address": email,
            "name":    display,
        }] if email else [],
        "categories":  cats,
    }


def _build_notes(assignments: List[Tuple[str, str, str]],
                 schools: List[str], rep: str) -> str:
    """Plain-text notes block with full role detail and all schools."""
    lines = []
    if rep:
        lines.append(f"Sales Rep: {rep}")
    if schools:
        if len(schools) == 1:
            lines.append(f"School: {schools[0]}")
        else:
            lines.append("Schools:")
            for s in schools:
                lines.append(f"  - {s}")
    if assignments:
        # Group by school
        by_school: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
        for ctype, role, school in assignments:
            key = (ctype, role)
            if key not in by_school[school]:
                by_school[school].append(key)
        lines.append("")
        lines.append("Roles:")
        for school in sorted(by_school.keys()):
            if school:
                lines.append(f"  {school}:")
                indent = "    "
            else:
                indent = "  "
            seen = set()
            for ctype, role in by_school[school]:
                key = (ctype.lower().strip(), role.lower().strip())
                if key in seen:
                    continue
                seen.add(key)
                if ctype and role:
                    lines.append(f"{indent}- {ctype} - {role}")
                elif role:
                    lines.append(f"{indent}- {role}")
                elif ctype:
                    lines.append(f"{indent}- {ctype}")
    return "\n".join(lines)


def contact_email(c: dict) -> str:
    addrs = c.get("emailAddresses") or []
    if not addrs:
        return ""
    return (addrs[0].get("address") or "").strip().lower()


def contact_needs_update(existing: dict, desired: dict) -> bool:
    """Compare the fields we care about."""
    fields = ["givenName", "surname", "displayName", "companyName",
              "jobTitle", "department", "personalNotes"]
    for f in fields:
        if (existing.get(f) or "") != (desired.get(f) or ""):
            return True
    # Email
    if contact_email(existing) != contact_email(desired):
        return True
    # Categories (compare as sets)
    if set(existing.get("categories") or []) != set(desired.get("categories") or []):
        return True
    return False


# -- Main sync ---------------------------------------------------------------
def main():
    print("=" * 60)
    print(f"  Outlook Contacts Sync  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    if not GOOGLE_SHEET_ID:
        print("ERROR: GOOGLE_SHEET_ID env var not set.")
        sys.exit(1)

    # 1. Load source data
    print("\n[1/5] Loading Google Sheet...")
    gc = get_gspread_client()
    contacts_data, rep_by_school, state_by_school = load_contacts_and_schools(gc)

    if ONLY_REP:
        print(f"  REP FILTER: only syncing schools assigned to {ONLY_REP}")

    # Filter to syncable rows with email AND allowed role AND (optionally)
    # the configured Sales Rep. Also collect EVERY role string seen (even
    # blocked ones) so we know which folder names "look like ours" --
    # used by empty-folder cleanup.
    syncable = []
    skipped_role = 0
    skipped_rep = 0
    sheet_role_names: set = set()
    for row in contacts_data:
        role = str(row.get(C_ROLE, "")).strip()
        if role:
            sheet_role_names.add(role)
            sheet_role_names.add(normalize_role_for_folder(role))
        if str(row.get(C_SYNC, "N")).strip().upper() != "Y":
            continue
        if not str(row.get(C_EMAIL, "")).strip():
            continue
        if not role:
            continue
        if not is_role_allowed(role):
            skipped_role += 1
            continue
        if ONLY_REP:
            school = str(row.get(C_SCHOOL, "")).strip().lower()
            row_rep = rep_by_school.get(school, "")
            if row_rep != ONLY_REP:
                skipped_rep += 1
                continue
        syncable.append(row)
    print(f"  {len(syncable)} syncable contacts "
          f"({skipped_role} skipped by role filter, "
          f"{skipped_rep} skipped by rep filter)")
    sheet_role_names_lower = {n.lower() for n in sheet_role_names if n}

    # 2. Auth + Graph client
    print("\n[2/5] Authenticating to Microsoft Graph...")
    token = get_access_token()
    g = Graph(token)

    # 3. Aggregate by person (one Outlook contact per email)
    print("\n[3/6] Aggregating by person...")
    people = aggregate_people(syncable, rep_by_school, state_by_school)
    print(f"  {len(people)} unique people")

    # 4. Decide each person's primary state + primary sub-folder
    #    (since we filter to a single rep, the rep is no longer the
    #    natural top-level grouping; we use State instead.)
    print("\n[4/6] Picking primary state + sub-folder per person...")
    # desired[(state, sub)] -> {email -> payload}
    desired: Dict[Tuple[str, str], Dict[str, dict]] = defaultdict(dict)
    for email, person in people.items():
        primary_state = pick_primary_state(list(person["states"]))
        primary_sub = pick_primary_subfolder(person["normalized_roles"])
        # Lock the rep on the payload to the configured ONLY_REP (or pick
        # alphabetically when no filter). Used for the Department field.
        primary_rep = (ONLY_REP
                       if ONLY_REP and ONLY_REP in person["reps"]
                       else pick_primary_rep(list(person["reps"])))
        person["reps"] = {primary_rep}
        payload = build_contact_payload(person)
        desired[(primary_state, primary_sub)][email] = payload
    state_count = len({state for state, _ in desired.keys()})
    sub_count = len({sub for _, sub in desired.keys()})
    print(f"  {state_count} states, {sub_count} distinct sub-folders, "
          f"{sum(len(v) for v in desired.values())} contacts")

    # 5. Ensure folder hierarchy exists  (State / Sport)
    print("\n[5/6] Ensuring state + sub-folders...")
    top_cache: Dict[str, str] = {}  # state_name -> top_folder_id
    sub_cache: Dict[Tuple[str, str], str] = {}  # (top_id, sub_name) -> sub_id

    # Pre-load existing top folders into cache once
    for f in g.get_all("/me/contactFolders"):
        top_cache[f["displayName"]] = f["id"]

    folder_ids: Dict[Tuple[str, str], str] = {}
    for (state, sub) in sorted(desired.keys()):
        top_id = ensure_top_folder(g, state, top_cache)
        # No rep suffix on sub-folders -- there's only one rep, so it'd
        # be redundant.
        sub_id = ensure_sub_folder(g, top_id, sub, sub_cache)
        folder_ids[(state, sub)] = sub_id

    # 6. Build a global view of every WIAA-Sync contact currently in Outlook,
    #    keyed by email. We need this to dedupe across folders and detect
    #    contacts that need to be moved to a new (rep, sub) folder.
    print("\n[6/6] Indexing existing WIAA-Sync contacts...")
    all_folders_now = list_all_folders_recursive(g)
    # email -> list of (folder_id, contact_id, contact_data, full_path)
    existing_by_email: Dict[str, List[Tuple[str, str, dict, str]]] = defaultdict(list)
    select = ("?$select=id,givenName,surname,displayName,companyName,"
              "jobTitle,department,emailAddresses,categories,parentFolderId,"
              "personalNotes")
    for fld in all_folders_now:
        contacts = g.get_all(f"/me/contactFolders/{fld['id']}/contacts" + select)
        for c in contacts:
            em = contact_email(c)
            if not em:
                continue
            cats = c.get("categories") or []
            if SYNC_TAG not in cats:
                continue
            existing_by_email[em].append(
                (fld["id"], c["id"], c, fld.get("fullPath", fld["displayName"]))
            )
    print(f"  Indexed {sum(len(v) for v in existing_by_email.values())} existing "
          f"WIAA-Sync contacts across {len(all_folders_now)} folders")

    # 7. Per-person sync: ensure each desired contact lives in correct folder
    print("\n[SYNC] Reconciling contacts...")
    totals = {"added": 0, "updated": 0, "moved": 0, "deleted": 0,
              "unchanged": 0, "skipped": 0, "dedup_deleted": 0}

    total_to_process = sum(len(v) for v in desired.values())
    processed = 0
    last_progress_pct = -1
    sync_start = time.time()

    for (state, sub), people_payloads in sorted(desired.items()):
        target_fid = folder_ids[(state, sub)]
        print(f"  [{state} / {sub}] {len(people_payloads)} people")
        for email, payload in people_payloads.items():
            processed += 1
            pct = (processed * 100) // max(total_to_process, 1)
            if pct != last_progress_pct and pct % 5 == 0:
                elapsed = time.time() - sync_start
                rate = processed / max(elapsed, 1)
                eta = (total_to_process - processed) / max(rate, 0.1)
                print(f"    [progress] {processed}/{total_to_process} "
                      f"({pct}%) -- {rate:.1f}/s -- ETA {int(eta)}s")
                last_progress_pct = pct
            existing_list = existing_by_email.get(email, [])
            if not existing_list:
                # Brand new
                try:
                    g.post(f"/me/contactFolders/{target_fid}/contacts", payload)
                    totals["added"] += 1
                except Exception as e:
                    print(f"    ERROR add {email}: {e}")
                    totals["skipped"] += 1
                continue

            # We have one or more existing copies of this email.
            # Pick one to keep (prefer one already in the right folder),
            # delete the rest as duplicates.
            keep_idx = 0
            for i, (fid, _, _, _) in enumerate(existing_list):
                if fid == target_fid:
                    keep_idx = i
                    break

            keep_fid, keep_cid, keep_data, keep_path = existing_list[keep_idx]

            # Delete duplicate copies in other folders
            for i, (_, dup_cid, _, dup_path) in enumerate(existing_list):
                if i == keep_idx:
                    continue
                try:
                    g.delete(f"/me/contacts/{dup_cid}")
                    totals["dedup_deleted"] += 1
                except Exception as e:
                    print(f"    ERROR dedup-delete {email} from {dup_path}: {e}")
                    totals["skipped"] += 1

            if keep_fid != target_fid:
                # Wrong folder -- delete and recreate in target
                try:
                    g.delete(f"/me/contacts/{keep_cid}")
                    g.post(f"/me/contactFolders/{target_fid}/contacts", payload)
                    totals["moved"] += 1
                except Exception as e:
                    print(f"    ERROR move {email}: {e}")
                    totals["skipped"] += 1
            else:
                # Right folder -- update if anything differs
                if contact_needs_update(keep_data, payload):
                    try:
                        g.patch(f"/me/contacts/{keep_cid}", payload)
                        totals["updated"] += 1
                    except Exception as e:
                        print(f"    ERROR update {email}: {e}")
                        totals["skipped"] += 1
                else:
                    totals["unchanged"] += 1

            existing_by_email.pop(email, None)  # mark handled

    # 8. Anything left in existing_by_email is a true orphan
    #    (their email is no longer in the sheet under an allowed role).
    print("\n[CLEANUP] Removing orphan WIAA-Sync contacts...")
    for email, copies in existing_by_email.items():
        for fid, cid, _, path in copies:
            try:
                g.delete(f"/me/contacts/{cid}")
                totals["deleted"] += 1
            except Exception as e:
                print(f"    ERROR delete orphan {email} from {path}: {e}")
                totals["skipped"] += 1

    # 9. Folder cleanup: remove old flat folders (sport name at top level)
    #    and any empty sub-folders. Walk top-down, then bottom-up so we
    #    delete children before parents.
    print("\n[CLEANUP] Pruning empty / leftover folders...")
    desired_top_names = {safe_folder_name(state) for state, _ in desired.keys()}
    # Sub-folders are stored under their state parent with the bare sport
    # name (no rep suffix needed when only one rep is synced).
    desired_sub_pairs = {(safe_folder_name(state), safe_folder_name(sub))
                         for state, sub in desired.keys()}
    # Set of rep names from the sheet -- used to recognise old top-level
    # rep folders as "ours" so they get cleaned up after the migration
    # from rep-based structure to state-based structure.
    old_rep_names_lower = {r.lower() for r in rep_by_school.values() if r}
    # Re-fetch since we deleted things above
    all_folders_after = list_all_folders_recursive(g)
    # Sort: children first (they have parentDisplayName), then top-level.
    children_first = sorted(all_folders_after,
                            key=lambda f: 0 if "parentDisplayName" in f else 1)
    for fld in children_first:
        fid = fld["id"]
        fname = fld["displayName"]
        parent = fld.get("parentDisplayName")  # only present for sub-folders

        if parent:
            # It's a sub-folder. Keep only if (parent, fname) is desired.
            if (parent, fname) in desired_sub_pairs:
                continue
        else:
            # It's a top-level folder. Keep only if it's a desired rep.
            if fname in desired_top_names:
                continue

        # Not desired. Delete WIAA-Sync contacts in it, and delete the folder
        # if it's empty AND looks like one we created.
        contacts_here = g.get_all(
            f"/me/contactFolders/{fid}/contacts?$select=id,categories"
        )
        deleted_here = 0
        kept_here = 0
        for c in contacts_here:
            if SYNC_TAG in (c.get("categories") or []):
                try:
                    g.delete(f"/me/contacts/{c['id']}")
                    totals["deleted"] += 1
                    deleted_here += 1
                except Exception:
                    totals["skipped"] += 1
            else:
                kept_here += 1
        # Folder deletion criteria:
        #   * Folder is empty AND
        #   * Either (a) we just emptied it of WIAA-Sync contacts, or
        #            (b) it was already empty AND its name matches a sheet
        #                role (so it's almost certainly a leftover from
        #                a prior run).
        if kept_here > 0:
            continue
        # Also try with any "(Rep Name)" suffix stripped, since the prior
        # structure named sub-folders like "Boys & Girls Golf (Andrew Murray)".
        fname_stripped = re.sub(r"\s*\([^)]+\)\s*$", "", fname).strip()
        looks_like_ours = (fname.lower() in sheet_role_names_lower
                           or fname_stripped.lower() in sheet_role_names_lower
                           or fname in desired_top_names
                           or fname.lower() in old_rep_names_lower)
        if deleted_here > 0 or looks_like_ours:
            # Skip folders we already renamed in a previous run -- they're
            # already marked for manual cleanup by the user.
            if fname.startswith(ARCHIVED_PREFIX):
                continue
            try:
                delete_contact_folder(g, fld)
                print(f"  [FOLDER] removed: "
                      f"{fld.get('fullPath', fname)}"
                      f" ({deleted_here} contacts cleared)")
            except Exception:
                # Graph refused (likely 403 ErrorCannotDeleteObject).
                # Fallback: rename with ZZZ_OLD_ prefix so the user can
                # find + bulk-delete these in Outlook desktop. Folders
                # sort alphabetically; this groups all dead folders at
                # the bottom of every rep's hierarchy.
                new_name = ARCHIVED_PREFIX + fname
                try:
                    rename_contact_folder(g, fld, new_name)
                    print(f"  [FOLDER] archived (delete denied by Graph): "
                          f"{fld.get('fullPath', fname)} -> {new_name}")
                except Exception as e2:
                    print(f"    ERROR archive folder {fname}: {e2}")

    # 10. Master-category cleanup. Categories live in a master list that
    #     persists independently of contacts -- old "(Rep Name)"-style
    #     categories from earlier runs hang around in the People view.
    print("\n[CLEANUP] Pruning ghost master-categories...")
    keep_categories: Set[str] = {SYNC_TAG}
    for people_payloads in desired.values():
        for payload in people_payloads.values():
            for c in payload.get("categories") or []:
                keep_categories.add(c)
    known_rep_names = {r for r in rep_by_school.values() if r}
    cat_deleted = cleanup_master_categories(
        g,
        keep_categories=keep_categories,
        known_rep_names=known_rep_names,
        known_role_names=sheet_role_names,
    )
    totals["categories_deleted"] = cat_deleted

    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    for k in ("added", "updated", "moved", "unchanged",
              "deleted", "dedup_deleted", "categories_deleted",
              "skipped"):
        print(f"  {k.replace('_', ' ').capitalize():20s}{totals.get(k, 0)}")
    print()


if __name__ == "__main__":
    main()
