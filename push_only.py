"""
push_only.py — push Contacts tab rows to NetSuite without scraping.

Reads the Contacts tab (already populated by the daily scrape) and
syncs each row with Sync=Y to NetSuite:
  - creates new contacts (no NS Contact ID yet) via sync_contact
  - updates existing ones (so title / email / names stay fresh)
  - inactivates rows flipped to Sync=N
  - refreshes the customer's Sales Team + Ship-To addressBook

Scope via SALES_REP_FILTER env var — one rep's schools only. Designed
for per-rep parallel invocations after the nightly scrape (one job
per rep; one rep's error doesn't block the others).

Env:
  GOOGLE_SHEET_ID, GOOGLE_CREDENTIALS_JSON, NS_* tokens
  SALES_REP_FILTER  - required for per-rep mode (blank = all reps)
  STATE_FILTER      - 'WI', 'IL', or blank for all
  SCHOOL_FILTER     - optional single-school testing
"""
from __future__ import annotations

import hashlib
import os
import re
import sys
import time
from datetime import datetime

import gspread


def row_hash(first, last, email, role):
    """Stable hash of the fields that matter to NS. Used to skip rows that
    haven't changed since the last push."""
    s = f"{first.strip().lower()}|{last.strip().lower()}|{email.strip().lower()}|{role.strip().lower()}"
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:16]


def _strip_gender(s):
    """Remove Boys/Girls/Boys & Girls qualifiers wherever they appear so
    gendered variants share a base ('Boys Athletic Director' ->
    'Athletic Director', 'Head Coach (Girls Wrestling)' ->
    'Head Coach (Wrestling)')."""
    s = re.sub(r"(?i)\bboys\s*&\s*girls\s+", "", s)
    s = re.sub(r"(?i)\bboys\s+and\s+girls\s+", "", s)
    s = re.sub(r"(?i)\bboys\s+", "", s)
    s = re.sub(r"(?i)\bgirls\s+", "", s)
    return re.sub(r"\s+", " ", s).strip()


def clean_titles(labels):
    """Collapse the redundant title variants IHSA emits into a minimal set.

    Three reductions, in order:
      1. Drop an "A & B" combined title when A and B both appear as their
         own titles ("Band Director & Marching Band Director" dropped when
         "Band Director" and "Marching Band Director" are present). Gender
         combos like "Boys & Girls Wrestling" are NOT split this way
         because their halves ("Boys", "Girls Wrestling") aren't titles.
      2. Within a gender family (same gender-stripped base): if the plain
         base exists use only it ("Athletic Director" wins over Boys/Girls
         AD); else if a "Boys & Girls X" form exists use only it
         (Kevin Milder -> Boys & Girls Wrestling); else if both Boys and
         Girls exist with no combined, synthesize "Boys & Girls X".
      3. Otherwise keep the single variant as-is (e.g. a boys-only coach).
    """
    # Pre-expand single-cell "A & B & C" titles into components so a long
    # IHSA blob like "athletic supervisor & boys athletic director's
    # assistant & girls athletic director's assistant" becomes three short
    # titles (which then dedupe/gender-collapse). Gender combos are left
    # whole: a part that is a bare "boys"/"girls" word means the "&" is
    # joining gender qualifiers, not distinct titles (e.g.
    # "Boys & Girls Wrestling").
    expanded = []
    for l in labels:
        l = (l or "").strip()
        if not l:
            continue
        # Don't split a gender combo ("Boys & Girls Wrestling") — that "&"
        # joins gender qualifiers, not distinct titles.
        is_gender_combo = re.search(r"(?i)boys\s*&\s*girls|boys\s+and\s+girls", l)
        if " & " in l and not is_gender_combo:
            expanded.extend(p.strip() for p in l.split(" & "))
            continue
        expanded.append(l)

    # de-dup, preserve first-seen order
    seen = []
    for l in expanded:
        if l and l not in seen:
            seen.append(l)
    label_set = set(seen)

    # Reduction 1: drop true "A & B" combos whose parts are standalone titles
    kept = []
    for l in seen:
        if " & " in l:
            parts = [p.strip() for p in l.split(" & ")]
            if len(parts) >= 2 and all(p in label_set for p in parts):
                continue
        kept.append(l)

    # Reduction 2/3: gender families keyed by gender-stripped base
    families = []          # list of (base, [variants]) preserving order
    index = {}
    for l in kept:
        base = _strip_gender(l)
        if base not in index:
            index[base] = len(families)
            families.append((base, []))
        families[index[base]][1].append(l)

    out = []
    for base, variants in families:
        if base in variants:
            out.append(base)
            continue
        bg = next((v for v in variants if re.search(r"(?i)boys\s*&\s*girls|boys\s+and\s+girls", v)), None)
        if bg:
            out.append(bg)
            continue
        boys = next((v for v in variants if re.search(r"(?i)\bboys\b", v)), None)
        girls = next((v for v in variants if re.search(r"(?i)\bgirls\b", v)), None)
        if boys and girls:
            # Coaches: boys & girls are different teams of one sport -> keep
            # a combined "Boys & Girls X". Admins (Athletic Director etc.):
            # the gender is meaningless for the role -> collapse to the base.
            is_coach = bool(re.search(r"(?i)coach\s*\(", boys))
            if is_coach:
                out.append(re.sub(r"(?i)\bboys\b", "Boys & Girls", boys, count=1))
            else:
                out.append(base)
        else:
            out.append(variants[0])
    return out

from netsuite_sync import (
    sync_school, sync_customer, sync_contact, inactivate_contact,
    remove_contact_ship_to, sync_address_book, compute_school_domain,
    restlet_available, ns_restlet_attach,
)
from school_netsuite_sync import (
    get_gspread_client,
    load_contacts, save_contacts,
    canonicalize_contact_school_names,
    GOOGLE_SHEET_ID, MASTER_TAB,
    M_NAME, M_URL, M_NS_ID, M_SALES, M_STATE, M_LOCKED, M_SYNCED,
    C_SCHOOL, C_FIRST, C_LAST, C_EMAIL, C_ROLE, C_TYPE,
    C_SYNC, C_NS_CID, C_NS_CUS, C_SYNCED, C_HASH,
)
# Reuse the IL row -> school_info shaper. Single source of truth so the
# manual IL workflow and this nightly push read the sheet identically.
from ihsa_sync import (
    school_info_from_row, M_FULL,
    M_CLASS, M_LEVEL, M_NICKNAME, M_COLORS, M_CONFERENCE,
    M_DISTRICT, M_ENROLLMENT, M_SIZE, M_PHONE,
    M_ADDR1, M_ADDR2, M_CITY, M_ZIP, M_WEBSITE,
)

# Optional Schools-tab column for parent/child customers (e.g. Mount Horeb:
# district parent + high-school subcustomer). Contacts' primary company and
# the customer update target the NS Customer ID (the child); when this
# column holds the parent's internal id we keep its Ship-To book current too.
M_PARENT = "NS Parent ID"

SCHOOL_FILTER    = os.environ.get("SCHOOL_FILTER", "").strip()
STATE_FILTER     = os.environ.get("STATE_FILTER", "").strip().upper()
SALES_REP_FILTER = os.environ.get("SALES_REP_FILTER", "").strip()
DELAY = 0.3  # much lower than scrape-included sync; no WIAA throttling needed


def load_schools(gc):
    wb = gc.open_by_key(GOOGLE_SHEET_ID)
    ws = wb.worksheet(MASTER_TAB)
    values = ws.get_all_values()
    if not values:
        return [], ws, None
    headers = values[0]
    synced_col = headers.index(M_SYNCED) + 1 if M_SYNCED in headers else None
    ns_id_col  = headers.index(M_NS_ID) + 1 if M_NS_ID in headers else None
    out = []
    for i, raw in enumerate(values[1:], start=2):
        rec = dict(zip(headers, raw))
        name  = str(rec.get(M_NAME, "")).strip()
        ns_id = str(rec.get(M_NS_ID, "")).strip()
        url   = str(rec.get(M_URL, "")).strip()
        state = str(rec.get(M_STATE, "")).strip().upper()
        rep   = str(rec.get(M_SALES, "")).strip()
        locked = str(rec.get(M_LOCKED, "")).strip().upper() == "Y"
        if not name or locked:
            continue
        # Blank / sentinel NS Customer ID means "no NS customer yet" — we no
        # longer skip these. They flow through to sync_school/sync_customer
        # with ns_customer_id="" which CREATES the customer, and the new ID
        # is written back to the sheet (see main()). This restores the old
        # behavior where adding a school to the list auto-created it in NS.
        if ns_id in ("nan", "None", "0"):
            ns_id = ""
        if SCHOOL_FILTER and name != SCHOOL_FILTER:
            continue
        if STATE_FILTER and state != STATE_FILTER:
            continue
        if SALES_REP_FILTER and rep.lower() != SALES_REP_FILTER.lower():
            continue
        out.append({"row": i, "name": name, "ns_id": ns_id, "url": url,
                    "state": state, "rep": rep, "raw": rec,
                    "parent": str(rec.get(M_PARENT, "")).strip()})
    return out, ws, synced_col, ns_id_col


def main():
    print("=" * 60)
    print(f"  PUSH ONLY  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    if SALES_REP_FILTER: print(f"  SALES_REP_FILTER: {SALES_REP_FILTER}")
    if STATE_FILTER:     print(f"  STATE_FILTER: {STATE_FILTER}")
    if SCHOOL_FILTER:    print(f"  SCHOOL_FILTER: {SCHOOL_FILTER}")
    print("=" * 60)

    if not GOOGLE_SHEET_ID:
        print("ERROR: GOOGLE_SHEET_ID env var not set.")
        sys.exit(1)

    gc = get_gspread_client()
    schools, master_ws, synced_col, ns_id_col = load_schools(gc)
    contacts_data, contacts_ws = load_contacts(gc)

    # Heal school renames BEFORE anything keys off School Name: a school
    # renamed on the Schools tab (or by the WIAA) orphans its Contacts-tab
    # rows under the old name, so the scraper re-adds everyone as blank new
    # rows and NetSuite gets duplicate contacts. Rename stale rows to the
    # canonical name, merge the duplicates, and persist right away so the
    # heal sticks even if the push later dies.
    _ren, _mrg, _ = canonicalize_contact_school_names(
        contacts_data, master_ws.get_all_records())
    if _ren or _mrg:
        save_contacts(contacts_ws, contacts_data)

    # Shared (co-op) people: one person actively serving 2+ schools — same
    # email or same stored NS Contact ID under multiple School Name values
    # among Sync=Y rows. These get ONE shared NS contact card attached to
    # every school (via the RESTlet) instead of per-school duplicate records.
    # Computed over the FULL Contacts tab (not just this rep's scope) so a
    # co-op pair split across two reps is still recognized. Sync=Y-only so
    # the departure branch can tell "left this school but still coaches at
    # the other" (detach here) from "left everywhere" (inactivate).
    y_schools_by_email, y_schools_by_cid = {}, {}
    for _c in contacts_data:
        if str(_c.get(C_SYNC, "N")).strip().upper() != "Y":
            continue
        _sch = str(_c.get(C_SCHOOL, "")).strip()
        if not _sch:
            continue
        _em = str(_c.get(C_EMAIL, "")).strip().lower()
        _cid = str(_c.get(C_NS_CID, "")).strip()
        if _em:
            y_schools_by_email.setdefault(_em, set()).add(_sch)
        if _cid.isdigit():
            y_schools_by_cid.setdefault(_cid, set()).add(_sch)
    shared_emails = {e for e, s in y_schools_by_email.items() if len(s) > 1}
    shared_cids   = {i for i, s in y_schools_by_cid.items() if len(s) > 1}

    # A shared card has ONE title field serving every school, so build it
    # from the person's roles across ALL their schools: roles they hold at
    # every school stay plain ("Head Coach (Boys Football)"); roles specific
    # to one school get that school appended ("Athletic Director (Horicon
    # High School)"). Every school's row then hashes the same merged title,
    # so nightly per-school runs agree instead of overwriting each other.
    def _row_label(role_txt, type_txt):
        # Admin rows: role IS the title. Coach rows: role is the sport,
        # type is "Head Coach"/"Coach" — combine to "Head Coach (Sport)".
        if type_txt and type_txt.lower() not in ("admin", ""):
            return f"{type_txt} ({role_txt})" if role_txt else type_txt
        return role_txt

    coop_label_schools = {}   # email -> {label: set(schools)}, insertion-ordered
    for _c in contacts_data:
        if str(_c.get(C_SYNC, "N")).strip().upper() != "Y":
            continue
        _em = str(_c.get(C_EMAIL, "")).strip().lower()
        if _em not in shared_emails:
            continue
        _sch = str(_c.get(C_SCHOOL, "")).strip()
        _label = _row_label(str(_c.get(C_ROLE, "")).strip(),
                            str(_c.get(C_TYPE, "")).strip())
        if _sch and _label:
            coop_label_schools.setdefault(_em, {}).setdefault(_label, set()).add(_sch)

    def coop_title(em):
        lab = coop_label_schools.get(em, {})
        if not lab:
            return ""
        all_schools = set().union(*lab.values())
        common   = [l for l, s in lab.items() if s == all_schools]
        specific = [(l, s) for l, s in lab.items() if s != all_schools]
        parts = clean_titles(common)
        for l, schools in specific:
            parts.append(f"{l} ({' & '.join(sorted(schools))})")
        return ", ".join(parts)

    print(f"  Schools in scope: {len(schools)}")
    print(f"  Contacts tab rows: {len(contacts_data)}")
    print(f"  Co-op (multi-school) people: {len(shared_emails)}"
          + ("" if restlet_available() else "  [RESTlet not configured — using per-school duplicates]")
          + "\n")

    synced_schools = 0
    errors = 0
    synced_updates = []
    created_updates = []   # (row, new_ns_id) for schools created this run

    for sch in schools:
        school_name = sch["name"]                              # Schools tab "School Name" column
        ns_id       = sch["ns_id"]
        rep         = sch["rep"]
        state       = sch["state"]
        # display_name is what NetSuite shows as companyName. Read from the
        # Full Name column; fall back to School Name if Full Name is blank.
        display_name = str(sch["raw"].get(M_FULL, "")).strip() or school_name

        print(f"\n[{school_name}]  NS {ns_id}  (rep: {rep})")

        school_contacts = [c for c in contacts_data
                           if c.get(C_SCHOOL, "").strip() == school_name]
        if not school_contacts and ns_id:
            # Existing customer with nothing on the Contacts tab to push —
            # still run sync_school below so the customer record and
            # Schools-tab fields (address, enrollment, etc.) get refreshed
            # from WIAA; only the per-contact push loop has nothing to do.
            # A school with zero matching Contacts rows despite WIAA
            # listing staff is often a sign the School Name here doesn't
            # match what's stored in the Contacts tab (e.g. after a rename)
            # — refreshing here at least keeps the Schools tab itself
            # accurate even when that join is broken.
            print(f"  (no rows on Contacts tab — refreshing customer/Schools-tab fields only)")
        if not school_contacts and not ns_id:
            # New school (blank NS ID) — still create the customer even
            # though no contacts have been added to the Contacts tab yet.
            print(f"  (no rows on Contacts tab — creating NS customer only)")

        # Update Customer. WI: sync_school scrapes WIAA + updates custom
        # fields, address book, etc. IL: IHSA API doesn't carry address
        # or school-attribute data, so we read them from the Schools tab
        # and feed sync_customer with the same shape the WIAA scraper
        # produces. Mirrors what ihsa_sync.py does for the manual workflow.
        if state == "IL":
            school_info_out = school_info_from_row(sch["raw"], "IL")
            try:
                result_id, created = sync_customer(
                    display_name, "IL", school_info_out,
                    contacts=[], ns_customer_id=ns_id, sales_rep=rep or None,
                )
            except Exception as e:
                print(f"  ERROR syncing IL customer: {e}")
                errors += 1
                time.sleep(DELAY)
                continue
            if not result_id:
                print(f"  Could not sync IL Customer — skipping contacts")
                errors += 1
                continue
        else:
            try:
                # Pass display_name (from Full Name column) — sync_school
                # propagates it through to sync_customer's companyName.
                result_id, school_info_out, _, created = sync_school(
                    school_name=display_name,
                    school_url=sch["url"],
                    state=state or "WI",
                    sync_contacts=[],
                    sales_rep=rep or None,
                    ns_customer_id=ns_id,
                )
            except Exception as e:
                print(f"  ERROR syncing customer: {e}")
                errors += 1
                time.sleep(DELAY)
                continue
            if not result_id:
                print(f"  Could not sync Customer — skipping contacts")
                errors += 1
                continue

            # Write scraped WIAA fields back to the Schools tab so it stays in
            # sync with the live source: every field here is overwritten
            # whenever it differs from the fresh WIAA scrape, even if the
            # cell already has a value. This mirrors what already happens in
            # NetSuite (build_customer_body syncs the same scraped fields to
            # the customer record every night) — the sheet should never be
            # allowed to drift from WIAA the way Adams-Friendship's address
            # did indefinitely with nothing to correct it.
            # No "county" here — WIAA's school detail page doesn't publish one.
            # IL schools are sheet-sourced so no write-back needed there.
            HEAL_FIELDS = [
                (M_ADDR1,      "address1"),
                (M_ADDR2,      "address2"),
                (M_CITY,       "city"),
                (M_STATE,      "state"),
                (M_ZIP,        "zip"),
                (M_CLASS,      "school_class"),
                (M_LEVEL,      "level"),
                (M_NICKNAME,   "nickname"),
                (M_COLORS,     "colors"),
                (M_CONFERENCE, "conference"),
                (M_DISTRICT,   "wiaa_district"),
                (M_ENROLLMENT, "enrollment"),
                (M_SIZE,       "school_size"),
                (M_PHONE,      "phone"),
                (M_WEBSITE,    "website"),
            ]
            headers = list(sch["raw"].keys())
            wiaa_updates = []
            for col_name, info_key in HEAL_FIELDS:
                scraped_val = school_info_out.get(info_key)
                if not scraped_val or col_name not in headers:
                    continue
                existing = str(sch["raw"].get(col_name, "")).strip()
                if existing.lower() == str(scraped_val).strip().lower():
                    continue  # already matches — no-op
                col_idx = headers.index(col_name) + 1
                wiaa_updates.append({
                    "range": gspread.utils.rowcol_to_a1(sch["row"], col_idx),
                    "values": [[str(scraped_val)]],
                })
            if wiaa_updates:
                master_ws.batch_update(wiaa_updates)
                print(f"  Healed {len(wiaa_updates)} field(s) on Schools tab to match live WIAA")

        synced_schools += 1
        synced_updates.append((sch["row"], datetime.now().strftime("%Y-%m-%d %H:%M")))

        # New school: write the freshly-created NS Customer ID back to the
        # Schools tab so future runs take the direct-PATCH path instead of
        # creating a duplicate.
        if created and not ns_id and result_id:
            created_updates.append((sch["row"], str(result_id)))
            ns_id = str(result_id)
            print(f"  Created — wrote NS Customer ID {result_id} to sheet row {sch['row']}")

        # Compute school domain for home-school detection
        sync_y = [
            {"email": str(c.get(C_EMAIL, "")).strip()}
            for c in school_contacts
            if str(c.get(C_SYNC, "N")).strip().upper() == "Y"
            and str(c.get(C_EMAIL, "")).strip()
        ]
        school_info_out["domain"] = compute_school_domain(sync_y)
        if school_info_out["domain"]:
            print(f"  School domain: {school_info_out['domain']}")

        # A person who holds several roles at one school (e.g. John Lalor =
        # Athletic Director AND head football coach AND head wrestling coach)
        # has multiple Sync=Y rows but should be ONE NetSuite contact with a
        # combined title — NS has a single `title` field per contact. Build
        # that combined title per email up front so every role shows in NS.
        email_title = {}   # email_lower -> "Athletic Director, Head Coach (Boys Football), ..."
        for c in school_contacts:
            if str(c.get(C_SYNC, "N")).strip().upper() != "Y":
                continue
            em = str(c.get(C_EMAIL, "")).strip().lower()
            if not em:
                continue
            role_txt = str(c.get(C_ROLE, "")).strip()
            type_txt = str(c.get(C_TYPE, "")).strip()
            # Admin rows: role IS the title. Coach rows: role is the sport,
            # type is "Head Coach"/"Coach" — combine to "Head Coach (Sport)".
            if type_txt and type_txt.lower() not in ("admin", ""):
                label = f"{type_txt} ({role_txt})" if role_txt else type_txt
            else:
                label = role_txt
            if not label:
                continue
            parts = email_title.setdefault(em, [])
            if label not in parts:               # de-dupe identical labels
                parts.append(label)

        def combined_title(em):
            return ", ".join(clean_titles(email_title.get(em, [])))

        # Dedupe: one person with multiple roles = multiple sheet rows but
        # one NS PATCH. Key by email; carry the NS Contact ID forward so we
        # don't re-PATCH the same record.
        pushed_emails = {}    # email_lower -> ns_contact_id (after sync)
        for c in school_contacts:
            sync_flag  = str(c.get(C_SYNC, "N")).strip().upper()
            first      = str(c.get(C_FIRST, "")).strip()
            last       = str(c.get(C_LAST, "")).strip()
            email      = str(c.get(C_EMAIL, "")).strip()
            role       = str(c.get(C_ROLE, "")).strip()
            contact_ns = str(c.get(C_NS_CID, "")).strip()
            if not email:
                continue
            c[C_NS_CUS] = str(result_id)
            em_key = email.lower()
            is_shared = em_key in shared_emails or contact_ns in shared_cids
            # Shared card: title merges roles across ALL the person's schools
            # (school-specific roles annotated with the school name) so every
            # school's nightly run writes the identical value. Otherwise the
            # usual this-school combined title.
            if is_shared:
                title = coop_title(em_key) or combined_title(em_key) or role
            else:
                title = combined_title(em_key) or role   # all roles, comma-joined

            if sync_flag == "Y":
                if contact_ns == "UNLINKED":
                    continue
                if em_key in pushed_emails:
                    # Same person, another role row — reuse the known NS ID
                    if pushed_emails[em_key]:
                        c[C_NS_CID] = str(pushed_emails[em_key])
                        c[C_SYNCED] = datetime.now().strftime("%Y-%m-%d %H:%M")
                        c[C_HASH]   = row_hash(first, last, email, title)
                    continue

                # Change detection keyed on the COMBINED title so picking up
                # or dropping a role re-pushes the contact.
                current_hash = row_hash(first, last, email, title)
                stored_hash  = str(c.get(C_HASH, "")).strip()
                if contact_ns and contact_ns not in ("nan", "None") and stored_hash == current_hash:
                    # No change since last push — skip the NS PATCH
                    pushed_emails[em_key] = contact_ns
                    continue

                new_id = sync_contact(result_id, school_name, {
                    "first": first, "last": last,
                    "email": email, "role": title,
                    "ns_id": contact_ns if contact_ns not in ("", "nan", "None") else "",
                }, school_info_out, shared=is_shared)
                if new_id:
                    c[C_NS_CID] = str(new_id)
                    c[C_SYNCED] = datetime.now().strftime("%Y-%m-%d %H:%M")
                    c[C_HASH]   = current_hash
                    pushed_emails[em_key] = str(new_id)
                elif new_id is None and not contact_ns:
                    c[C_NS_CID] = "UNLINKED"
                    pushed_emails[em_key] = ""
                time.sleep(0.15)
            elif sync_flag == "N" and contact_ns not in ("", "nan", "None", "UNLINKED"):
                if em_key not in pushed_emails:
                    # Same PERSON still active at THIS school under another
                    # row? Happens when their email changes: the old-email
                    # row goes Sync=N while the new-email row (sharing the
                    # NS Contact ID) stays Y. That's a dead ROW, not a
                    # departed person — unlink the row, touch nothing in NS
                    # (inactivating would kill the still-active contact and
                    # remove_contact_ship_to would strip their address,
                    # since the Ship-To label is the person's name).
                    if school_name in y_schools_by_cid.get(contact_ns, set()):
                        print(f"  Stale row (not a departure): {first} {last} "
                              f"still active at {school_name} under another row "
                              f"— unlinking only")
                        pushed_emails[em_key] = ""
                        c[C_NS_CID] = ""
                        continue
                    # Does this person still actively serve another school?
                    still_at = (y_schools_by_email.get(em_key, set())
                                | y_schools_by_cid.get(contact_ns, set())) - {school_name}
                    if still_at and restlet_available():
                        # Co-op person leaving THIS school only: detach the
                        # shared card from this school's Relationships tab
                        # and drop their Ship-To here — but keep the card
                        # itself active for their other school(s).
                        print(f"  Co-op departure: {first} {last} left {school_name} "
                              f"but still at {sorted(still_at)[:3]} — detaching, not inactivating")
                        ns_restlet_attach(contact_ns, result_id, "detach")
                        remove_contact_ship_to(result_id, f"{first} {last}")
                    else:
                        inactivate_contact(contact_ns, f"{first} {last}")
                        remove_contact_ship_to(result_id, f"{first} {last}")
                    pushed_emails[em_key] = ""
                c[C_NS_CID] = ""
                time.sleep(0.15)

        # Ship-To addresses. WI: WIAA scrape filled school_info_out with
        # the school's address. IL: school_info_out comes from the sheet,
        # so addresses are present whenever Andy populated Address1/City.
        # Skip if we genuinely don't have an address row.
        if school_info_out.get("address1") or school_info_out.get("city") or state != "IL":
            active_contacts = [
                {"first": str(c.get(C_FIRST, "")).strip(),
                 "last":  str(c.get(C_LAST, "")).strip(),
                 "email": str(c.get(C_EMAIL, "")).strip(),
                 "role":  str(c.get(C_ROLE, "")).strip()}
                for c in school_contacts
                if str(c.get(C_SYNC, "N")).strip().upper() == "Y"
            ]
            if active_contacts and school_info_out:
                # Pass display_name (Full Name column) so the address
                # addressee matches the customer's company name, not the
                # short School Name.
                sync_address_book(result_id, school_info_out, active_contacts,
                                  school_name=display_name)
                # Parent/child customers: also keep the parent's Ship-To
                # book current. Addressee uses School Name, which holds the
                # district-level name for these schools. heal_billing=False:
                # the parent's Bill-To is the district office and must not
                # be overwritten with the school's WIAA address.
                parent_id = sch.get("parent", "")
                if parent_id.isdigit() and parent_id != str(result_id):
                    sync_address_book(parent_id, school_info_out, active_contacts,
                                      school_name=school_name,
                                      heal_billing=False)

        # Save every 10 schools so a timeout doesn't lose all hash progress.
        # sync_y_N is modulo — cheap sheet write vs. hours of re-push on re-run.
        if synced_schools % 10 == 0:
            save_contacts(contacts_ws, contacts_data)

        time.sleep(DELAY)

    # Final save catches anything after the last modulo checkpoint
    save_contacts(contacts_ws, contacts_data)
    if synced_col and synced_updates:
        batch = [{
            "range": gspread.utils.rowcol_to_a1(row, synced_col),
            "values": [[ts]],
        } for row, ts in synced_updates]
        master_ws.batch_update(batch)
    if ns_id_col and created_updates:
        batch = [{
            "range": gspread.utils.rowcol_to_a1(row, ns_id_col),
            "values": [[new_id]],
        } for row, new_id in created_updates]
        master_ws.batch_update(batch)

    print(f"\n{'=' * 60}")
    print(f"  PUSH COMPLETE")
    print(f"  Schools pushed:  {synced_schools}")
    print(f"  Customers created: {len(created_updates)}")
    for row, new_id in created_updates:
        print(f"    row {row}  ->  NS {new_id}")
    print(f"  Errors:          {errors}")
    print(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)


if __name__ == "__main__":
    main()
