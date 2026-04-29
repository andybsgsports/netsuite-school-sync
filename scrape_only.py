"""
scrape_only.py — fast Google-Sheet-only refresh.

Iterates every school in the Schools tab (WI + IL), scrapes WIAA (for WI
rows) or IHSA (for IL rows), and appends any new (school, email, role)
rows to the Contacts tab. Does NOT call NetSuite.

Use when you want the master sheet populated quickly for review. A
separate task/workflow pushes those contacts to NetSuite later.

Env:
  GOOGLE_SHEET_ID, GOOGLE_CREDENTIALS_JSON
  SCHOOL_FILTER  - optional, single-school testing
  STATE_FILTER   - optional, "WI" or "IL"
"""
from __future__ import annotations

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import gspread

from netsuite_sync import scrape_wiaa_school_detail, smart_title
from ihsa_sync import scrape_school as ihsa_scrape_school, extract_school_id
from school_netsuite_sync import (
    get_gspread_client,
    load_contacts, save_contacts,
    GOOGLE_SHEET_ID, MASTER_TAB,
    M_NAME, M_URL, M_NS_ID, M_STATE, M_LOCKED, M_SYNCED,
    C_SCHOOL, C_FIRST, C_LAST, C_EMAIL, C_ROLE, C_TYPE,
    C_SYNC, C_NS_CID, C_NS_CUS, C_SYNCED,
)

SCHOOL_FILTER = os.environ.get("SCHOOL_FILTER", "").strip()
STATE_FILTER  = os.environ.get("STATE_FILTER", "").strip().upper()
CONCURRENCY   = int(os.environ.get("CONCURRENCY", "10"))  # parallel WIAA/IHSA fetches


def sort_schools_tab(ws):
    """Reorder the Schools tab alphabetically by School Name (keeps header)."""
    values = ws.get_all_values()
    if len(values) < 2:
        return
    headers = values[0]
    if M_NAME not in headers:
        return
    name_idx = headers.index(M_NAME)
    body = values[1:]
    sorted_body = sorted(body, key=lambda r: (r[name_idx].strip().lower()
                                              if len(r) > name_idx else ""))
    if sorted_body == body:
        return  # already sorted, no write needed
    ws.clear()
    ws.update(range_name="A1", values=[headers] + sorted_body)
    print(f"  [SHEETS] Schools tab re-sorted alphabetically ({len(sorted_body)} rows)")


def load_all_schools(gc):
    wb = gc.open_by_key(GOOGLE_SHEET_ID)
    ws = wb.worksheet(MASTER_TAB)
    sort_schools_tab(ws)
    values = ws.get_all_values()
    if not values:
        return [], ws, None
    headers = values[0]
    synced_col = headers.index(M_SYNCED) + 1 if M_SYNCED in headers else None
    out = []
    for i, raw in enumerate(values[1:], start=2):
        rec = dict(zip(headers, raw))
        name = str(rec.get(M_NAME, "")).strip()
        url  = str(rec.get(M_URL, "")).strip()
        state = str(rec.get(M_STATE, "")).strip().upper()
        locked = str(rec.get(M_LOCKED, "")).strip().upper() == "Y"
        ns_id = str(rec.get(M_NS_ID, "")).strip()
        if not name or not url or locked:
            continue
        if SCHOOL_FILTER and name != SCHOOL_FILTER:
            continue
        if STATE_FILTER and state != STATE_FILTER:
            continue
        out.append({"row": i, "name": name, "url": url, "state": state, "ns_id": ns_id})
    return out, ws, synced_col


def scrape_wi(url):
    try:
        info, admins, coaches = scrape_wiaa_school_detail(url)
    except Exception as exc:
        print(f"    WIAA scrape error: {exc}")
        return []
    return [
        *[{"first": smart_title(p.get("first","")),
           "last":  smart_title(p.get("last","")),
           "email": p.get("email",""),
           "role":  p.get("role",""),
           "type":  p.get("type","Admin")}
          for p in admins],
        *[{"first": smart_title(p.get("first","")),
           "last":  smart_title(p.get("last","")),
           "email": p.get("email",""),
           "role":  p.get("role",""),
           "type":  p.get("type","Coach")}
          for p in coaches],
    ]


def scrape_il(url):
    school_id = extract_school_id(url)
    if not school_id:
        return []
    try:
        people = ihsa_scrape_school(school_id)
    except Exception as exc:
        print(f"    IHSA scrape error: {exc}")
        return []
    out = []
    for p in people:
        out.append({
            "first": smart_title(p.get("first","")),
            "last":  smart_title(p.get("last","")),
            "email": p.get("email",""),
            "role":  p.get("role",""),
            "type":  p.get("type",""),
        })
    return out


def main():
    print("=" * 60)
    print(f"  SCRAPE-ONLY  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    if STATE_FILTER:  print(f"  STATE_FILTER: {STATE_FILTER}")
    if SCHOOL_FILTER: print(f"  SCHOOL_FILTER: {SCHOOL_FILTER}")
    print("=" * 60 + "\n")

    if not GOOGLE_SHEET_ID:
        print("ERROR: GOOGLE_SHEET_ID env var not set.")
        sys.exit(1)

    gc = get_gspread_client()
    schools, master_ws, synced_col = load_all_schools(gc)
    contacts_data, contacts_ws = load_contacts(gc)

    existing_keys = {
        (str(c.get(C_SCHOOL, "")).strip(),
         str(c.get(C_EMAIL, "")).strip().lower(),
         str(c.get(C_ROLE, "")).strip().lower())
        for c in contacts_data
        if str(c.get(C_EMAIL, "")).strip()
    }

    print(f"  Schools to scrape: {len(schools)}")
    print(f"  Existing contacts in sheet: {len(contacts_data)}\n")

    added = 0
    errors = 0
    synced_updates = []

    def scrape_one(sch):
        """Returns (sch, people_list). Runs in a worker thread."""
        state = sch["state"]
        people = scrape_wi(sch["url"]) if state == "WI" else scrape_il(sch["url"])
        return sch, people

    print(f"  Scraping with {CONCURRENCY} parallel workers\n")
    results = []
    done = 0
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futures = {pool.submit(scrape_one, s): s for s in schools}
        for fut in as_completed(futures):
            done += 1
            sch, people = fut.result()
            print(f"[{done}/{len(schools)}] {sch['name']} ({sch['state']}) — {len(people)} contacts")
            results.append((sch, people))

    # Merge scraped results into contacts_data on the main thread (avoids
    # thread-safety issues with the shared list + existing_keys set).
    for sch, people in results:
        new_for_school = 0
        for p in people:
            em = (p["email"] or "").strip().lower()
            rk = (p["role"]  or "").strip().lower()
            if not em:
                continue
            if (sch["name"], em, rk) in existing_keys:
                continue
            contacts_data.append({
                C_SCHOOL: sch["name"],
                C_FIRST:  p["first"],
                C_LAST:   p["last"],
                C_EMAIL:  p["email"],
                C_ROLE:   p["role"],
                C_TYPE:   p["type"],
                C_SYNC:   "Y",
                C_NS_CID: "",
                C_NS_CUS: sch["ns_id"],
                C_SYNCED: "",
            })
            existing_keys.add((sch["name"], em, rk))
            new_for_school += 1
            added += 1
        if new_for_school:
            print(f"  + {sch['name']}: {new_for_school} new row(s)")
        synced_updates.append((sch["row"], datetime.now().strftime("%Y-%m-%d %H:%M")))
    scanned = len(schools)

    # Save once at end (much faster than per-school)
    save_contacts(contacts_ws, contacts_data)

    # Update Last Synced column for scraped schools
    if synced_col and synced_updates:
        batch = [{
            "range": gspread.utils.rowcol_to_a1(row, synced_col),
            "values": [[ts]],
        } for row, ts in synced_updates]
        master_ws.batch_update(batch)

    print(f"\n{'=' * 60}")
    print(f"  SCRAPE COMPLETE")
    print(f"  Schools scanned: {scanned}")
    print(f"  New rows added:  {added}")
    print(f"  Errors:          {errors}")
    print(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)


if __name__ == "__main__":
    main()
