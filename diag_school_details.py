"""
diag_school_details.py — read-only. For each school in DIAG_SCHOOLS
(comma-separated School Names), print the Schools-tab values next to what
the live source returns today (WIAA scrape for WI, IHSA API for IL), so a
stale field (e.g. Pecatonica enrollment 299 in the sheet vs 295 on IHSA) is
visible field by field. Touches no sheet cells and no NetSuite records.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from school_netsuite_sync import get_gspread_client, GOOGLE_SHEET_ID, MASTER_TAB, M_NAME, M_URL, M_STATE
from netsuite_sync import scrape_wiaa_school_detail
from ihsa_sync import (
    fetch_school_details, extract_school_id,
    M_CLASS, M_NICKNAME, M_COLORS, M_CONFERENCE, M_DISTRICT, M_ENROLLMENT,
    M_PHONE, M_ADDR1, M_ADDR2, M_CITY, M_ZIP, M_WEBSITE, M_SIZE,
)

FIELDS = [
    (M_ENROLLMENT, "enrollment"), (M_SIZE, "school_size"), (M_CLASS, "school_class"),
    (M_NICKNAME, "nickname"), (M_COLORS, "colors"), (M_CONFERENCE, "conference"),
    (M_DISTRICT, "wiaa_district"), (M_PHONE, "phone"), (M_ADDR1, "address1"),
    (M_ADDR2, "address2"), (M_CITY, "city"), (M_ZIP, "zip"), (M_WEBSITE, "website"),
]


def main():
    names = [n.strip() for n in os.environ.get("DIAG_SCHOOLS", "").split(",") if n.strip()]
    gc = get_gspread_client()
    records = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(MASTER_TAB).get_all_records()
    by_name = {str(r.get(M_NAME, "")).strip(): r for r in records}
    for name in names:
        r = by_name.get(name)
        print("=" * 78)
        if not r:
            print(f"  {name}: not on the Schools tab")
            continue
        state = str(r.get(M_STATE, "")).strip().upper()
        url = str(r.get(M_URL, "")).strip()
        print(f"  {name}  [{state}]  {url}")
        try:
            if state == "IL":
                live = fetch_school_details(extract_school_id(url))
            else:
                live, _a, _c = scrape_wiaa_school_detail(url)
        except Exception as e:
            print(f"  source error: {e!r}")
            continue
        if not live:
            print("  source returned nothing")
        for col, key in FIELDS:
            sheet_v = str(r.get(col, "")).strip()
            live_v = "" if live.get(key) in (None, "") else str(live.get(key)).strip()
            flag = "" if sheet_v.lower() == live_v.lower() else ("   <-- differs" if live_v else "   (source blank)")
            print(f"    {col:<12} sheet={sheet_v!r:<40} live={live_v!r}{flag}")


if __name__ == "__main__":
    main()
