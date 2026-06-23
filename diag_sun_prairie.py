"""
diag_sun_prairie.py — dump the Schools-tab row(s) matching 'Sun Prairie'
so we can see exactly what state the row is in (NS Customer ID, Parent ID,
Locked, URL, State, Last Synced, Notes) and whether a Match Confidence
column even exists.
"""
from __future__ import annotations
import os, json, gspread
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "")

def main():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"]), scopes=SCOPES)
    gc = gspread.authorize(creds)
    wb = gc.open_by_key(SHEET_ID)
    print(f"Spreadsheet: {wb.title}  (id={SHEET_ID})")
    ws = wb.worksheet("Schools")
    values = ws.get_all_values()
    headers = values[0]
    print(f"\nColumns ({len(headers)}): {headers}\n")
    has_mc = "Match Confidence" in headers
    print(f"Has 'Match Confidence' column? {has_mc}\n")

    hit = False
    for i, raw in enumerate(values[1:], start=2):
        rec = dict(zip(headers, raw))
        name = str(rec.get("School Name", ""))
        if "sun prairie" in name.lower():
            hit = True
            print("=" * 60)
            print(f"ROW {i}: {name}")
            for h in headers:
                v = rec.get(h, "")
                if v:
                    print(f"   {h:18}: {v}")
    if not hit:
        print("No row whose School Name contains 'Sun Prairie' was found.")

if __name__ == "__main__":
    main()
