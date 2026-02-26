import gspread
import requests
import time
from google.oauth2.service_account import Credentials
from netsuite_sync import make_auth, NS_ACCOUNT
SHEET_ID = "1_Hz78NcOCSaDXBlsEHmbZAVfmwnLRgjVGRqt3RaTvhw"
CREDS_FILE = "credentials.json"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
BASE_URL = "https://" + NS_ACCOUNT + ".suitetalk.api.netsuite.com/services/rest/record/v1"
def ns_search(school_name):
    encoded = requests.utils.quote(school_name)
    url = BASE_URL + "/customer?q=companyName+CONTAINS+" + encoded + "&limit=10&isInactive=false"
    headers = {
        "Authorization": make_auth("GET", url),
        "Content-Type": "application/json"
    }
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        items = resp.json().get("items", [])
        return items
    else:
        print("  ERROR " + str(resp.status_code) + ": " + resp.text[:300])
        return []
print("Connecting to Google Sheets...")
creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
ws = sh.worksheet("Schools")
rows = ws.get_all_values()
headers = rows[0]
name_col = headers.index("School Name")
ns_id_col = headers.index("NS Customer ID")
print("Looking up NetSuite Customer IDs...\\n")
updates = []
for i, row in enumerate(rows[1:], start=2):
    school_name = row[name_col] if len(row) > name_col else ""
    current_id = row[ns_id_col] if len(row) > ns_id_col else ""
    if not school_name:
        continue
    if current_id:
        print("  [SKIP] " + school_name + " already has ID: " + current_id)
        continue
    results = ns_search(school_name)
    if len(results) == 1:
        ns_id = str(results[0]["id"])
        ns_name = results[0].get("companyName", "")
        print("  [FOUND] " + school_name + " -> " + ns_id + " (" + ns_name + ")")
        updates.append((i, ns_id_col + 1, ns_id))
    elif len(results) > 1:
        print("  [MULTIPLE] " + school_name + " -> " + str(len(results)) + " matches:")
        for r in results:
            print("    " + str(r["id"]) + " - " + r.get("companyName", ""))
    else:
        print("  [NOT FOUND] " + school_name)
    time.sleep(0.3)
print("\\nWriting IDs back to sheet...")
for row_num, col_num, val in updates:
    ws.update_cell(row_num, col_num, val)
    time.sleep(0.1)
print("Done! " + str(len(updates)) + " IDs written to sheet.")