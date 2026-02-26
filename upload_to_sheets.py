import openpyxl
import gspread
from google.oauth2.service_account import Credentials
SHEET_ID = "1_Hz78NcOCSaDXBlsEHmbZAVfmwnLRgjVGRqt3RaTvhw"
CREDS_FILE = "credentials.json"
INPUT_FILE = "School_Master_List.xlsx"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
print("Connecting to Google Sheets...")
creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
print("Loading Excel file...")
wb = openpyxl.load_workbook(INPUT_FILE)
def upload_tab(ws_excel, tab_name):
    print("Uploading " + tab_name + " tab...")
    rows = list(ws_excel.iter_rows(values_only=True))
    clean = []
    for row in rows:
        clean.append([str(v) if v is not None else "" for v in row])
    try:
        ws = sh.worksheet(tab_name)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows=len(clean)+10, cols=len(clean[0])+5)
    ws.update(clean)
    print("  -> " + str(len(clean)-1) + " rows uploaded to " + tab_name)
upload_tab(wb["Schools"], "Schools")
upload_tab(wb["Contacts"], "Contacts")
print("Done! Google Sheet updated.")