import csv
import gspread
import time
from google.oauth2.service_account import Credentials
SHEET_ID = "1_Hz78NcOCSaDXBlsEHmbZAVfmwnLRgjVGRqt3RaTvhw"
CREDS_FILE = "credentials.json"
CSV_FILE = "CustomersProjects412.csv"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
def norm(s):
    s = s.lower().strip()
    for w in ["high school","hs","school district","district","school"]:
        s = s.replace(w,"")
    s = s.replace("st.","saint").replace("mt.","mount").replace("-"," ").replace("'","")
    return " ".join(s.split())
print("Loading CSV...")
ns = []
with open(CSV_FILE,encoding="utf-8-sig") as f:
    for row in csv.DictReader(f):
        ns.append({"id":row["Internal ID"],"name":row["Company Name"],"norm":norm(row["Company Name"])})
print("Loaded "+str(len(ns))+" customers")
creds = Credentials.from_service_account_file(CREDS_FILE,scopes=SCOPES)
gc = gspread.authorize(creds)
ws = gc.open_by_key(SHEET_ID).worksheet("Schools")
rows = ws.get_all_values()
headers = rows[0]
nc = headers.index("School Name")
ic = headers.index("NS Customer ID")
updates = []
for i,row in enumerate(rows[1:],start=2):
    sn = row[nc] if len(row)>nc else ""
    cur = row[ic] if len(row)>ic else ""
    if not sn or cur:
        continue
    nn = norm(sn)
    hits = [c for c in ns if nn in c["norm"] or c["norm"] in nn]
    if len(hits)==1:
        print("FOUND: "+sn+" -> "+hits[0]["id"]+" ("+hits[0]["name"]+")")
        updates.append((i,ic+1,hits[0]["id"]))
    elif len(hits)>1:
        print("MULTI: "+sn)
        for h in hits:
            print("  "+h["id"]+" "+h["name"])
    else:
        print("MISS:  "+sn+" ["+nn+"]")
print("Writing "+str(len(updates))+" updates...")
for r,c,v in updates:
    ws.update_cell(r,c,v)
    time.sleep(0.1)
print("Done!")