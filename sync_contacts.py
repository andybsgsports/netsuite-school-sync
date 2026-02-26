import gspread
import requests
import json
import time
from datetime import datetime
from google.oauth2.service_account import Credentials
# ── Config ────────────────────────────────────────────────────────────────────
SHEET_ID   = '1_Hz78NcOCSaDXBlsEHmbZAVfmwnLRgjVGRqt3RaTvhw'
NS_ACCOUNT = '11319665'
NS_BASE    = f'https://{NS_ACCOUNT}.suitetalk.api.netsuite.com'
NS_REST    = f'https://{NS_ACCOUNT}.app.netsuite.com/services/rest/record/v1'
SUBSIDIARY = 1   # Badger Sporting Goods Company
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']
# ── NetSuite OAuth1 session ───────────────────────────────────────────────────
# Paste your NS credentials here
NS_CONSUMER_KEY    = 'YOUR_CONSUMER_KEY'
NS_CONSUMER_SECRET = 'YOUR_CONSUMER_SECRET'
NS_TOKEN           = 'YOUR_TOKEN_ID'
NS_TOKEN_SECRET    = 'YOUR_TOKEN_SECRET'
from requests_oauthlib import OAuth1
def ns_session():
    return OAuth1(
        NS_CONSUMER_KEY, NS_CONSUMER_SECRET,
        NS_TOKEN, NS_TOKEN_SECRET,
        signature_method='HMAC-SHA256'
    )
# ── Google Sheets ─────────────────────────────────────────────────────────────
creds = Credentials.from_service_account_file('credentials.json', scopes=SCOPES)
gc    = gspread.authorize(creds)
wb    = gc.open_by_key(SHEET_ID)
schools_ws  = wb.worksheet('Schools')
contacts_ws = wb.worksheet('Contacts')
print("Reading Schools tab...")
school_rows = schools_ws.get_all_values()
sh = school_rows[0]
sn_col  = sh.index('School Name')
nsc_col = sh.index('NS Customer ID')
nsp_col = sh.index('NS Parent Customer ID')
# Build school → {primary_id, parent_id} map
school_map = {}
for row in school_rows[1:]:
    name = row[sn_col].strip() if len(row) > sn_col else ''
    nsid = row[nsc_col].strip() if len(row) > nsc_col else ''
    nspa = row[nsp_col].strip() if len(row) > nsp_col else ''
    if name and nsid:
        # Extract just the numeric part from "381-Beloit Turner School District"
        primary_num = nsid.split('-')[0].strip()
        parent_num  = nspa.split('-')[0].strip() if nspa else None
        school_map[name] = {'primary': primary_num, 'parent': parent_num}
print(f"Loaded {len(school_map)} schools with NS IDs")
print("Reading Contacts tab...")
contact_rows = contacts_ws.get_all_values()
ch = contact_rows[0]
csn_col   = ch.index('School Name')
cfn_col   = ch.index('First Name')
cln_col   = ch.index('Last Name')
cem_col   = ch.index('Email')
crl_col   = ch.index('Role')
ctp_col   = ch.index('Type')
csync_col = ch.index('Sync (Y/N)')
cnid_col  = ch.index('NS Contact ID')
clsd_col  = ch.index('Last Synced')
print(f"Loaded {len(contact_rows)-1} contacts")
# ── Get NetSuite internal ID from customer display number ─────────────────────
def get_ns_internal_id(customer_num):
    """Look up NetSuite internal ID by customer display number"""
    resp = requests.post(
        f'https://{NS_ACCOUNT}.app.netsuite.com/services/rest/query/v1/suiteql',
        auth=ns_session(),
        headers={'Content-Type': 'application/json', 'Prefer': 'transient'},
        json={'q': f"SELECT id FROM customer WHERE entityid = '{customer_num}' FETCH FIRST 1 ROWS ONLY"}
    )
    data = resp.json()
    if data.get('items'):
        return data['items'][0]['id']
    return None
# Cache internal IDs
id_cache = {}
def get_internal_id(num):
    if num not in id_cache:
        id_cache[num] = get_ns_internal_id(num)
    return id_cache[num]
# ── Check if contact already exists on a customer ────────────────────────────
def find_existing_contact(internal_customer_id, email, first, last):
    """Search for existing contact by email or name on this customer"""
    # Search by email first
    if email:
        resp = requests.post(
            f'https://{NS_ACCOUNT}.app.netsuite.com/services/rest/query/v1/suiteql',
            auth=ns_session(),
            headers={'Content-Type': 'application/json', 'Prefer': 'transient'},
            json={'q': f"SELECT id FROM contact WHERE email = '{email}' AND company = '{internal_customer_id}' FETCH FIRST 1 ROWS ONLY"}
        )
        data = resp.json()
        if data.get('items'):
            return data['items'][0]['id']
    # Search by name
    resp = requests.post(
        f'https://{NS_ACCOUNT}.app.netsuite.com/services/rest/query/v1/suiteql',
        auth=ns_session(),
        headers={'Content-Type': 'application/json', 'Prefer': 'transient'},
        json={'q': f"SELECT id FROM contact WHERE firstname = '{first}' AND lastname = '{last}' AND company = '{internal_customer_id}' FETCH FIRST 1 ROWS ONLY"}
    )
    data = resp.json()
    if data.get('items'):
        return data['items'][0]['id']
    return None
# ── Create or update a contact on a customer ─────────────────────────────────
def upsert_contact(internal_customer_id, first, last, email, title, existing_id=None):
    payload = {
        'firstName': first,
        'lastName':  last,
        'email':     email,
        'title':     title,
        'company':   {'id': str(internal_customer_id)},
        'subsidiary':{'id': str(SUBSIDIARY)}
    }
    if existing_id:
        # Update
        resp = requests.patch(
            f'{NS_REST}/contact/{existing_id}',
            auth=ns_session(),
            headers={'Content-Type': 'application/json'},
            json=payload
        )
        return existing_id, 'updated', resp.status_code
    else:
        # Create
        resp = requests.post(
            f'{NS_REST}/contact',
            auth=ns_session(),
            headers={'Content-Type': 'application/json'},
            json=payload
        )
        if resp.status_code == 204:
            new_id = resp.headers.get('Location','').split('/')[-1]
            return new_id, 'created', resp.status_code
        return None, 'failed', resp.status_code
# ── Main sync loop ─────────────────────────────────────────────────────────────
print("\\nStarting contact sync...")
synced = 0
skipped = 0
errors = 0
for i, row in enumerate(contact_rows[1:], start=2):
    if len(row) <= max(csn_col, cfn_col, cln_col):
        continue
    school_name = row[csn_col].strip()
    first       = row[cfn_col].strip()
    last        = row[cln_col].strip()
    email       = row[cem_col].strip() if len(row) > cem_col else ''
    role        = row[crl_col].strip() if len(row) > crl_col else ''
    if not school_name or not first or not last:
        skipped += 1
        continue
    school = school_map.get(school_name)
    if not school or not school['primary']:
        print(f"  Row {i}: No NS ID for school '{school_name}' — skipping")
        skipped += 1
        continue
    # Get internal IDs
    primary_internal = get_internal_id(school['primary'])
    if not primary_internal:
        print(f"  Row {i}: Could not find NS internal ID for customer {school['primary']} — skipping")
        skipped += 1
        continue
    targets = [primary_internal]
    if school['parent']:
        parent_internal = get_internal_id(school['parent'])
        if parent_internal:
            targets.append(parent_internal)
    contact_ids = []
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    for cust_internal_id in targets:
        existing_id = find_existing_contact(cust_internal_id, email, first, last)
        ns_id, action, status = upsert_contact(
            cust_internal_id, first, last, email, role, existing_id
        )
        if ns_id:
            contact_ids.append(str(ns_id))
            print(f"  Row {i}: {first} {last} @ {school_name} — {action} (NS ID: {ns_id})")
        else:
            print(f"  Row {i}: {first} {last} @ {school_name} — FAILED (status {status})")
            errors += 1
        time.sleep(0.2)
    # Write NS Contact ID and Last Synced back to sheet
    if contact_ids:
        contacts_ws.update_cell(i, cnid_col + 1, ', '.join(contact_ids))
        contacts_ws.update_cell(i, clsd_col + 1, now)
        synced += 1
        time.sleep(0.1)
print(f"\\n✅ Done! Synced: {synced} | Skipped: {skipped} | Errors: {errors}")