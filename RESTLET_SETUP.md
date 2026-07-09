# Shared Contact Cards — RESTlet Setup (one-time, ~10 minutes)

## Why

NetSuite fully supports **one contact card attached to multiple schools**
(the "Attach" button on a customer's Relationships > Contacts tab — e.g.
Tony Brewer on both Albany and Monticello). But the REST API our sync uses
treats that list (`contactRoles`) as **read-only**, which is why the sync
historically created a duplicate contact record per school for co-op
coaches like Bret St. Arnauld (Mt. Horeb + Barneveld).

`suitescript/attach_contact_restlet.js` is a tiny script that runs *inside*
NetSuite and exposes the native attach/detach to the sync. Once deployed,
the sync maintains **one card per person, attached to every school they
serve** — no more duplicates, and the Manage Duplicates page stays clean.

## Step 1 — Upload the script file

1. In NetSuite: **Documents > Files > File Cabinet**
2. Open the **SuiteScripts** folder
3. Click **Add File** and upload `suitescript/attach_contact_restlet.js`
   (download it from this repo first)

## Step 2 — Create the Script record

1. **Customization > Scripting > Scripts > New**
2. In the SCRIPT FILE field, select `attach_contact_restlet.js` → click
   **Create Script Record** (NetSuite auto-detects it as a RESTlet)
3. Name: `BSG Attach Contact` — then **Save**

## Step 3 — Deploy it

1. On the script record, go to the **Deployments** tab → **Add** (or click
   **Deploy Script**)
2. Set:
   - **Status:** Released
   - **Log Level:** Audit
   - **Audience > Roles:** check the role your sync integration's access
     token uses (the same role you granted SuiteAnalytics Workbook to).
     ⚠️ This is the step people miss — without it the sync gets a
     permission error.
3. **Save**

## Step 4 — Copy the two IDs

On the saved deployment page, find the **External URL**. It ends with
something like:

```
...restlet.nl?script=1234&deploy=1
```

Copy the two values (`1234` and `1`).

## Step 5 — Add them as GitHub secrets

GitHub repo → **Settings > Secrets and variables > Actions** → add:

| Secret name | Value |
|---|---|
| `NS_RESTLET_SCRIPT_ID` | the `script=` value (e.g. `1234`) |
| `NS_RESTLET_DEPLOY_ID` | the `deploy=` value (e.g. `1`) |

## Step 6 — Migrate the existing duplicates

GitHub → **Actions > "Manual - Merge Co-op Contacts (shared cards)"** →
Run workflow:

1. First run: leave **live** unchecked, optionally set **email** to one
   test person (e.g. Bret St. Arnauld's email) — this prints the plan and
   changes nothing.
2. If the plan looks right: run again with **live** checked (and the email
   filter removed to do everyone).

What the migration does per co-op person: keeps their oldest contact
record as the shared card, attaches it to every school they serve,
retires the duplicate records (renamed + inactivated), and repoints the
sheet's NS Contact ID column at the shared card.

## After that — fully automatic

The nightly sync detects co-op people on its own (same email at 2+ schools
on the Contacts tab) and:

- **New co-op coach:** creates one card at the first school, *attaches* it
  at every additional school (no duplicate).
- **Updates:** edits the one shared card without bouncing its primary
  company between schools.
- **Leaves one school:** *detaches* the card from that school only — it
  stays active for their other school(s).
- **Leaves all schools:** the card is inactivated as usual.

If the two secrets are ever missing, everything silently falls back to the
old per-school-duplicate behavior — nothing breaks.
