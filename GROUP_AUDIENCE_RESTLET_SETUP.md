# Group Audience Fix — RESTlet Setup (one-time, ~10 minutes)

## Why

The 13 coach/AD Groups (Lists > Relationships > Groups: Baseball Coaches,
Boys Football Coaches, Athletic Directors, ...) are **dynamic** — each
one's membership is recomputed live from a linked saved search, nothing is
stored on the group record itself. None of those searches filter out
inactive contacts, so every duplicate this sync has ever retired (e.g.
"Ryan McKittrick (dup 54939)") still shows up as a group member. Adding an
`Inactive = No` filter to each search fixes it permanently — NetSuite
recomputes the group from the search on every view/send, so there's
nothing to re-run once it's done.

`suitescript/group_audience_restlet.js` does that fix via `N/search`
(load the saved search, add the filter, save). It's a **separate** script
from `attach_contact_restlet.js` — the one the nightly sync depends on
every run — so a bug here can never affect attach/detach.

## Step 1 — Upload the script file

1. In NetSuite: **Documents > Files > File Cabinet**
2. Open the **SuiteScripts** folder
3. Click **Add File** and upload `suitescript/group_audience_restlet.js`
   (download it from this repo first)

## Step 2 — Create the Script record

1. **Customization > Scripting > Scripts > New**
2. In the SCRIPT FILE field, select `group_audience_restlet.js` → click
   **Create Script Record** (NetSuite auto-detects it as a RESTlet)
3. Name: `BSG Group Audience Fix` — then **Save**

## Step 3 — Deploy it

1. On the script record, go to the **Deployments** tab → **Add**
2. Set:
   - **Status:** Released
   - **Log Level:** Audit
   - **Audience > Roles:** check the role your sync integration's access
     token uses (same role as the other RESTlet's deployment)
3. **Save**

## Step 4 — Copy the two IDs and add them as GitHub secrets

On the saved deployment page, the **External URL** ends with
`...restlet.nl?script=1234&deploy=1`. GitHub repo → **Settings > Secrets
and variables > Actions** → add:

| Secret name | Value |
|---|---|
| `NS_GROUP_RESTLET_SCRIPT_ID` | the `script=` value |
| `NS_GROUP_RESTLET_DEPLOY_ID` | the `deploy=` value |

## Step 5 — Get the 13 saved searches' internal ids

CRM Group records aren't reachable from SuiteScript at all (same wall as
the REST API — confirmed live: `record.load({type:'group'...})` fails
with "The record type [GROUP] is invalid"), so this needs each **saved
search's own** internal id, not the group's.

1. NetSuite: **Lists > Search > Saved Searches**
2. Filter **Type = Contact**
3. Find the 13 rows named `<Sport> Coaches - Email Audience` (and
   `Athletic Directors - Email Audience`) and note each one's **ID**
   column value (a number, or a `customsearch_...` id — either works)

## Step 6 — Run it

GitHub → **Actions > "Manual - Fix Group Audiences"** → Run workflow:

1. **search_ids_json** — paste all 13, e.g.:
   ```json
   {"Athletic Directors": 12340, "Baseball Coaches": 12341, "Boys Basketball Coaches": 12342, "Boys Football Coaches": 12343, "Boys Soccer Coaches": 12344, "Cross Country Coaches": 12345, "Girls Basketball Coaches": 12346, "Girls Soccer Coaches": 12347, "Girls Volleyball Coaches": 12348, "Gymnastics Coaches": 12349, "Softball Coaches": 12350, "Track and Field Coaches": 12351, "Wrestling Coaches": 12352}
   ```
2. **sales_rep_id** — NetSuite employee id to scope every search to
   (defaults to `3` = Andrew Murray). Leave blank to skip scoping and
   only fix the inactive filter.
3. First run: leave **live** unchecked — this prints, per search, its
   title/type, current filters/columns, current member count, and what
   it *would* change (including the member count after the change).
   Nothing is written.
4. **Read the plan.** If any entry shows an error, stop and share the
   output rather than running blind against a live marketing search.
5. Once the dry-run plan looks right for all 13: run again with **live**
   checked, same inputs. It's idempotent — safe to re-run.
