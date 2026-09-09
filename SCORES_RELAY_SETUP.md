# Weekly Scores — Outlook Relay Setup (Andy's PC)

## How it works

1. Monday ~5:20 AM — the GitHub job emails **andy@bsgsports.com** one scores
   digest per sales rep, e.g. `Kyle Loughrin — School Scores — Week of …`
   (from andybsgsports@gmail.com; the rep is worked out from the name)
   (GitHub schedules can run hours late — 2026-09-07 it ran at 12:38 PM —
   which is why the PC task repeats hourly all day, see below.)
2. Monday 8:00 AM, then every hour until 8:00 PM — Task Scheduler runs
   `run_scores_relay.bat` on Andy's PC. `relay_scores_outlook.py` finds any
   scores emails from the GitHub sender in the last 36 hours not yet relayed and sends each one
   to its rep as a **new message from andy@bsgsports.com** — same body,
   clean subject (tag removed), no forwarding header block. Runs that find
   nothing new do nothing.
3. Each original is tagged with the Outlook category **Scores Relayed**, so
   it is never sent twice. Andy keeps the originals; the reps get clean
   copies; Paul's copy CCs Julie.

The GitHub side stays in test mode forever (all emails to Andy). Never set
`SCORES_LIVE_DEFAULT` — the relay *is* the delivery.

**Delivery design (Andy's choice, 2026-09-09):** `TEST_TO` at the top of
`relay_scores_outlook.py` stays set to andy@bsgsports.com. The relay sends
every clean copy (subject "Kyle Loughrin — School Scores — Week of …",
from andy@bsgsports.com) to Andy, and Andy's own Outlook rules redirect
each one to its rep. This is the permanent setup, not a test mode.
Rules must match the copy **from Andy Murray** only (the GitHub original
from andybsgsports@gmail.com has the identical subject), otherwise the rep
gets it twice. The relay adds no CC in this mode — Julie's copy of
Paul's email is handled by the rule.

## One-time setup (~10 minutes)

Same folder as the other local scripts:
`C:\Users\andre\OneDrive - Badger Sporting Goods\Desktop\Illinois Contact List\Netsuite Contacts Sync`

1. In Command Prompt, paste (one time only):
   ```
   cd "C:\Users\andre\OneDrive - Badger Sporting Goods\Desktop\Illinois Contact List\Netsuite Contacts Sync"
   curl -L -o run_scores_relay.bat https://raw.githubusercontent.com/andybsgsports/netsuite-school-sync/master/run_scores_relay.bat
   curl -L -o TEST_scores_relay.bat https://raw.githubusercontent.com/andybsgsports/netsuite-school-sync/master/TEST_scores_relay.bat
   ```
   Both batch files download the latest `relay_scores_outlook.py` from
   GitHub every time they run, so the script never needs re-downloading.
2. Open Command Prompt (Windows key → type `cmd` → Enter) and run:
   ```
   pip install pywin32
   ```
3. **Classic Outlook** must be signed in once as andy@bsgsports.com (done
   2026-09-04). "New Outlook" cannot be automated, but the two share the
   same mailbox. The script starts classic Outlook in the background when
   it runs, syncs mail, sends, waits for the Outbox to empty and closes it
   again — nothing needs to be open.
4. **Test on yourself:** double-click `TEST_scores_relay.bat` in that folder.
   It re-sends the tagged scores emails from the last 7 days to
   andy@bsgsports.com (not the reps), from andy@bsgsports.com — clean
   copies exactly as the reps would get them — and marks the originals
   "Scores Relayed" so Monday's run never re-sends them.
   Old test batches are ignored automatically: the Monday run only looks
   at the last 36 hours.

## Task Scheduler

1. Start → **Task Scheduler** → **Create Task…**
2. **General:** Name `Scores Relay`; leave **Run only when user is logged on**
   selected (Outlook automation only works inside the logged-in desktop
   session — do NOT pick "whether user is logged on or not")
3. **Triggers → New:** Weekly, **Monday**, **8:00 AM**; under *Advanced
   settings* tick **Repeat task every: 1 hour** — **for a duration of: 12
   hours** (covers a late GitHub run any time before 8 PM)
4. **Actions → New:** Program/script:
   `C:\Users\andre\OneDrive - Badger Sporting Goods\Desktop\Illinois Contact List\Netsuite Contacts Sync\run_scores_relay.bat`
5. **Settings:** check **Run task as soon as possible after a scheduled start
   is missed** (so a PC that was asleep at 8 AM catches up on wake)
6. OK

Everything the script does is written to `relay_log.txt` in that folder.
The PC must be logged in (screen lock is fine) at 8:00 AM Monday;
otherwise the task runs at the next login.

## Day-to-day

- Nothing. Monday morning the reps get their digests from Andy.
- Reps' addresses come from `REP_EMAILS` in the script (rep name → address);
  which schools a rep gets comes from the **Sales Rep** column on the sheet.
- To CC someone on a rep's copy, edit `CC_FOR` at the top of the script.
- If the PC was off all Monday, the task runs at next startup (within the
  36-hour window). Older than that, run it by hand with `--since-hours 72`.
