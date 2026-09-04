# Weekly Scores — Outlook Relay Setup (Andy's PC)

## How it works

1. Monday 7:00 AM — the GitHub job emails **andy@bsgsports.com** one scores
   digest per sales rep, subject tagged with the rep's address, e.g.
   `[TEST → kylel@bsgsports.com] Kyle Loughrin — School Scores — Week of …`
2. Monday 8:00 AM — Task Scheduler runs `run_scores_relay.bat` on Andy's PC.
   `relay_scores_outlook.py` finds those tagged emails in Outlook and sends
   each one to its rep as a **new message from andy@bsgsports.com** — same
   body, clean subject (tag removed), no forwarding header block.
3. Each original is tagged with the Outlook category **Scores Relayed**, so
   it is never sent twice. Andy keeps the originals; the reps get clean
   copies; Paul's copy CCs Julie.

The GitHub side stays in test mode forever (all emails to Andy). Never set
`SCORES_LIVE_DEFAULT` — the relay *is* the delivery.

## One-time setup (~10 minutes)

Same folder as the other local scripts:
`C:\Users\andre\OneDrive - Badger Sporting Goods\Desktop\Illinois Contact List\Netsuite Contacts Sync`

1. Download these two files from GitHub into that folder
   (open the file → download icon ⤓):
   - `relay_scores_outlook.py`
   - `run_scores_relay.bat`
2. Open Command Prompt (Windows key → type `cmd` → Enter) and run:
   ```
   pip install pywin32
   ```
3. **Classic Outlook** must be signed in as andy@bsgsports.com and open when
   the script runs. "New Outlook" cannot be automated, but the two run side
   by side on the same mailbox — keep using New Outlook day to day, just
   open **Outlook (classic)** from the Start menu (or flip the "New Outlook"
   toggle off) and leave it running in the background. Andy uses New
   Outlook, so classic Outlook must be signed in once and left open on
   Mondays.
4. **Tag the old test emails so they are never relayed.** In Command Prompt:
   ```
   cd "C:\Users\andre\OneDrive - Badger Sporting Goods\Desktop\Illinois Contact List\Netsuite Contacts Sync"
   python relay_scores_outlook.py --mark-existing --since-hours 720
   ```
   (Marks every tagged scores email from the last 30 days as done.)
5. **Test the relay on yourself** — after the next Monday email arrives:
   ```
   python relay_scores_outlook.py --dry-run
   python relay_scores_outlook.py --test-to andy@bsgsports.com
   ```
   You'll receive clean copies from yourself, exactly as the reps would.
   (Note: `--test-to` marks them relayed; to relay the same emails to the
   reps afterwards, remove the "Scores Relayed" category from them in
   Outlook first.)

## Task Scheduler

1. Start → **Task Scheduler** → **Create Task…**
2. **General:** Name `Scores Relay`; leave **Run only when user is logged on**
   selected (Outlook automation only works inside the logged-in desktop
   session — do NOT pick "whether user is logged on or not")
3. **Triggers → New:** Weekly, **Monday**, **8:00 AM**
4. **Actions → New:** Program/script:
   `C:\Users\andre\OneDrive - Badger Sporting Goods\Desktop\Illinois Contact List\Netsuite Contacts Sync\run_scores_relay.bat`
5. **Settings:** check **Run task as soon as possible after a scheduled start
   is missed** (so a PC that was asleep at 8 AM catches up on wake)
6. OK

Everything the script does is written to `relay_log.txt` in that folder.
The PC must be logged in (screen lock is fine) and classic Outlook open at
8:00 AM Monday; otherwise the task runs at the next login.

## Day-to-day

- Nothing. Monday morning the reps get their digests from Andy.
- Reps' addresses come from the subject tag the GitHub job writes; to change
  who gets what, change the **Sales Rep** column on the master sheet.
- To CC someone on a rep's copy, edit `CC_FOR` at the top of the script.
- If the PC was off all Monday, the task runs at next startup (within the
  36-hour window). Older than that, run it by hand with `--since-hours 72`.
