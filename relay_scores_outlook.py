"""
relay_scores_outlook.py
-----------------------
Runs on Andy's Windows PC (Task Scheduler, Monday mornings). Finds the
weekly scores emails the GitHub job delivered to andy@bsgsports.com —
subjects like "[TEST → kylel@bsgsports.com] Kyle Loughrin — School Scores
— Week of ..." — and re-sends each one to its sales rep as a brand-new
message FROM andy@bsgsports.com: same HTML body, clean subject, no
forwarding header block. Each original is tagged with the Outlook category
"Scores Relayed" so it is never sent twice.

Requires classic Outlook (desktop) signed in as andy@bsgsports.com, and:
    pip install pywin32

Usage:
    python relay_scores_outlook.py                 # relay anything new
    python relay_scores_outlook.py --dry-run       # show what WOULD be sent
    python relay_scores_outlook.py --test-to andy@bsgsports.com
                                                   # relay, but deliver copies
                                                   # to Andy instead of reps
    python relay_scores_outlook.py --mark-existing # tag all current matches as
                                                   # done WITHOUT sending (run
                                                   # once at setup so old test
                                                   # batches are never relayed)
    --since-hours N   only consider emails received in the last N hours
                      (default 36: covers Monday 7 AM email for a Monday or
                      Tuesday run, ignores older test batches)
"""

import argparse
import re
import sys
from datetime import datetime, timedelta

# Who else is copied on a rep's email (mirrors REPS "cc" in rep_digests.py)
CC_FOR = {
    "paul@bsgsports.com": "julie@bsgsports.com",
}

SCRIPT_VERSION = "2026-09-04d"           # printed at startup so we know which copy is running
SENDER_ADDRESS = "andy@bsgsports.com"     # account to send from
RELAYED_CATEGORY = "Scores Relayed"
TAG_RE = re.compile(r"^\[TEST\s*(?:→|->|>)\s*([^\]]+)\]\s*(.+)$")
INBOX = 6  # olFolderInbox


def log(msg):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def outlook_session():
    try:
        import win32com.client as win32
    except ImportError:
        log("ERROR: pywin32 not installed. Run:  pip install pywin32")
        sys.exit(1)
    app = win32.Dispatch("Outlook.Application")
    ns = app.GetNamespace("MAPI")
    return app, ns


def sending_account(ns):
    """The Outlook account for SENDER_ADDRESS, if configured (else default)."""
    try:
        for acct in ns.Accounts:
            if str(acct.SmtpAddress).lower() == SENDER_ADDRESS:
                return acct
    except Exception:
        pass
    return None


def _received_local(item):
    """Naive local datetime for item.ReceivedTime (pywin32 gives tz-aware)."""
    rt = item.ReceivedTime
    try:
        return rt.replace(tzinfo=None)
    except Exception:
        return datetime(rt.year, rt.month, rt.day, rt.hour, rt.minute, rt.second)


def _walk_mail_folders(folder, depth=0, max_depth=4):
    """Yield this folder and its subfolders (mail folders only)."""
    try:
        if folder.DefaultItemType == 0:          # olMailItem folders
            yield folder
    except Exception:
        pass
    if depth >= max_depth:
        return
    try:
        for sub in folder.Folders:
            yield from _walk_mail_folders(sub, depth + 1, max_depth)
    except Exception:
        pass


def all_mail_folders(ns, debug=False):
    """Every mail folder in the Outlook profile — Andy's own mailbox first,
    then other stores. Public Folders (thousands of shared folders) are
    skipped: the scores emails are never there and walking them takes ages."""
    out = []
    try:
        stores = list(ns.Stores)
    except Exception as e:
        log(f"  (stores unavailable: {e}); falling back to default inbox")
        return [ns.GetDefaultFolder(INBOX)]
    # own mailbox first so it is found quickly
    stores.sort(key=lambda st: 0 if SENDER_ADDRESS in
                str(getattr(st, "DisplayName", "")).lower() else 1)
    for store in stores:
        name = str(getattr(store, "DisplayName", "?"))
        if name.lower().startswith("public folders"):
            if debug:
                log(f"  store '{name}': skipped")
            continue
        try:
            folders = list(_walk_mail_folders(store.GetRootFolder()))
        except Exception as e:
            log(f"  (store {name} skipped: {e})")
            continue
        if debug:
            log(f"  store '{name}': {len(folders)} mail folder(s)")
        out.extend(folders)
    return out


def candidate_items(ns, since_hours, debug=False):
    cutoff = datetime.now() - timedelta(hours=since_hours)
    out, seen_ids = [], set()
    for folder in all_mail_folders(ns, debug=debug):
        name = folder.Name
        if name.lower() in ("deleted items", "junk email", "junk e-mail",
                            "sent items", "outbox", "drafts", "archive"):
            continue
        try:
            items = folder.Items
            items.Sort("[ReceivedTime]", True)
        except Exception:
            continue
        checked = 0
        for item in items:
            try:
                if getattr(item, "Class", 0) != 43:      # olMail only
                    continue
                received = _received_local(item)
                if received < cutoff:
                    break                                # sorted desc: done here
                checked += 1
                subject = str(item.Subject or "")
                if debug and checked <= 5:
                    log(f"    [{name}] {received:%m/%d %H:%M}  {subject[:90]}")
                if not TAG_RE.match(subject):
                    continue
                cats = str(item.Categories or "")
                if RELAYED_CATEGORY.lower() in cats.lower():
                    continue
                if item.EntryID in seen_ids:
                    continue
                seen_ids.add(item.EntryID)
                out.append(item)
            except Exception as e:
                log(f"  skip (unreadable item in {name}): {e}")
        if debug and checked:
            log(f"  folder '{name}': {checked} recent item(s) scanned")
    return out


def mark_relayed(item):
    cats = str(item.Categories or "").strip()
    item.Categories = f"{cats}; {RELAYED_CATEGORY}" if cats else RELAYED_CATEGORY
    item.Save()


def relay(app, ns, item, test_to=None, dry_run=False):
    m = TAG_RE.match(str(item.Subject))
    rep_addr, clean_subject = m.group(1).strip(), m.group(2).strip()
    to_addr = test_to or rep_addr
    cc_addr = "" if test_to else CC_FOR.get(rep_addr.lower(), "")

    log(f"  -> {to_addr}{' cc ' + cc_addr if cc_addr else ''} | {clean_subject}"
        + ("   [DRY RUN]" if dry_run else ""))
    if dry_run:
        return

    mail = app.CreateItem(0)
    acct = sending_account(ns)
    if acct is not None:
        try:
            mail._oleobj_.Invoke(*(64209, 0, 8, 0, acct))   # SendUsingAccount
        except Exception:
            try:
                mail.SendUsingAccount = acct
            except Exception:
                pass                                        # default account
    mail.To = to_addr
    if cc_addr:
        mail.CC = cc_addr
    mail.Subject = clean_subject
    mail.HTMLBody = item.HTMLBody          # identical body, no FW: header block
    mail.Send()
    mark_relayed(item)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--test-to", metavar="EMAIL",
                    help="send the relays to this address instead of the reps")
    ap.add_argument("--mark-existing", action="store_true",
                    help="tag all current matches as relayed without sending")
    ap.add_argument("--since-hours", type=float, default=36)
    ap.add_argument("--debug", action="store_true",
                    help="list accounts, folders and recent subjects scanned")
    args = ap.parse_args()

    log(f"Scores relay starting (script version {SCRIPT_VERSION})")
    app, ns = outlook_session()
    if args.debug:
        try:
            for acct in ns.Accounts:
                log(f"  account: {acct.SmtpAddress}")
            for store in ns.Stores:
                log(f"  store:   {store.DisplayName}")
        except Exception as e:
            log(f"  (could not list accounts: {e})")
    items = candidate_items(ns, args.since_hours, debug=args.debug)
    log(f"{len(items)} tagged scores email(s) received in the last "
        f"{args.since_hours:g}h not yet relayed")

    if args.mark_existing:
        for item in items:
            mark_relayed(item)
            log(f"  marked (not sent): {item.Subject}")
        log("Done (mark-existing)")
        return

    sent = 0
    for item in items:
        try:
            relay(app, ns, item, test_to=args.test_to, dry_run=args.dry_run)
            sent += 1
        except Exception as e:
            log(f"  ERROR relaying '{item.Subject}': {e}")
    log(f"Done — {sent} relayed" + (" (dry run)" if args.dry_run else ""))


if __name__ == "__main__":
    main()
