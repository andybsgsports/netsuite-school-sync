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


def candidate_items(ns, since_hours):
    inbox = ns.GetDefaultFolder(INBOX)
    items = inbox.Items
    items.Sort("[ReceivedTime]", True)
    cutoff = datetime.now() - timedelta(hours=since_hours)
    # Outlook Restrict wants US-style date text
    restricted = items.Restrict(f"[ReceivedTime] >= '{cutoff:%m/%d/%Y %I:%M %p}'")
    out = []
    for item in restricted:
        try:
            if getattr(item, "Class", 0) != 43:      # olMail only
                continue
            subject = str(item.Subject or "")
            if not TAG_RE.match(subject):
                continue
            cats = str(item.Categories or "")
            if RELAYED_CATEGORY.lower() in cats.lower():
                continue
            out.append(item)
        except Exception as e:
            log(f"  skip (unreadable item): {e}")
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
    args = ap.parse_args()

    log("Scores relay starting")
    app, ns = outlook_session()
    items = candidate_items(ns, args.since_hours)
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
