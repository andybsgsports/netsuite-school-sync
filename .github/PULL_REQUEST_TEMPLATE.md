## What & why

<!-- What's broken or missing, and what this PR does about it. Link to a
     GitHub Actions run or sheet/NetSuite evidence if this is a bug fix. -->

## Change

<!-- Summary of the actual code change. If this touches sync/heal logic,
     say which entry point(s) it affects: nightly push, full daily sync,
     IHSA sync, a manual workflow, etc. -->

## Verification

<!-- How you confirmed this works. For NetSuite/Sheets logic, offline
     fixtures with stubbed API calls are usually the only option in CI —
     say what cases you covered. For anything you ran live, link the run. -->

- [ ] `python -m py_compile` (or equivalent) passes on changed files
- [ ] Secret scan (gitleaks) passes — no credentials in the diff
- [ ] If this writes to NetSuite/the sheet: defaults to dry-run, requires
      an explicit `LIVE=1`/`--live` flag to apply

## Notes for reviewer

<!-- Anything risky, any follow-up needed, anything intentionally left
     out of scope. -->
