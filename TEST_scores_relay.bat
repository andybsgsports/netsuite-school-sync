@echo off
rem Double-click to test: fetches the latest script, then re-sends today's
rem tagged scores emails to andy@bsgsports.com (NOT to the reps), from
rem andy@bsgsports.com. Window stays open so you can read the result.
set PYTHONUTF8=1
cd /d "C:\Users\andre\OneDrive - Badger Sporting Goods\Desktop\Illinois Contact List\Netsuite Contacts Sync"
echo Downloading latest relay script...
curl -sL -o relay_scores_outlook.py https://raw.githubusercontent.com/andybsgsports/netsuite-school-sync/master/relay_scores_outlook.py
echo.
python relay_scores_outlook.py --test-to andy@bsgsports.com --since-hours 48 --debug
echo.
echo ---- finished. Take a screenshot of this window if anything looks wrong. ----
pause
