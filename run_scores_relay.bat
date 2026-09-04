@echo off
rem Monday 8:00 AM (Task Scheduler). Fetches the latest script, relays the
rem tagged scores emails to the reps, logs everything to relay_log.txt.
set PYTHONUTF8=1
cd /d "C:\Users\andre\OneDrive - Badger Sporting Goods\Desktop\Illinois Contact List\Netsuite Contacts Sync"
echo [%date% %time%] Starting scores relay... >> relay_log.txt
curl -sL -o relay_scores_outlook.py https://raw.githubusercontent.com/andybsgsports/netsuite-school-sync/master/relay_scores_outlook.py >> relay_log.txt 2>&1
python relay_scores_outlook.py >> relay_log.txt 2>&1
echo [%date% %time%] Scores relay finished. >> relay_log.txt
