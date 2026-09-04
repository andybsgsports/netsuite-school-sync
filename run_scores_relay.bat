@echo off
set PYTHONUTF8=1
cd /d "C:\Users\andre\OneDrive - Badger Sporting Goods\Desktop\Illinois Contact List\Netsuite Contacts Sync"
echo [%date% %time%] Starting scores relay... >> relay_log.txt
python relay_scores_outlook.py >> relay_log.txt 2>&1
echo [%date% %time%] Scores relay finished. >> relay_log.txt
