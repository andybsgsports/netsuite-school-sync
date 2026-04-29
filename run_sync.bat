@echo off
cd /d "C:\Users\andre\OneDrive - Badger Sporting Goods\Desktop\Illinois Contact List\Netsuite Contacts Sync"
echo [%date% %time%] Starting sync... >> sync_log.txt
python school_netsuite_sync.py >> sync_log.txt 2>&1
echo [%date% %time%] Sync finished. >> sync_log.txt
