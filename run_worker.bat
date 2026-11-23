@echo off
REM Contoh menjalankan Worker. Ganti MASTER_IP dengan IP LAN komputer Master.
set MASTER_IP=192.168.1.10
python worker.py --master-ip %MASTER_IP% --port 5050 --id WORKER-01 --cores 4
pause
