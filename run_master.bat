@echo off
REM Contoh menjalankan Master untuk 9 Worker remote (+ Master lokal sebagai Worker).
REM --confirm-start: Master langsung siap; ketik `y` untuk mulai tanpa menunggu
REM seluruh Worker. Ketik `n` untuk menunggu Worker lain, `q` untuk batal.
REM Tambahkan --no-local-worker jika Master harus murni koordinator.
python master.py --workers 9 --confirm-start --lb dynamic --chunks-per-core 4 --task-timeout 45 --tag uas --scenario-name Distributed_Dynamic_9W --log-lb
pause
