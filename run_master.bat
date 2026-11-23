@echo off
REM Contoh menjalankan Master untuk 9 Worker remote (+ Master lokal sebagai Worker).
REM Ubah --workers sesuai jumlah Worker yang benar-benar terhubung.
REM Tambahkan --no-local-worker jika Master harus murni koordinator.
python master.py --workers 9 --lb dynamic --chunks-per-core 4 --task-timeout 45 --tag uas --scenario-name Distributed_Dynamic_9W --log-lb
pause
