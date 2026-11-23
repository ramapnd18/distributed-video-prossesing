@echo off
REM Contoh menjalankan Master untuk 9 Worker fisik.
REM Ubah --workers sesuai jumlah Worker yang benar-benar terhubung.
python master.py --workers 9 --lb dynamic --tag uas --scenario-name Distributed_Dynamic_9W --log-lb
pause
