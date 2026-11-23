@echo off
REM Jalankan Worker. Master dicari otomatis lewat UDP broadcast.
REM Ganti WORKER-01 sesuai nomor PC dan --cores sesuai jumlah core fisik.
REM Jika jaringan memblokir UDP, tambahkan: --master-ip <IP_MASTER>
python worker.py --id WORKER-01 --cores 4
pause
