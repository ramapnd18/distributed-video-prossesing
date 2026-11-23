@echo off
REM Jalankan Master dengan pengaturan default (video & CSV bawaan).
REM Setelah siap, ketik `y` untuk mulai tanpa menunggu jumlah Worker.
REM Opsi lain misalnya: python master.py --workers 9 --lb static
python master.py
pause
