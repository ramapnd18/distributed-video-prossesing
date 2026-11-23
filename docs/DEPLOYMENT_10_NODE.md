# Deployment 10 Node Fisik (1 Master + 9 Worker)

Panduan ini untuk pengujian wajib UAS: **10 node fisik** dalam satu LAN/WLAN.

## 1. Topologi

```
                         +---------------------------+
                         |   MASTER (192.168.1.10)   |
                         |   master.py --workers 9   |
                         |   TCP 5050 + UDP 9999     |
                         |   + Worker lokal (default)|
                         +-------------+-------------+
                                       |
              +------------------------+------------------------+
              |            |           |           |            |
        +-----+----+ +-----+----+ +----+-----+ +---+------+ +---+------+
        | WORKER-01| | WORKER-02| | WORKER-03| | WORKER-04| |  ...     |
        | .11      | | .12      | | .13      | | .14      | |          |
        +----------+ +----------+ +----------+ +----------+ +----------+
              ...          ...          ...          ...
        +----------+ +----------+ +----------+ +----------+ +----------+
        | WORKER-05| | WORKER-06| | WORKER-07| | WORKER-08| | WORKER-09|
        | .15      | | .16      | | .17      | | .18      | | .19      |
        +----------+ +----------+ +----------+ +----------+ +----------+
```

Semua node harus berada di subnet yang sama, saling `ping`, port TCP 5050 dan
UDP 9999 Master terbuka. UDP 9999 hanya dipakai auto-discovery; jika jaringan
kampus memblokir broadcast, Worker tetap bisa memakai `--master-ip` manual.

## 2. Rencana IP (contoh)

| Node | Hostname | IP statis | Peran | Core |
|---|---|---|---|---|
| Master | MASTER | 192.168.1.10 | Server, queue, LB, assembler, Worker lokal | semua core |
| Worker 1 | WORKER-01 | 192.168.1.11 | Worker | 4 |
| Worker 2 | WORKER-02 | 192.168.1.12 | Worker | 4 |
| Worker 3 | WORKER-03 | 192.168.1.13 | Worker | 4 |
| Worker 4 | WORKER-04 | 192.168.1.14 | Worker | 4 |
| Worker 5 | WORKER-05 | 192.168.1.15 | Worker | 4 |
| Worker 6 | WORKER-06 | 192.168.1.16 | Worker | 4 |
| Worker 7 | WORKER-07 | 192.168.1.17 | Worker | 4 |
| Worker 8 | WORKER-08 | 192.168.1.18 | Worker | 4 |
| Worker 9 | WORKER-09 | 192.168.1.19 | Worker | 4 |

Sesuaikan jumlah core dengan spesifikasi asli tiap PC. Jangan mengarang data
spesifikasi di laporan.

## 3. Instalasi di semua node

```bash
git clone <repo-url> distributed-video-processing
cd distributed-video-processing
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/macOS
source .venv/bin/activate
pip install -r requirements.txt
```

Salin video uji ke Master: `data/input_video.mp4`.
Worker **tidak** perlu video; Worker hanya menerima byte frame dari Master.

## 4. Konfigurasi jaringan

### Windows (Master)

```powershell
# Buka TCP 5050 (task/result) + UDP 9999 (auto-discovery)
New-NetFirewallRule -DisplayName "DVP Master 5050" `
  -Direction Inbound -Protocol TCP -LocalPort 5050 -Action Allow
New-NetFirewallRule -DisplayName "DVP Discovery 9999" `
  -Direction Inbound -Protocol UDP -LocalPort 9999 -Action Allow

# Cek IP
ipconfig
```

### Linux (Master)

```bash
sudo ufw allow 5050/tcp
sudo ufw allow 9999/udp
ip addr show
```

### Cek konektivitas dari setiap Worker

```bash
ping 192.168.1.10
# Windows
Test-NetConnection 192.168.1.10 -Port 5050
```

## 5. Urutan menjalankan

### Master

```bash
python master.py
```

Perintah itu memakai default: `data/input_video.mp4`, output
`data/output_video.mp4`, `--workers 9`, dan konfirmasi mulai. Untuk eksperimen
dengan resolusi/tag khusus, tambahkan flag yang perlu saja, misalnya:

```bash
python master.py --target-res 1920x1080 --tag uas-dynamic \
  --scenario-name Distributed_Dynamic_9W --log-lb
```

Master tidak menunggu 9 Worker. Setelah cache siap, Master menampilkan jumlah
Worker yang sudah terdaftar dan bertanya:

```text
[START] Mulai pemrosesan sekarang? [y] mulai, [n] tunggu Worker lain, [q] batal:
```

Ketik `y` untuk mulai segera, `n` untuk menunggu Worker lain (ditanya lagi
setelah 10 detik atau saat ada Worker baru), atau `q` untuk membatalkan.
Dengan `--no-confirm-start`, Master baru mulai setelah 9 Worker terdaftar atau
`--startup-timeout` habis (dipakai skrip otomatis).

Tanpa `--batch-size`, ukuran task dihitung otomatis dari total core. Jika ingin
task lebih besar (mengurangi overhead jaringan), pakai `--chunks-per-core 2`
atau `--task-frames <n>`; `--batch-size` hanya batas atas.

Master ikut komputasi sebagai `MASTER-LOCAL` secara default. Untuk membuat
Master murni koordinator (mis. agar perbandingan node bersih), tambahkan
`--no-local-worker`.

Untuk membandingkan static vs dynamic, jalankan juga dengan `--lb static`
dan tag/`--scenario-name` yang sesuai. Agar Speedup terisi otomatis, buat
baseline sequential lebih dulu dengan tag, frame limit, dan resolusi yang sama:

```bash
python benchmark.py --mode sequential --tag uas-dynamic
```

Jika baseline memakai tag lain, isi `--baseline-time <T1_detik>` secara eksplisit.

Master akan menampilkan:

```
[DISCOVER] Auto-discovery aktif di UDP 9999. IP Master: 192.168.1.10
[MASTER] Mendengarkan koneksi di 0.0.0.0:5050 (Menunggu 9 Worker remote, LB=DYNAMIC).
[LOCAL] Master ikut komputasi sebagai MASTER-LOCAL (8 core, is_local=True).
```

### 9 Worker (di masing-masing PC)

```bash
python worker.py --id WORKER-01 --cores 4
python worker.py --id WORKER-02 --cores 4
...
python worker.py --id WORKER-09 --cores 4
```

Worker menemukan Master otomatis. Jika UDP diblokir, isi manual:

```bash
python worker.py --master-ip 192.168.1.10 --port 5050 --id WORKER-01 --cores 4
```

Jalankan Worker **setelah** Master listen, atau bersamaan. Worker mengulang
pencarian Master (UDP/TCP sweep) dan koneksi sampai `--connect-timeout`
(default 60 detik), jadi Worker yang menyala lebih dulu tetap akan bergabung.

Master mulai memproses setelah 9 Worker remote terdaftar (`[9/9]`) atau setelah
`--startup-timeout` (default 120s). Dengan `--confirm-start`, Master mulai
segera setelah Anda mengetik `y`. Jika ada Worker yang gagal mendaftar, Master
tetap mulai dengan Worker yang ada (tercatat di kolom `notes`).

## 6. Verifikasi saat berjalan

Di Master:

```
[REGISTER] WORKER-01 bergabung (4 cores, 8.0 GB RAM) [1/9]
...
[DECOMPOSE] Total data: 600 frame | Total core: 36 core | Ukuran task: 5 frame | Jumlah task: 120
[MASTER] Pemrosesan dimulai (DYNAMIC LB) - seluruh 9 Worker remote terdaftar.
[OK] Task #000 <- WORKER-03 (1.24s) | Selesai: 1/120
```

Di Worker:

```
[WORKER-01] Memproses Task #004 (5 frame) di 4 core...
[WORKER-01] Task #004 terkirim (1.18s, 5 frame).
```

Di akhir, Master menulis:

- `data/output_video.mp4`
- `logs/experiment_results.csv`
- `logs/distribution_<tag>_<timestamp>.csv`
- `logs/task_timeline_<tag>_<timestamp>.csv`
- `logs/lb_scores_<tag>_<timestamp>.csv`

### Dynamic LB di lapangan

Pada mode dynamic, Master menghitung capacity score tiap Worker:

```
score = cores * (1 - 0.60*cpu/100) * (1 - 0.40*ram/100)
```

Worker dengan skor tinggi dapat mengambil beberapa task per round-trip. Jumlahnya
dihitung otomatis: `ceil(core_worker / frame_per_task)` dibatasi
`--max-tasks-per-assignment` (default 4), selama antrean masih cukup untuk
Worker lain. Contoh: Worker 4 core dengan task 5 frame mengambil 1 task;
Master 8 core dengan task 5 frame mengambil 2 task. Karena itu, PC dengan core
lebih banyak wajar menerima task lebih banyak. Bandingkan kolom `tasks_done` di
`distribution_*.csv` dan riwayat skor di `lb_scores_*.csv`; jangan memaksa
distribusi rata seperti mode static.

### Simulasi sebelum ke lab

Gunakan `simulate_cluster.py` (default 9 Worker x 1 core) atau
`scripts/local_cluster.py` untuk memastikan konfigurasi, video, dan tag sudah
benar sebelum pindah ke 10 PC fisik. Hasil simulasi **bukan** pengganti
demonstrasi 10 node fisik.

## 7. Checklist 10 node

- [ ] 1 Master + 9 Worker fisik menyala dan terhubung LAN/WLAN.
- [ ] Semua node `ping` Master.
- [ ] Port TCP 5050 dan UDP 9999 terbuka di firewall Master.
- [ ] Python 3.10+ dan `requirements.txt` terpasang di semua node.
- [ ] Video uji ada di Master (`data/input_video.mp4`).
- [ ] Master dijalankan dengan `--workers 9`.
- [ ] Semua 9 Worker muncul di log registrasi `[9/9]` (auto-discovery atau manual).
- [ ] Kolom `nodes_total` di CSV = 10 (9 remote + Master lokal).
- [ ] Tag, frame limit, dan resolusi baseline T1 sama dengan run distributed.
- [ ] Output video berisi jumlah frame yang sama dengan input (`lost_frames=0`).
- [ ] CSV metrik, timeline, distribusi, dan `lb_scores` tersimpan.
- [ ] Rekaman video demonstrasi memperlihatkan 10 layar/node dan proses berjalan.
- [ ] Video demonstrasi maksimal 5 menit, format `.mp4`.

## 8. Simulasi fault tolerance 10 node

Saat pemrosesan berjalan, matikan paksa 1-2 Worker:

- Tutup terminal Worker dengan `Ctrl+C`, atau
- `taskkill /F /PID <pid>` (Windows), atau
- cabut koneksi Wi-Fi/kabel.

Master akan menampilkan:

```
[FAULT TOLERANCE] Worker 'WORKER-04' terdeteksi MATI (...). 3 task di-requeue.
[FAULT TOLERANCE] Task #021 dikembalikan ke antrean (attempt 1/3): ...
[OK] Task #021 <- WORKER-07 (1.02s) | Selesai: 87/120
```

Nilai yang dicatat:

- `recovery_time_sec` = waktu dari deteksi sampai task hasil requeue diambil
  Worker sehat.
- `requeued_tasks` = jumlah task yang dikembalikan.
- `lost_frames` harus tetap 0.

## 9. Tips rekaman demonstrasi (maks. 5 menit)

1. (0:00-0:30) Tampilkan topologi dan 10 node menyala, `ipconfig`/hostname.
2. (0:30-1:00) Jalankan Master, tunjukkan 9 Worker terdaftar.
3. (1:00-2:30) Tunjukkan progress task dan CPU task manager di beberapa Worker.
4. (2:30-3:30) Matikan 1-2 Worker, tunjukkan requeue dan task lanjut.
5. (3:30-4:30) Tunjukkan video output, CSV metrik, dan tabel speedup.
6. (4:30-5:00) Ringkasan hasil.

## 10. Troubleshooting lapangan

| Gejala | Penyebab umum | Solusi |
|---|---|---|
| Worker stuck "Menunggu Master" | IP/port salah, firewall, UDP diblokir | Cek `ping`, `Test-NetConnection`, buka TCP 5050 + UDP 9999; atau pakai `--master-ip`. |
| Worker tidak ditemukan (auto) | Broadcast UDP diblokir AP/Wi-Fi | Pakai `--master-ip 192.168.1.10` manual. |
| Master hanya melihat 8/9 | Satu Worker crash/salah ID | Cek log Worker; restart. |
| Worker dianggap mati | Heartbeat > timeout | Naikkan `--hb-timeout` di Master, cek Wi-Fi. |
| Task timeout berulang | Task terlalu besar / Worker lambat | Naikkan `--task-timeout` atau perkecil `--task-frames`. |
| Transfer lambat | Wi-Fi 2.4 GHz / task terlalu kecil | Pakai kabel/5 GHz, turunkan `--chunks-per-core` (mis. 2). |
| RAM Master penuh | Video sangat besar | Cache disk otomatis; jangan hapus `data/frames_cache` saat run. |
| Output tidak bisa diputar | Codec `mp4v` tidak tersedia | Install OpenCV lengkap / pakai VLC. |
