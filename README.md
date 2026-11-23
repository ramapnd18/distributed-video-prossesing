# Distributed Video Processing - Master/Worker

Sistem komputasi terdistribusi untuk **batch frame extraction & multi-stage video filtering**
(resize -> grayscale -> Gaussian blur -> Canny edge + bounding box) dengan arsitektur
**1 Master + N Worker** di jaringan lokal.

Proyek UAS Komputasi Paralel dan Sistem Terdistribusi.

## Fitur yang sudah diimplementasikan

| Mekanisme wajib | Implementasi |
|---|---|
| Task Decomposition | Video dipecah menjadi batch 30-50 frame (`task_id`, `start_frame`, `end_frame`). |
| Parallel Processing | Worker memakai `ProcessPoolExecutor` persisten di seluruh core CPU. |
| Distributed Processing | TCP socket + header 4 byte + `pickle` + `zlib` (tanpa Celery/Ray/gRPC). |
| Dynamic Load Balancing | Pull-based + **capacity score** (core, CPU, RAM) + prefetch task untuk Worker berkapasitas tinggi. |
| Static Load Balancing | `--lb static`: task dibagi rata di awal untuk pembanding. |
| Fault Tolerance | Heartbeat 2 detik di socket terpisah, timeout 6 detik, requeue task `IN_PROGRESS`, deteksi socket putus. |
| Recovery Time | Diukur dari deteksi kegagalan sampai task hasil requeue diambil Worker sehat. |
| Performance Evaluation | CSV otomatis: execution time, throughput, speedup, efficiency, CPU/RAM, recovery, distribusi task, frame hilang. |
| Video Reconstruction | Hasil batch diurutkan berdasarkan `task_id` lalu ditulis ulang ke `.mp4`. |

### Keputusan desain penting

- **Frame cache di disk.** Master tidak menyimpan seluruh video di RAM. Frame diekstrak
  sekali ke `data/frames_cache/*.jpg`, lalu task hanya menyimpan metadata. Video 1080p
  2 menit (7200 frame) akan memakan ~40 GB jika disimpan sebagai array mentah.
- **Transfer hemat bandwidth.** Frame input dikirim sebagai **JPEG** (kualitas 90),
  frame hasil sebagai **PNG lossless**. Keduanya tetap dibungkus `zlib`.
- **Heartbeat di socket terpisah.** Heartbeat tidak terblokir saat Worker mengirim
  hasil batch besar; menghindari false-positive "Worker mati".
- **Worker pool persisten.** Process pool dibuat sekali per Worker, bukan per batch.
  IPC antar-process hanya membawa byte JPEG/PNG terkompresi, bukan array 1080p.
- **Requeue aman.** Task yang di-requeue dibaca ulang dari cache, jadi tidak ada
  ketergantungan pada state memori Worker yang mati.
- **Dynamic LB sadar kapasitas.** Skor dihitung dari `cores`, `cpu_usage`, dan
  `ram_usage`:
  `score = cores * (1 - 0.60*cpu/100) * (1 - 0.40*ram/100)`.
  Worker dengan skor >= 75% skor tertinggi boleh mengambil maksimal
  `--max-tasks-per-assignment` batch per round-trip (default 2), selama antrean
  masih cukup untuk Worker lain. Riwayat skor ditulis ke `logs/lb_scores_*.csv`
  (selalu) dan detail assignment dicetak jika `--log-lb`.

## Struktur direktori

```
.
├── config.py                  # IP, port, batch, resolusi, heartbeat, path, prefetch
├── protocol.py                # TCP packet: header + pickle + zlib
├── processor.py               # Pipeline OpenCV + multiprocessing
├── master.py                  # Server, queue, LB, fault tolerance, assembler, metrik
├── worker.py                  # Client, heartbeat, ProcessPoolExecutor
├── benchmark.py               # Sequential (T1) & Local Parallel
├── simulate_cluster.py        # Alias lokal, default 9 Worker x 1 core
├── requirements.txt
├── run_master.bat             # Helper Windows
├── run_worker.bat
├── run_smoke_test.bat
├── data/
│   ├── input_video.mp4        # Video sumber (buat/unduh sendiri)
│   ├── SOURCES.txt            # Tautan & lisensi video
│   └── frames_cache/          # Cache JPEG (otomatis)
├── logs/
│   ├── experiment_results.csv # Data mentah untuk BAB IV
│   ├── distribution_*.csv     # Distribusi & skor kapasitas per Worker
│   ├── task_timeline_*.csv    # Timeline task (bisa dibuat Gantt chart)
│   ├── lb_scores_*.csv        # Riwayat capacity score
│   └── runs/                  # Log stdout Master/Worker dari local_cluster
├── scripts/
│   ├── make_sample_video.py   # Video sampel sintetis
│   ├── local_cluster.py       # 1 Master + N Worker di satu PC (dev)
│   └── smoke_test.py          # Uji end-to-end dynamic/static/fault
├── utils/
│   ├── frame_codec.py         # JPEG/PNG encode-decode
│   ├── metrics_logger.py      # CSV logger + sampler CPU/RAM
│   └── video_utils.py         # Cache frame, task, rekonstruksi video
└── docs/
    ├── DEPLOYMENT_10_NODE.md  # Panduan 10 node fisik
    └── EXPERIMENTS.md         # Matriks 6 skenario + cara analisis
```

## Instalasi

Prasyarat: **Python 3.10+**.

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/macOS
source .venv/bin/activate

pip install -r requirements.txt
```

Cek:

```bash
python -c "import cv2, numpy, psutil; print(cv2.__version__, numpy.__version__, psutil.__version__)"
```

## Quick start (satu komputer, untuk development)

```bash
# 1. Buat video sampel 20 detik 1280x720 (jika belum ada)
python scripts/make_sample_video.py

# 2. Uji end-to-end: dynamic LB, static LB, fault tolerance
python scripts/smoke_test.py

# 3. Lihat tabel metrik
python benchmark.py --report
```

Simulasi 1 Master + 9 Worker lokal (default `simulate_cluster.py`):

```bash
python simulate_cluster.py --video data/input_smoke.mp4 \
  --frame-limit 90 --target-res 640x360 --tag sim9
```

Atau jalankan cluster lokal manual dengan jumlah Worker tertentu:

```bash
python scripts/local_cluster.py --workers 2 --lb dynamic \
  --video data/input_video.mp4 --output data/output_video.mp4 \
  --batch-size 30 --target-res 1280x720 --cores-per-worker 4 \
  --tag dev
```

> `local_cluster.py` / `simulate_cluster.py` hanya untuk development. Pengujian
> UAS wajib memakai 10 node fisik (1 Master + 9 Worker) sesuai
> `docs/DEPLOYMENT_10_NODE.md`.

## Menjalankan 10 node fisik

Di komputer **Master**:

```bash
python master.py --workers 9 --lb dynamic \
  --video data/input_video.mp4 --output data/output_video.mp4 \
  --batch-size 30 --target-res 1920x1080 \
  --tag uas --scenario-name Distributed_Dynamic_9W \
  --csv logs/experiment_results.csv --log-lb
```

Di setiap komputer **Worker** (9 PC):

```bash
python worker.py --master-ip 192.168.1.10 --port 5050 \
  --id WORKER-01 --cores 4
```

Ganti `--id` menjadi `WORKER-01` ... `WORKER-09` dan `--cores` sesuai jumlah
core fisik tiap PC. Pastikan port `5050` dibuka di firewall Master.

Master baru mulai memproses setelah jumlah Worker terdaftar = `--workers`.
Gunakan `--startup-timeout 120` (default) agar Master tidak menunggu selamanya.

## Mode pengujian & perintah

| Skenario | Perintah |
|---|---|
| Sequential (T1) | `python benchmark.py --mode sequential --tag uas` |
| Local Parallel | `python benchmark.py --mode local-parallel --cores 4 --tag uas` |
| Distributed Static | `python master.py --workers 9 --lb static --tag uas` + 9 Worker |
| Distributed Dynamic | `python master.py --workers 9 --lb dynamic --tag uas` + 9 Worker |
| Scalability | Ulangi dynamic dengan `--workers 1/2/4/6/9`, tag berbeda per jumlah Worker |
| Fault Tolerance | Jalankan 9 Worker, lalu `Ctrl+C` 1-2 Worker di tengah proses |

Contoh uji fault tolerance otomatis di local cluster:

```bash
python scripts/local_cluster.py --workers 2 --lb dynamic \
  --crash-worker-index 0 --crash-on-task 1 --tag fault-test
```

Worker akan `os._exit(137)` saat menerima task ke-1. Master mendeteksi socket
putus, me-requeue task, dan task diselesaikan Worker lain. Cek kolom
`recovery_time_sec`, `requeued_tasks`, dan `lost_frames` di CSV.

### Baseline T1 dan tag eksperimen

Speedup hanya dihitung jika ada baseline sequential dengan **tag, jumlah frame,
dan resolusi target yang sama**. Jadi jalankan baseline lebih dulu:

```bash
python benchmark.py --mode sequential --tag uas
python benchmark.py --mode local-parallel --cores 4 --tag uas
python scripts/local_cluster.py --workers 9 --lb dynamic --tag uas ...
python benchmark.py --report --tag uas
```

Jika ingin memakai baseline lain, isi eksplisit:

```bash
python master.py --workers 9 --lb dynamic --baseline-time 123.45
```

## Metrik

Rumus yang dipakai:

```
Speedup          S_p = T1 / Tp
Efficiency/core  E_core = (S_p / total_core) x 100%
Efficiency/node  E_node = (S_p / jumlah_worker) x 100%
Throughput       = total_frame / Tp              (frame/detik)
Recovery Time    = T_requeue_diambil - T_failure_detected
```

`E_core` adalah efisiensi hardware dan dipakai sebagai `efficiency_pct`.
`E_node` disimpan sebagai `node_efficiency_pct`; nilainya bisa >100% karena
setiap Worker node memakai beberapa core. Untuk perbandingan antar jumlah node,
gunakan `E_node`; untuk melihat seberapa efisien seluruh core, gunakan `E_core`.

Kolom CSV utama (`logs/experiment_results.csv`):

```
timestamp, tag, scenario, scenario_name, status, lb_mode, workers,
num_workers, workers_seen, cores_per_worker, total_cores, batch_size,
target_res, total_frames, exec_time_sec, baseline_t1_sec, throughput_fps,
speedup, efficiency_pct, node_efficiency_pct, avg_cpu_pct, max_cpu_pct,
avg_ram_pct, max_ram_pct, recovery_time_sec, requeued_tasks,
failed_tasks, lost_frames, worker_task_distribution, hostname, cpu_cores, notes
```

- `exec_time_sec` Master = dari semua Worker siap sampai hasil terakhir diterima.
  Waktu ekstraksi cache dan rekonstruksi video **tidak** termasuk.
- Benchmark sequential/local parallel memakai cache frame yang sama, warmup di
  luar timer, dan decode JPEG + pipeline + encode PNG termasuk dalam timer.
- `avg_cpu_pct` / `avg_ram_pct` berasal dari telemetri heartbeat Worker.
  Jika run terlalu singkat, dipakai sampler lokal sebagai fallback.
- `worker_task_distribution` berformat `WORKER-01:12|WORKER-02:14|...`.
- `lost_frames` harus **0** untuk validasi integritas hasil.
- File pendukung: `distribution_*.csv` (task & skor per Worker),
  `task_timeline_*.csv` (Gantt), `lb_scores_*.csv` (riwayat capacity score).

## Simulasi kegagalan

1. **Crash terkontrol (otomatis):** `worker.py --crash-on-task N`.
2. **Manual:** tekan `Ctrl+C` pada terminal Worker di tengah pemrosesan.
3. **Putus jaringan:** nonaktifkan Wi-Fi/kabel Worker; Master akan mendeteksi
   timeout heartbeat (default 6 detik).
4. **Matikan proses paksa:** `taskkill /F /PID <pid>` (Windows) atau `kill -9`.

Master akan menandai Worker `DEAD`, mencatat `death_reason`, dan mengembalikan
task `IN_PROGRESS` (serta sisa antrean static) ke `PENDING`.

## Troubleshooting

| Masalah | Solusi |
|---|---|
| `Video input tidak ditemukan` | `python scripts/make_sample_video.py` atau isi `--video`. |
| Worker tidak bisa connect | Cek IP Master, `ping`, firewall port 5050, dan `--bind 0.0.0.0`. |
| Master menunggu terus | Jumlah Worker kurang dari `--workers`; cek log registrasi. |
| Worker dianggap mati padahal hidup | Interval heartbeat harus < `--hb-timeout`; cek jaringan. |
| `Address already in use` | Ganti `--port`, atau hentikan proses Master lama. |
| Multiprocessing error di Windows | Jalankan dari root repo; jangan hapus guard `if __name__ == "__main__"`. |
| Output video 0 frame | Cek `lost_frames`, `failed_tasks`, dan log Worker. |
| Speedup kosong | Baseline sequential belum ada untuk tag/frame/resolusi yang sama; jalankan `benchmark.py --mode sequential --tag ...` atau isi `--baseline-time`. |
| Cache frame besar | Hapus `data/frames_cache/` atau pakai `--frame-limit` saat uji. |

## Batasan

- `pickle` hanya aman untuk LAN tepercaya; jangan expose port Master ke internet.
- Video output memakai codec `mp4v`; pastikan OpenCV build mendukungnya.
- Pengujian "9 Worker" di satu PC hanya simulasi; laporan UAS tetap harus
  menunjukkan 10 node fisik pada video demonstrasi.
- Dynamic LB memakai skor kapasitas heuristik dan prefetch terbatas; belum ada
  work stealing penuh atau migrasi task yang sudah berjalan.
