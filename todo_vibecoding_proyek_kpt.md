# TODO: Distributed Video Processing (Master-Worker Architecture)

> **Instruksi untuk AI / Vibecoding Agent:**
> Baca seluruh bagian **System Context & Constraints** sebelum menulis atau mengubah kode. Kerjakan tugas secara berurutan per fase. Setelah menyelesaikan satu item, ubah `[ ]` menjadi `[x]` dan pastikan seluruh skrip dapat dijalankan tanpa *syntax* maupun *runtime error*.

---

## System Context & Architectural Constraints

1. **Tujuan Proyek:** Aplikasi komputasi paralel & terdistribusi berbasis arsitektur **Master-Worker** menggunakan minimal **10 node fisik** (1 Master + 9 Worker) untuk mata kuliah *Komputasi Paralel dan Sistem Terdistribusi*.
2. **Tech Stack Wajib:** Python 3.10+, `opencv-python` (`cv2`), `numpy`, `psutil`, `concurrent.futures.ProcessPoolExecutor`, dan Python `socket` (TCP) murni dengan protokol `struct` + `pickle` + `zlib`. **Jangan** mengganti lapisan jaringan dengan library eksternal seperti Celery, Ray, atau gRPC.
3. **Struktur Direktori Target:**
   ```text
   distributed-video-processing/
   ├── config.py                # [DONE] Konfigurasi IP, port, batch size, timeout
   ├── protocol.py              # [DONE] Framing TCP (4-byte header) + kompresi zlib
   ├── processor.py             # [DONE] Pipeline OpenCV (Resize, Gray, Blur, Canny, Contours)
   ├── master.py                # [DONE] Server, Queue, LB, Heartbeat, Assembler
   ├── worker.py                # [DONE] Client, Heartbeat thread, ProcessPoolExecutor
   ├── discovery.py             # [DONE] UDP broadcast + TCP sweep auto-discovery Master
   ├── benchmark.py             # [DONE] Mode Sequential (T1) & Local Parallel
   ├── simulate_cluster.py      # [DONE] Simulasi 1 Master + N Worker di 1 PC untuk uji lokal
   ├── requirements.txt         # [DONE] Daftar dependensi Python
   ├── README.md                # [DONE] Panduan instalasi & perintah 6 skenario uji UAS
   ├── utils/
   │   ├── __init__.py          # [DONE] Package initializer
   │   └── metrics_logger.py    # [DONE] Kalkulator Speedup, Efficiency, Throughput & CSV Logger
   ├── data/
   │   ├── input_video.mp4      # Dataset video uji (1080p/720p)
   │   ├── SOURCES.txt          # [DONE] Sitasi sumber & lisensi resmi video sampel
   │   └── output_video.mp4     # Hasil rekonstruksi video
   └── logs/
       └── experiment_results.csv # Output otomatis seluruh hasil pengujian
   ```
4. **6 Metrik Evaluasi Wajib:**
   - Execution Time ($T_1$ untuk sekuensial, $T_p$ untuk paralel dengan $p$ Worker)
   - Speedup: $S_p = \frac{T_1}{T_p}$
   - Efficiency: $E_p = \frac{S_p}{p} \times 100\%$
   - Throughput: $\frac{\text{Total Frame}}{T_p}$ (FPS)
   - Rata-rata CPU & Memory Utilization (%)
   - Recovery Time ($T_{\text{requeue}} - T_{\text{failure\_detected}}$)

---

## Phase 1: Project Setup & Critical Concurrency Bugfixes

- [x] **1.1 Buat file dependensi & dokumentasi sumber aset**
  - Buat `requirements.txt` berisi `opencv-python`, `numpy`, dan `psutil`.
  - Buat `data/SOURCES.txt` sebagai template pencatatan URL resmi, nama pembuat, resolusi, FPS, dan lisensi video sampel (misal *Big Buck Bunny* atau *Pexels*).
  - Buat `utils/__init__.py` agar folder `utils` terbaca sebagai modul Python.

- [x] **1.2 Perbaiki deadlock socket lock pada `worker.py`**
  - **Masalah saat ini:** Pada `worker.py`, `with self.sock_lock:` membungkus sekaligus `send_packet(sock, MSG_REQUEST_TASK, ...)` dan `recv_packet(sock)`. Saat Master menahan respons di `self.all_workers_ready.wait()`, `recv_packet` memblokir `sock_lock` sehingga `heartbeat_sender` gagal mengirim sinyal hidup dan Worker berisiko dianggap mati oleh Master.
  - **Solusi:** Gunakan `self.send_lock` terpisah khusus saat memanggil `send_packet()`, sedangkan `recv_packet()` dijalankan di luar *lock* karena hanya *main thread* yang membaca dari `sock`.
  - Pastikan Worker keluar dengan bersih (*exit code 0*) tanpa *traceback* saat menerima `MSG_SHUTDOWN` atau koneksi ditutup Master di akhir eksekusi.

---

## Phase 2: Metrics Logger & Local Benchmarking (`T1` Baseline)

- [x] **2.1 Implementasikan `utils/metrics_logger.py`**
  - Buat kelas/fungsi untuk mencatat hasil setiap eksperimen ke `logs/experiment_results.csv` (buat folder `logs/` otomatis jika belum ada).
  - Kolom wajib CSV: `timestamp`, `scenario_name`, `lb_mode`, `num_workers`, `cores_per_worker`, `batch_size`, `total_frames`, `exec_time_sec`, `baseline_t1_sec`, `speedup`, `efficiency_pct`, `throughput_fps`, `avg_cpu_pct`, `avg_ram_pct`, `recovery_time_sec`, `worker_task_distribution`.
  - Tambahkan fungsi helper `get_latest_baseline_t1(csv_path)` agar pengujian terdistribusi otomatis membaca nilai $T_1$ terakhir untuk menghitung `speedup` dan `efficiency_pct`.

- [x] **2.2 Implementasikan `benchmark.py` (Skenario 1 & Skenario 2)**
  - Tambahkan CLI flag `--mode` dengan pilihan `sequential` dan `local-parallel`, serta `--video` dan `--output`.
  - **Mode `sequential`:** Jalankan `process_batch_sequential()` pada seluruh *frame* video menggunakan 1 *core* CPU, ukur waktu eksekusi sebagai baseline $T_1$, rekam utilisasi CPU/RAM via `psutil`, simpan video hasil, dan catat ke `experiment_results.csv`.
  - **Mode `local-parallel`:** Jalankan `process_batch_parallel()` menggunakan seluruh *core* lokal pada 1 komputer tanpa TCP socket, hitung *Speedup* terhadap $T_1$, dan catat ke `experiment_results.csv`.

---

## Phase 3: Dynamic Load Balancing & Telemetry Enhancement di `master.py`

- [x] **3.1 Tingkatkan algoritma Dynamic Load Balancing berbasis kapabilitas & telemetri**
  - Saat ini `master.py` baru menggunakan *pull-based queue* sederhana. Tambahkan perhitungan **Worker Capacity Score** pada `master.py` menggunakan data registrasi (`cores`) dan telemetri `HEARTBEAT` (`cpu_usage`, `ram_usage`).
  - Pada mode `--lb dynamic`, jika terdapat antrean *task* atau variasi ukuran *chunk*, prioritaskan pemberian *task* dan log skor beban tiap Worker agar terlihat jelas perbedaannya dengan mode `--lb static` (pembagian rata di awal tanpa mempertimbangkan kecepatan/beban Worker).
  - Simpan riwayat sampel `cpu_usage` dan `ram_usage` dari setiap paket `HEARTBEAT` untuk menghitung rata-rata utilisasi CPU & RAM klaster di akhir eksekusi.

- [x] **3.2 Integrasikan `metrics_logger.py` ke `master.py`**
  - Di akhir metode `MasterServer.start()`, hitung total waktu eksekusi ($T_p$), *Throughput*, rata-rata CPU/RAM seluruh Worker, total *Recovery Time* (jika ada simulasi *fault tolerance*), dan distribusi jumlah *task* per Worker (misal format `W1:12|W2:14|...`).
  - Otomatis simpan ringkasan eksperimen tersebut ke `logs/experiment_results.csv` menggunakan `metrics_logger.py`.
  - Tambahkan flag opsional `--scenario-name` pada CLI `master.py` (misal `--scenario-name "Scalability_9_Workers"` atau `"Fault_Tolerance_Test"`) agar data di CSV mudah diidentifikasi saat menyusun BAB IV laporan.

---

## Phase 4: Local Multi-Node Simulation & Deployment Helpers

- [x] **4.1 Buat `simulate_cluster.py` untuk uji coba 10 node di satu laptop**
  - Buat skrip *launcher* menggunakan `subprocess.Popen` yang menjalankan 1 proses `master.py` dan $N$ proses `worker.py` (default `--workers 9`, dengan `--cores 1` atau `2` per Worker) di `127.0.0.1` agar tim bisa menguji alur penuh (termasuk mematikan 1 PID Worker di tengah jalan untuk uji *Fault Tolerance*) sebelum pindah ke 10 komputer fisik lab.
  - Pastikan semua *child process* dibersihkan saat skrip selesai atau ditekan `Ctrl+C`.

- [x] **4.2 Buat `README.md` sebagai panduan operasional tim**
  - Tuliskan langkah instalasi di 10 node fisik (konfigurasi IP statis/LAN, *firewall port* `5050`, cara cek IP Master).
  - Sediakan daftar perintah *copy-paste* siap pakai untuk ke-6 skenario pengujian UAS:
    1. Sequential (`T1`)
    2. Local Parallel
    3. Distributed Static LB (9 Workers)
    4. Distributed Dynamic LB (9 Workers)
    5. Scalability (`1, 2, 4, 6, 9` Workers)
    6. Fault Tolerance (simulasi mematikan 2 Worker saat berjalan)

---

## Phase 5: End-to-End Verification Checklist

- [x] **5.1 Validasi integritas output video:** Pastikan jumlah *frame* pada `data/output_video.mp4` **100% sama** dengan `data/input_video.mp4` baik pada eksekusi normal maupun setelah 2 Worker dimatikan paksa di tengah proses. *(Tervalidasi pada cluster lokal: 3/3 smoke test PASS, `lost_frames=0`; validasi 10 node fisik dilakukan saat demo.)*
- [x] **5.2 Validasi log CSV:** Pastikan `logs/experiment_results.csv` terisi rapi tanpa nilai `NaN` atau pembagian dengan nol (*ZeroDivisionError*).
- [ ] **5.3 Persiapan pengumpulan UAS:** Pastikan seluruh file siap dikompresi ke `.zip`/`.rar` dan siap didemonstrasikan dalam video berdurasi maksimal 5 menit pada 10 node fisik. *(Kode, dokumentasi, dan skrip sudah siap; rekaman demo 10 node fisik belum dibuat.)*

---

## Phase 6: Refactor Algoritma Mengikuti Referensi Praktikum

- [x] **6.1 Auto-discovery Master/Worker**
  - `discovery.py`: responder UDP `DISCOVER_MASTER` + `DISCOVER_WORKERS` (port 9999), `discover_master()` dengan fallback TCP sweep.
  - `worker.py`: `--master-ip auto` (default), flag `--discovery-port` / `--discovery-timeout`.
  - `master.py`: `--discovery/--no-discovery`, `--discovery-port`.

- [x] **6.2 Dekomposisi task dari jumlah core**
  - `task_frames = ceil(total_frame / (total_core x chunks_per_core))`, minimal 3 frame, maksimal `--batch-size`.
  - CLI baru: `--task-frames`, `--chunks-per-core`, `--min-task-frames`.

- [x] **6.3 Master ikut komputasi (gaya referensi `is_local`)**
  - `MASTER-LOCAL` terdaftar sebagai Worker; semua frame dari task yang di-prefetch diproses dalam satu `pool.map` agar seluruh core terpakai.
  - CLI: `--local-worker/--no-local-worker` (default aktif), `--local-cores`.

- [x] **6.4 Timeout per task + retry**
  - Task yang berjalan > `--task-timeout` (default 45 detik) memicu Worker ditandai mati dan task di-requeue (maks. 3 percobaan).

- [x] **6.5 Validasi**
  - Compile 15 file Python OK.
  - `scripts/smoke_test.py`: 3/3 PASS, `lost_frames=0`.
  - Master-only (`--workers 0`), dynamic 2 Worker, static + local worker, dan auto-discovery (UDP) diuji end-to-end.

---

## Catatan Implementasi (Deviasi & Keputusan Teknis)

- **Efficiency:** `efficiency_pct` dihitung per **total core** (`S_p / total_cores * 100`), sedangkan `node_efficiency_pct` dihitung per **jumlah Worker node** (`S_p / num_workers * 100`). Ini menghindari nilai efisiensi >100% yang membingungkan saat tiap node multicore. Keduanya tersedia di CSV dan tabel report.
- **Baseline T1:** `get_latest_baseline_t1()` hanya memakai baris sequential dengan `tag`, `total_frames`, dan `target_res` yang sama. Jika belum ada, gunakan `--baseline-time`.
- **Frame cache & transfer:** Master menyimpan frame sebagai JPEG di disk (bukan array di RAM). Input dikirim JPEG, hasil dikirim PNG lossless, keduanya dibungkus zlib. Ini keputusan desain agar 1080p tidak menghabiskan RAM/bandwidth.
- **Benchmark:** Mode sequential/local-parallel memakai jalur `process_encoded_frame` (decode JPEG + pipeline + encode PNG) agar adil dibanding Worker; `process_batch_sequential()` dan `process_batch_parallel()` tetap tersedia dan mendukung `encoded=True`.
- **Dynamic LB:** Pull-based, satu round-trip minimal satu task; capacity score `cores*(1-0.6*cpu/100)*(1-0.4*ram/100)` menentukan prefetch `ceil(core_worker / task_frames)` dibatasi `--max-tasks-per-assignment` (default 4). Riwayat skor selalu ditulis ke `logs/lb_scores_*.csv`; `--log-lb` menambah log assignment detail.
- **Dekomposisi task:** Ukuran task tidak lagi batch tetap 30 frame. Master menghitung `task_frames = ceil(total_frame / (total_core x chunks_per_core))` (default `chunks_per_core=4`), minimal 3 frame, maksimal `--batch-size`. Task mengecil saat jumlah Worker bertambah.
- **Master ikut komputasi:** `MASTER-LOCAL` didaftarkan sebagai Worker dengan seluruh core mesin (bisa dibatasi `--local-cores`), dan batch task diproses dalam satu `pool.map` agar semua core terpakai. Nonaktifkan dengan `--no-local-worker`.
- **Discovery:** Modul `discovery.py` menjawab broadcast UDP `DISCOVER_MASTER` (port 9999) dan melakukan TCP sweep sebagai fallback. Worker default `--master-ip auto`; mode manual tetap didukung.
- **Timeout per task:** Selain heartbeat timeout 6 detik, setiap task yang berjalan lebih dari `--task-timeout` (default 45 detik) memicu Worker ditandai mati dan task di-requeue (maksimal 3 percobaan).
- **Simulasi lokal:** `simulate_cluster.py` default 9 Worker x 1 core; `scripts/local_cluster.py` default 2 Worker x 2 core untuk development cepat.
- **CLI:** `master.py` menerima `--scenario` dan alias `--scenario-name`; `local_cluster.py` meneruskan `--scenario-name`.