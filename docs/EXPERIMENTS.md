# Matriks Eksperimen & Analisis (BAB IV)

Dokumen ini memetakan 6 skenario wajib ke perintah, data CSV, dan cara
menuliskannya di laporan.

## 0. Persiapan

Gunakan video, batch size, resolusi target, jumlah core per Worker, dan hardware
yang **sama** untuk semua skenario agar perbandingan adil.

Contoh:

- Video: 1280x720, 30 FPS, 20 detik (600 frame) atau 1920x1080, 60 detik.
- Batch: 30 frame/task.
- Target: resolusi yang sama dengan video uji.
- Tag: `uas` (harus sama untuk T1, local parallel, dan distributed).

Buat baseline lebih dulu:

```bash
python benchmark.py --mode sequential \
  --video data/input_video.mp4 --frame-limit 240 \
  --batch-size 30 --target-res 1280x720 --tag uas

python benchmark.py --mode local-parallel \
  --video data/input_video.mp4 --frame-limit 240 \
  --batch-size 30 --target-res 1280x720 --cores 4 --tag uas
```

Catat `exec_time_sec` sequential sebagai **T1**. Master/benchmark hanya memakai
baseline dengan **tag, jumlah frame, dan resolusi target yang sama**. Jika belum
ada, isi `--baseline-time` secara eksplisit.

## 1. Sequential Processing (T1)

```bash
python benchmark.py --mode sequential \
  --video data/input_video.mp4 --frame-limit 240 \
  --batch-size 30 --target-res 1280x720 --tag uas
```

Tujuan: baseline 1 core, tanpa jaringan.

Data: `scenario=sequential`, `workers=1`, `total_cores=1`, `exec_time_sec=T1`.

Analisis: baseline ini dipakai untuk semua perhitungan Speedup. Timer mencakup
decode JPEG + pipeline OpenCV + encode PNG, sama seperti jalur Worker, tetapi
tanpa overhead jaringan.

## 2. Local Parallel Processing

```bash
python benchmark.py --mode local-parallel \
  --video data/input_video.mp4 --frame-limit 240 \
  --batch-size 30 --target-res 1280x720 --cores 4 --tag uas
```

Tujuan: efek multiprocessing pada 1 node, tanpa network overhead.

Data: `scenario=local_parallel`, `workers=4`, `total_cores=4`.

Analisis:

- Speedup lokal = `T1 / T_local`.
- Jika speedup < 1 pada video kecil, jelaskan overhead spawn process dan
  ukuran workload; ulangi dengan video 1080p yang lebih berat.
- Bandingkan efisiensi lokal vs distributed untuk melihat biaya jaringan.

## 3. Distributed Processing (Static vs Dynamic)

Fisik (9 Worker, masing-masing 4 core):

```bash
python master.py --workers 9 --lb static \
  --video data/input_video.mp4 --output data/output_static.mp4 \
  --batch-size 30 --target-res 1280x720 \
  --tag uas --scenario-name Distributed_Static_9W --log-lb

python master.py --workers 9 --lb dynamic \
  --video data/input_video.mp4 --output data/output_dynamic.mp4 \
  --batch-size 30 --target-res 1280x720 \
  --tag uas --scenario-name Distributed_Dynamic_9W --log-lb
```

Jalankan 9 Worker di PC masing-masing (lihat `DEPLOYMENT_10_NODE.md`).

Alternatif simulasi lokal:

```bash
python simulate_cluster.py --workers 9 --lb static \
  --video data/input_video.mp4 --frame-limit 240 \
  --batch-size 30 --target-res 1280x720 --tag uas --log-lb

python simulate_cluster.py --workers 9 --lb dynamic \
  --video data/input_video.mp4 --frame-limit 240 \
  --batch-size 30 --target-res 1280x720 --tag uas --log-lb
```

Data tambahan:

- `logs/distribution_<tag>_<stamp>.csv`: task, skor kapasitas, CPU/RAM per Worker.
- `logs/task_timeline_<tag>_<stamp>.csv`: waktu assign/complete per task.
- `logs/lb_scores_<tag>_<stamp>.csv`: riwayat capacity score saat
  register/heartbeat/assign/dead.

Analisis:

- Bandingkan `exec_time_sec` static vs dynamic.
- Bandingkan pemerataan `tasks_done` antar Worker.
- Pada static, Worker lambat menjadi bottleneck (makespan = Worker terlama).
- Pada dynamic, Worker cepat/memiliki core lebih banyak mendapat lebih banyak
  task. Skor kapasitas:
  `cores * (1 - 0.60*cpu/100) * (1 - 0.40*ram/100)`.
- Worker dengan skor >= 75% skor tertinggi boleh mengambil maksimal 2 batch per
  round-trip (`--max-tasks-per-assignment 2`) selama antrean masih cukup.
- Buat Gantt chart dari `task_timeline_*.csv` (sumbu X waktu, sumbu Y Worker,
  batang dari `assigned` sampai `completed`).

## 4. Scalability (1, 2, 4, 6, 9 Worker)

Fisik: jalankan Master dengan jumlah Worker yang diuji dan nyalakan hanya
sebanyak itu Worker. Gunakan tag berbeda per titik:

```bash
python master.py --workers 1 --lb dynamic --tag uas-scale-1 --log-lb ...
python master.py --workers 2 --lb dynamic --tag uas-scale-2 --log-lb ...
python master.py --workers 4 --lb dynamic --tag uas-scale-4 --log-lb ...
python master.py --workers 6 --lb dynamic --tag uas-scale-6 --log-lb ...
python master.py --workers 9 --lb dynamic --tag uas-scale-9 --log-lb ...
```

Simulasi lokal (1 core per Worker agar adil di satu PC):

```bash
python simulate_cluster.py --workers 1 --cores-per-worker 1 --tag uas-scale-1 ...
python simulate_cluster.py --workers 2 --cores-per-worker 1 --tag uas-scale-2 ...
python simulate_cluster.py --workers 4 --cores-per-worker 1 --tag uas-scale-4 ...
python simulate_cluster.py --workers 6 --cores-per-worker 1 --tag uas-scale-6 ...
python simulate_cluster.py --workers 9 --cores-per-worker 1 --tag uas-scale-9 ...
```

Setiap run harus memakai video, frame limit, batch, dan resolusi yang sama.

Analisis:

- Plot Speedup vs jumlah Worker.
- Plot Efficiency vs jumlah Worker (`efficiency_pct` per core atau
  `node_efficiency_pct` per node; jelaskan pilihan di laporan).
- Plot Throughput vs jumlah Worker.
- Jelaskan deviasi dari linear speedup: overhead jaringan, serial section
  (Master membaca cache + mengirim task), I/O disk, dan ketidakseragaman hardware.
- Ideal: Speedup naik lalu melandai; Efficiency turun saat p bertambah.

## 5. Fault Tolerance

Skenario: 9 Worker, matikan 1-2 Worker di tengah proses.

Manual:

```bash
# Master + 9 Worker berjalan, lalu Ctrl+C pada 2 terminal Worker
```

Otomatis di local cluster (untuk uji logika):

```bash
python scripts/local_cluster.py --workers 3 --lb dynamic \
  --crash-worker-index 0 --crash-on-task 1 \
  --tag uas-fault --scenario-name Fault_Tolerance_2W
```

Data yang diambil:

- `recovery_time_sec`
- `requeued_tasks`
- `lost_frames` (harus 0)
- `status` (harus `completed`)
- Log `[FAULT TOLERANCE]` di Master.

Analisis:

- Recovery Time = waktu dari deteksi sampai task requeue diambil Worker sehat.
- Pastikan jumlah frame output = jumlah frame input.
- Bandingkan total waktu run normal vs run dengan kegagalan (overhead recovery).
- Jelaskan mekanisme: heartbeat timeout + socket EOF + requeue + retry maksimum.

## 6. Performance Evaluation (6 metrik wajib)

| Metrik | Rumus / Sumber | Kolom CSV |
|---|---|---|
| Execution Time | wall-clock mulai semua Worker siap sampai hasil terakhir | `exec_time_sec` |
| Speedup | `T1 / Tp` | `speedup` |
| Efficiency per core | `(Speedup / total_cores) * 100%` | `efficiency_pct` |
| Efficiency per node | `(Speedup / num_workers) * 100%` | `node_efficiency_pct` |
| Throughput | `total_frame / Tp` | `throughput_fps` |
| CPU Utilization | rata-rata telemetri heartbeat Worker | `avg_cpu_pct`, `max_cpu_pct` |
| Memory Utilization | rata-rata telemetri heartbeat Worker | `avg_ram_pct`, `max_ram_pct` |
| Recovery Time | `T_redispatch - T_failure_detected` | `recovery_time_sec` |
| Distribusi task | jumlah task selesai per Worker | `worker_task_distribution` |

`efficiency_pct` bisa >100% pada `node_efficiency_pct` jika tiap node
multicore; itu normal. Gunakan `efficiency_pct` untuk efisiensi hardware dan
`node_efficiency_pct` untuk efisiensi per jumlah node.

Tabel rekap yang disarankan:

| Skenario | p | Core | Frame | Tp (s) | Speedup | Eff/core (%) | Eff/node (%) | Throughput (FPS) | CPU (%) | RAM (%) | Recovery (s) | Lost |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Sequential | 1 | 1 | 1800 | ... | 1.000 | 100.00 | 100.00 | ... | ... | ... | 0 | 0 |
| Local Parallel | 4 | 4 | 1800 | ... | ... | ... | ... | ... | ... | ... | 0 | 0 |
| Distributed Static | 9 | 36 | 1800 | ... | ... | ... | ... | ... | ... | ... | 0 | 0 |
| Distributed Dynamic | 9 | 36 | 1800 | ... | ... | ... | ... | ... | ... | ... | 0 | 0 |
| Fault Tolerance | 9 | 36 | 1800 | ... | ... | ... | ... | ... | ... | ... | ... | 0 |

## 7. Grafik yang disarankan

1. **Bar chart** Execution Time: sequential vs local parallel vs distributed.
2. **Line chart** Speedup vs jumlah Worker (1, 2, 4, 6, 9) + garis ideal `y=x`.
3. **Line chart** Efficiency vs jumlah Worker.
4. **Line chart** Throughput vs jumlah Worker.
5. **Bar chart** distribusi task per Worker: static vs dynamic.
6. **Gantt chart** timeline task dari CSV.
7. **Bar chart** Recovery Time untuk 1 vs 2 Worker yang dimatikan.
8. **Line chart** capacity score per Worker dari `lb_scores_*.csv`.
9. **Time series** CPU/RAM (bisa dari sampler; untuk grafik detail tambahkan
   logger sendiri bila perlu).

## 8. Validasi sebelum menulis laporan

- [ ] Semua run memakai video, frame limit, batch, resolusi, dan hardware yang sama.
- [ ] `lost_frames = 0` pada semua run yang valid.
- [ ] Jumlah frame output = jumlah frame input.
- [ ] `status = completed`.
- [ ] T1 diukur di komputer yang sama dengan Master (atau dijelaskan).
- [ ] Speedup/Efficiency dihitung dari T1 dengan tag yang sama.
- [ ] Data mentah CSV dilampirkan di laporan.
- [ ] Tautan & lisensi video dicantumkan (`data/SOURCES.txt`).

## 9. Contoh analisis singkat

- **Static vs Dynamic:** static cepat saat semua Worker homogen, tetapi
  dynamic lebih tahan terhadap heterogenitas dan kegagalan karena task tidak
  terikat ke satu Worker.
- **Capacity score:** Worker dengan core lebih banyak dan beban lebih rendah
  mendapat prefetch task lebih awal; lihat `lb_scores_*.csv` untuk bukti.
- **Scalability:** kenaikan Speedup tidak linear karena Master menjadi
  pengirim tunggal (serial section) dan biaya transfer frame.
- **Fault Tolerance:** recovery time bergantung pada deteksi (heartbeat/EOF)
  dan ketersediaan Worker sehat; `lost_frames=0` membuktikan requeue bekerja.
- **CPU-bound:** jika CPU utilization tinggi dan network utilization rendah,
  bottleneck ada di komputasi; jika transfer besar dan CPU rendah, pertimbangkan
  batch size atau kompresi.
