# Distributed Video Processing - Master/Worker

Sistem komputasi terdistribusi untuk **batch frame extraction & multi-stage video filtering**
(resize -> grayscale -> Gaussian blur -> Canny edge + bounding box) dengan arsitektur
**1 Master + N Worker** di jaringan lokal.

Proyek UAS Komputasi Paralel dan Sistem Terdistribusi.

## Fitur utama

- Memproses video per batch frame melalui resize, grayscale, Gaussian blur,
  Canny edge detection, dan bounding box.
- Menggunakan beberapa Worker dengan load balancing `dynamic` (default) atau
  `static`.
- Master ikut menghitung secara default; gunakan `--no-local-worker` untuk
  menonaktifkannya.
- Worker menemukan Master lewat UDP broadcast, dengan fallback TCP sweep atau
  alamat manual (`--master-ip`).
- Heartbeat dan requeue membantu menangani Worker atau task yang gagal.
- Menyusun hasil menjadi video MP4 dan mencatat metrik ke CSV.

## Instalasi

Prasyarat: **Python 3.10+**.

```bash
python -m venv .venv
```

Aktifkan environment:

```powershell
.\.venv\Scripts\Activate.ps1
```

Di Linux/macOS, gunakan `source .venv/bin/activate`. Setelah environment aktif,
pasang dependensi:

```bash
python -m pip install -r requirements.txt
```

Di VS Code, pilih interpreter `.venv` melalui **Python: Select Interpreter**.

Cek:

```bash
python -c "import cv2, numpy, psutil; print(cv2.__version__, numpy.__version__, psutil.__version__)"
```

## Uji lokal

```bash
# Uji pembagian beban dinamis/statis dan toleransi kegagalan
python scripts/smoke_test.py

# Tampilkan ringkasan hasil
python benchmark.py --report
```

Untuk simulasi cluster lokal:

```bash
python scripts/local_cluster.py --workers 2 --lb dynamic --frame-limit 120 --target-res 640x360 --tag dev
```

Skrip lokal hanya untuk pengembangan. Panduan topologi dan jaringan fisik ada di
[`docs/DEPLOYMENT_10_NODE.md`](docs/DEPLOYMENT_10_NODE.md).

## Menjalankan sistem

### Master dan Worker di jaringan

Jalankan Master lebih dulu pada komputer yang memiliki `data/input_video.mp4`:

```bash
python master.py --workers 9 --lb dynamic --tag uas
```

Default output adalah `data/output_video.mp4`. Master juga ikut menghitung
menggunakan seluruh core lokal. Gunakan `--local-cores 4` untuk membatasinya
atau `--no-local-worker` agar Master hanya menjadi koordinator.
`--workers 9` berarti sembilan Worker remote; jika Master lokal aktif, jumlah
node komputasi menjadi sepuluh setelah semuanya terhubung.

Jalankan pada tiap komputer Worker dengan ID unik:

```bash
python worker.py --id WORKER-01 --cores 4
```

Ubah ID dan jumlah core sesuai komputer. Auto-discovery menggunakan UDP 9999
dengan fallback TCP sweep pada port 5050. Jika tidak menemukan Master, jalankan
dengan IP Master secara eksplisit:

```bash
python worker.py --master-ip 192.168.1.10 --port 5050 --id WORKER-01 --cores 4
```

Pastikan semua komputer berada di jaringan yang saling terhubung dan firewall
Master mengizinkan TCP 5050 serta UDP 9999.

Setelah cache siap, Master meminta konfirmasi:

- `y`: mulai dengan Worker yang sudah terhubung.
- `n`: tampilkan Worker aktif, tunggu perubahan daftar, lalu tampilkan daftar
  terbaru dan tanyakan konfirmasi lagi.
- `q`: batalkan proses.

Daftar Worker tidak ditampilkan sebelum jawaban pertama. Gunakan
`--no-confirm-start` jika Master harus otomatis menunggu jumlah `--workers`
hingga `--startup-timeout` (default 120 detik).

### Master saja

Untuk menjalankan tanpa Worker remote:

```bash
python master.py --workers 0 --frame-limit 120 --tag demo
```

### Benchmark dan load balancing

| Pengujian | Perintah |
|---|---|
| Baseline sekuensial | `python benchmark.py --mode sequential --tag uas` |
| Paralel lokal | `python benchmark.py --mode local-parallel --cores 4 --tag uas` |
| Terdistribusi, dinamis | `python master.py --workers 9 --lb dynamic --tag uas` |
| Terdistribusi, statis | `python master.py --workers 9 --lb static --tag uas` |
| Lihat hasil bertag | `python benchmark.py --report --tag uas` |

Untuk pengujian terdistribusi, jalankan Worker setelah Master. Gunakan tag yang
sama untuk membandingkan skenario. Percepatan (speedup) dihitung jika baseline
sekuensial dengan tag, jumlah frame, dan resolusi yang sama tersedia.

## Hasil

Video hasil disimpan ke `data/output_video.mp4`. Metrik utama dan file detail
disimpan di `logs/`:

- `experiment_results.csv`: satu baris per run; gunakan `--tag` untuk menyaring
  ringkasan, misalnya `python benchmark.py --report --tag uas`.
- `distribution_*.csv`: jumlah task yang ditugaskan dan diselesaikan per Worker.
- `task_timeline_*.csv` dan `lb_scores_*.csv`: detail waktu task dan load balancing.

Run dinyatakan berhasil jika status `completed` dan `lost_frames` bernilai `0`.
`exec_time_sec` tidak memasukkan ekstraksi cache atau penulisan video. Speedup
dan efisiensi tersedia jika baseline sekuensial dengan tag, jumlah frame, dan
resolusi yang sama sudah dijalankan.

Throughput = total frame / waktu eksekusi. Speedup = waktu sekuensial / waktu
terdistribusi. Efisiensi per core = speedup / jumlah core x 100%; efisiensi per
node = speedup / jumlah node komputasi x 100% (Master lokal dihitung jika aktif).

## Toleransi kegagalan

Master mendeteksi Worker yang koneksinya putus atau heartbeat-nya terlambat,
lalu mengantrekan ulang task yang belum selesai. Uji simulasi lokal:

```bash
python scripts/local_cluster.py --workers 2 --lb dynamic --crash-worker-index 0 --crash-on-task 1 --tag fault-test
```

Untuk pengujian fisik, hentikan satu Worker dengan `Ctrl+C` saat proses berjalan.

## Troubleshooting

| Masalah | Solusi |
|---|---|
| Video input tidak ditemukan | Letakkan video di `data/input_video.mp4`, buat sampel dengan `python scripts/make_sample_video.py`, atau isi `--video`. |
| Worker tidak menemukan Master | Jalankan Master lebih dulu; pastikan UDP 9999/TCP 5050 dan subnet benar. Jika perlu, gunakan `--master-ip <IP>`. |
| Master belum mulai | Pastikan Worker sudah terhubung. Pilih `n` untuk menunggu Worker tambahan atau gunakan `--no-confirm-start`. |
| Worker dengan hostname sama tertukar | Berikan `--id` unik pada setiap Worker. |
| Status run `interrupted` atau frame hilang | Run berhenti sebelum semua task selesai; periksa log Master/Worker dan `lost_frames`. Gunakan tag untuk membedakan run pada CSV. |
| Task timeout | Naikkan `--task-timeout` atau kurangi ukuran task dengan `--task-frames`. |
| `Address already in use` | Hentikan Master lama atau pilih port lain dengan `--port`. |
| Speedup tidak tersedia | Jalankan baseline sekuensial dengan tag, jumlah frame, dan resolusi yang sama. |

## Batasan

- `pickle` hanya aman untuk LAN tepercaya; jangan expose port Master ke internet.
- Video output memakai codec `mp4v`; pastikan OpenCV build mendukungnya.
- Pengujian banyak Worker di satu PC hanya simulasi; ikuti
  [`docs/DEPLOYMENT_10_NODE.md`](docs/DEPLOYMENT_10_NODE.md) untuk uji fisik.
