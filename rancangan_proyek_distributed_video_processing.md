# Rancangan Proyek UAS: Distributed Video Processing Berbasis Arsitektur Master-Worker

## 1. Analisis Kebutuhan Tugas UAS

Proyek UAS mata kuliah **Komputasi Paralel dan Sistem Terdistribusi** menuntut pembangunan sistem komputasi terdistribusi berbasis arsitektur **Master-Worker** menggunakan minimal **10 node fisik** (1 Master dan 9 Worker) yang terhubung dalam satu jaringan lokal (LAN/WLAN) oleh tim beranggotakan 3 mahasiswa.

### A. Komponen Teknis Wajib
Sistem yang dibangun wajib mengimplementasikan 6 mekanisme inti berikut:
1. **Task Decomposition:** Memecah pekerjaan besar menjadi sub-tugas (*task*) independen yang dapat dieksekusi secara terpisah.
2. **Parallel Processing:** Memanfaatkan arsitektur *multicore* CPU pada masing-masing node Worker menggunakan pustaka *multiprocessing*.
3. **Distributed Processing:** Membangun komunikasi jaringan (pengiriman *task*, pengambilan hasil, dan sinkronisasi status) antara Master dan 9 Worker.
4. **Dynamic Load Balancing:** Mengalokasikan *task* secara adaptif berdasarkan kapasitas dan beban sumber daya *real-time* tiap Worker, bukan sekadar pembagian statis di awal.
5. **Fault Tolerance:** Mendeteksi Worker yang mati atau terputus (*node failure*) dan mengembalikan *task* yang belum selesai ke antrean (*requeue*/*retry*) agar tetap diproses Worker lain.
6. **Performance Evaluation:** Mengukur dan menganalisis performa sistem menggunakan 6 metrik wajib: *Execution Time*, *Speedup*, *Efficiency*, *Throughput*, *CPU/Memory Utilization*, dan *Recovery Time*.

### B. Skenario Pengujian Wajib
Program harus mendukung mode pengujian terkontrol untuk menghasilkan data analisis pada 6 skenario:
* **Sequential Processing:** Eksekusi 1 proses tunggal pada 1 komputer sebagai *baseline*.
* **Local Parallel Processing:** Eksekusi *multiprocessing* pada 1 komputer tanpa komunikasi jaringan.
* **Distributed Processing:** Eksekusi lintas jaringan menggunakan kombinasi Master dan Worker.
* **Dynamic Load Balancing:** Komparasi distribusi beban kerja sebelum (statis) dan sesudah (dinamis) mekanisme diterapkan.
* **Fault Tolerance:** Simulasi pemutusan paksa 1–2 Worker di tengah proses untuk mengukur *Recovery Time* dan memastikan hasil akhir tetap utuh.
* **Scalability:** Pengujian bertahap dengan variasi jumlah **1, 2, 4, 6, dan 9 Worker**.

### C. Luaran (*Deliverables*) Proyek
* **Source Code:** Seluruh kode Master, Worker, modul jaringan, *load balancing*, *fault tolerance*, dan pencatat metrik dalam format `.zip` atau `.rar`.
* **Laporan Proyek:** Terdiri dari BAB I (Pendahuluan), BAB II (Studi Pustaka), BAB III (Metodologi & Topologi), BAB IV (Hasil & Pembahasan), dan Kesimpulan.
* **Video Demonstrasi:** Rekaman berdurasi maksimal 5 menit (`.mp4`) yang membuktikan aplikasi berjalan secara nyata pada minimal 10 node fisik.

---

## 2. Pemilihan & Spesifikasi Tema Proyek

* **Tema Terpilih:** Tema 2 — **Distributed Video Processing**
* **Fokus Kasus:** *Batch Frame Extraction & Multi-Stage Video Filtering*
* **Justifikasi Beban Komputasi:** Pemrosesan video beresolusi tinggi (misalnya **1080p Full HD atau 4K, 30/60 FPS, durasi 1–2 menit** yang menghasilkan $1.800$ hingga $7.200$ *frame*) dengan kombinasi operasi pengolahan citra bertingkat (*resizing*, konversi *grayscale*, *Gaussian blur*, dan *Canny edge detection*) bersifat *CPU-bound*. Beban komputasi ini cukup tinggi untuk menunjukkan keunggulan pemrosesan paralel terdistribusi dibandingkan *overhead* transfer jaringan.
* **Strategi Dekomposisi:** Video tidak dikirim per 1 *frame* untuk menghindari tingginya *network latency*, melainkan dipecah menjadi potongan **batch/chunk** berisi **30–50 frame per task**.
* **Sumber Aset Resmi:** Menggunakan video sampel berlisensi terbuka (*open-license*), seperti *Big Buck Bunny* (Blender Foundation) atau video sampel dari *Pexels* / *Pixabay*, dengan menyertakan tautan resmi serta informasi lisensinya di dalam repositori dan laporan.

---

## 3. Rancangan Sistem Program

### A. Spesifikasi Teknologi (*Tech Stack*)
* **Bahasa Pemrograman:** Python 3.10+
* **Pemrosesan Video & Citra:** `opencv-python` (`cv2`) dan `numpy`
* **Paralelisme Lokal (Worker):** `concurrent.futures.ProcessPoolExecutor` atau `multiprocessing.Pool`
* **Komunikasi Jaringan:** Python `socket` (TCP) dengan protokol *custom header* (`struct` untuk panjang pesan) serta serialisasi `pickle` dan kompresi `zlib` untuk efisiensi *bandwidth* saat transfer *frame*.
* **Monitoring Sumber Daya:** `psutil` untuk membaca utilisasi CPU dan RAM secara *real-time*.

### B. Arsitektur & Alur Kerja Sistem

    +-------------------------------------------------------------------+
    |                         MASTER NODE (1 PC)                        |
    |  [Video Splitter] ---> [Task Queue (PENDING)]                     |
    |                              |   ^                                |
    |     (Dynamic Dispatcher)     |   | (Requeue jika Worker mati)     |
    |            |                 v   |                                |
    |            |          [In-Progress Tracker]                       |
    |            v                 ^                                    |
    |  [TCP Socket Server] <-------+---- [Heartbeat Monitor Thread]     |
    |            |                                                      |
    |            v                                                      |
    |  [Result Aggregator & Video Reconstructor] ---> Output (.mp4)     |
    +-------------------------------------------------------------------+
           |                  |                  |                |
      (LAN/Wi-Fi)        (LAN/Wi-Fi)        (LAN/Wi-Fi)      (LAN/Wi-Fi)
           v                  v                  v                v
    +-------------+    +-------------+    +-------------+  +-------------+
    |  WORKER 1   |    |  WORKER 2   |    |  WORKER ... |  |  WORKER 9   |
    | [Net Client]|    | [Net Client]|    | [Net Client]|  | [Net Client]|
    | [Heartbeat] |    | [Heartbeat] |    | [Heartbeat] |  | [Heartbeat] |
    | [Multi-Core |    | [Multi-Core |    | [Multi-Core |  | [Multi-Core |
    |  Pool CPU]  |    |  Pool CPU]  |    |  Pool CPU]  |  |  Pool CPU]  |
    +-------------+    +-------------+    +-------------+  +-------------+

### C. Mekanisme Detail Tiap Komponen

#### 1. Master Node (`master.py`)
* **Task Decomposition:** Membaca file video input menggunakan `cv2.VideoCapture`, memecahnya menjadi potongan *batch* (misal 30 *frame* per *task*), dan menyematkan metadata: `task_id`, `start_frame`, `end_frame`, serta data *frame* terkompresi (`zlib`).
* **Dynamic Load Balancing (Pull-Based + Capability Scoring):**
  * Saat Worker terhubung, Worker mengirim profil perangkat keras (`jumlah_core_cpu`, `kapasitas_ram`) dan rutin mengirim pembaruan `cpu_usage` melalui paket *heartbeat*.
  * Menggunakan mekanisme **Work-Stealing / On-Demand Pull**: Worker yang telah menyelesaikan satu *batch* langsung meminta *task* berikutnya ke Master. Prioritas dan alokasi *task* menyesuaikan kapasitas Worker secara dinamis sehingga node dengan spesifikasi lebih tinggi memproses lebih banyak *batch* secara alami.
  * Disediakan parameter `--lb-mode static` (membagi rata seluruh *task* di awal ke semua Worker) dan `--lb-mode dynamic` (distribusi *on-demand*) untuk kebutuhan pengujian perbandingan.
* **Fault Tolerance (Heartbeat & Task Requeue):**
  * *Thread* pengawas (*Heartbeat Monitor*) memeriksa status koneksi tiap Worker setiap **2 detik**.
  * Jika Worker tidak merespons selama **6 detik** (simulasi kabel terputus atau proses dimatikan paksa), Master menandai Worker sebagai `DEAD`, mencatat *timestamp* deteksi, dan mengembalikan seluruh `task_id` berstatus `IN_PROGRESS` milik Worker tersebut ke antrean `PENDING`.
  * Selisih waktu antara deteksi kegagalan hingga *task* diambil kembali oleh Worker sehat dicatat sebagai **Recovery Time**.
* **Result Aggregator & Video Reconstruction:** Master mengumpulkan *batch* hasil dari seluruh Worker, mengurutkannya kembali berdasarkan `task_id`, dan menyusunnya menjadi file video akhir menggunakan `cv2.VideoWriter`.

#### 2. Worker Node (`worker.py`)
* **Koneksi & Telemetri:** Terhubung ke alamat IP Master via TCP Socket dan menjalankan *background thread* untuk mengirim sinyal `HEARTBEAT` beserta data utilisasi CPU dan memori (`psutil.cpu_percent()`, `psutil.virtual_memory().percent`) setiap 2 detik.
* **Parallel Processing (Multiprocessing):** Saat menerima satu *batch* berisi 30 *frame*, Worker mendekompresi payload dan mendistribusikan ke-30 *frame* tersebut ke `ProcessPoolExecutor(max_workers=os.cpu_count())` agar diproses secara paralel oleh seluruh *core* CPU lokal.

#### 3. Pipeline Pemrosesan Frame (`processor.py`)
Setiap *frame* menjalani empat tahap pemrosesan citra berikut:
1. *Resizing / Upscaling* ke resolusi target (misal $1920 \times 1080$).
2. Konversi ruang warna ke *Grayscale* (`cv2.cvtColor`).
3. *Gaussian Blur* dengan kernel $7 \times 7$ (`cv2.GaussianBlur`) untuk reduksi *noise*.
4. Deteksi tepi *Canny Edge Detection* (`cv2.Canny`) dan ekstraksi kontur (`cv2.findContours`) untuk menggambar *bounding box* objek pada *frame* keluaran.

---

## 4. Struktur Direktori Proyek

Struktur modular dirancang agar pembagian tugas pengembangan kode dapat didistribusikan secara merata kepada 3 anggota tim:

    distributed-video-processing/
    ├── config.py                # Konfigurasi IP Master, Port, ukuran batch, interval heartbeat
    ├── protocol.py              # Helper pengiriman/penerimaan paket TCP (Header + Payload + Zlib)
    ├── processor.py             # Fungsi pemrosesan frame OpenCV & multiprocessing pool
    ├── master.py                # Server Master, Task Queue, Load Balancer, Fault Tolerance, Assembler
    ├── worker.py                # Client Worker, Heartbeat sender, Executor task paralel
    ├── benchmark.py             # Script otomatis menjalankan mode Sequential & Local Parallel
    ├── utils/
    │   └── metrics_logger.py    # Pencatat otomatis metrik performa ke format CSV
    ├── data/
    │   ├── input_video.mp4      # Dataset video sumber
    │   ├── SOURCES.txt          # Dokumentasi tautan & lisensi resmi video sumber
    │   └── output_video.mp4     # Hasil rekonstruksi video
    └── logs/
        └── experiment_results.csv # Output data mentah untuk analisis grafik di BAB IV

---

## 5. Rancangan Eksekusi Pengujian & Formula Metrik

Program menyediakan argumen *Command Line Interface* (CLI) untuk menjalankan seluruh skenario eksperimen secara terstandar:

| Skenario Uji | Perintah Eksekusi | Tujuan Pengukuran |
| :--- | :--- | :--- |
| **1. Sequential** | `python benchmark.py --mode sequential` | Mendapatkan $T_1$ (*baseline* waktu eksekusi 1 *core* tanpa jaringan). |
| **2. Local Parallel** | `python benchmark.py --mode local-parallel` | Mengukur efek *multiprocessing* pada 1 node tanpa *network overhead*. |
| **3. Static vs Dynamic LB** | `python master.py --workers 9 --lb static` vs `--lb dynamic` | Membandingkan pemerataan beban kerja dan total waktu selesai antar Worker. |
| **4. Scalability** | `python master.py --workers [1\|2\|4\|6\|9] --lb dynamic` | Mengukur tren peningkatan performa dari 1 hingga 9 node fisik. |
| **5. Fault Tolerance** | Jalankan 9 Worker, lalu hentikan paksa 2 Worker di tengah proses | Mengukur *Recovery Time* dan memvalidasi $0\%$ *frame* hilang pada video akhir. |

### Persamaan Metrik Evaluasi
Seluruh metrik dihitung secara otomatis oleh modul `utils/metrics_logger.py` menggunakan formula standar komputasi paralel berikut:

* **Speedup ($S_p$):**
  $$S_p = \frac{T_1}{T_p}$$
  *(di mana $T_1$ adalah waktu eksekusi sekuensial dan $T_p$ adalah waktu eksekusi paralel menggunakan $p$ Worker).*

* **Efficiency ($E_p$):**
  $$E_p = \frac{S_p}{p} \times 100\%$$

* **Throughput:**
  $$\text{Throughput} = \frac{\text{Total Frame Diproses}}{T_p} \quad (\text{frame/detik})$$

* **Recovery Time:**
  $$\text{Recovery Time} = T_{\text{requeue}} - T_{\text{failure\_detected}}$$