# config.py
"""Konfigurasi global sistem Distributed Video Processing (Master-Worker)."""
import os

# ---------------------------------------------------------------------------
# Konfigurasi Jaringan Master-Worker
# ---------------------------------------------------------------------------
MASTER_BIND_HOST = "0.0.0.0"       # Master listen di semua interface jaringan
DEFAULT_MASTER_IP = "127.0.0.1"    # Ubah ke IP LAN Master saat uji 10 node fisik
MASTER_PORT = 5050

# ---------------------------------------------------------------------------
# Konfigurasi Pemrosesan Video & Task Decomposition
# ---------------------------------------------------------------------------
BATCH_SIZE = 30                    # Jumlah frame per task (mengurangi overhead jaringan)
TARGET_RESOLUTION = (1920, 1080)   # Resolusi standar output (Full HD)
ZLIB_COMPRESSION_LEVEL = 3         # Level 1-9 (3 = seimbang antara kecepatan CPU & ukuran paket)

# Frame input dikirim sebagai JPEG (jauh lebih kecil daripada array mentah),
# frame hasil dikirim sebagai PNG lossless (citra edge sangat mudah dikompresi).
JPEG_QUALITY = 90
PNG_COMPRESSION_LEVEL = 1

# Batas keamanan ukuran satu paket (byte) agar paket rusak tidak menghabiskan RAM.
MAX_PACKET_BYTES = 512 * 1024 * 1024

# ---------------------------------------------------------------------------
# Konfigurasi Fault Tolerance (Heartbeat)
# ---------------------------------------------------------------------------
HEARTBEAT_INTERVAL = 2.0           # Worker mengirim sinyal hidup tiap 2 detik
WORKER_TIMEOUT = 6.0               # Master menganggap Worker mati jika diam > 6 detik
STARTUP_TIMEOUT = 120.0            # Batas menunggu seluruh Worker terdaftar (0 = tunggu selamanya)
NO_WORKER_TIMEOUT = 60.0           # Batas tanpa Worker hidup sebelum eksperimen dibatalkan
MAX_TASK_ATTEMPTS = 5              # Batas retry satu task sebelum ditandai gagal
MAX_TASKS_PER_ASSIGNMENT = 2       # Dynamic LB: prefetch task untuk Worker berkapasitas tinggi

# ---------------------------------------------------------------------------
# Konfigurasi Koneksi & Polling
# ---------------------------------------------------------------------------
CONNECT_TIMEOUT = 60.0             # Worker mencoba menyambung ke Master selama ini
SOCKET_TIMEOUT = 300.0             # Timeout baca/tulis socket (detik)
WAIT_POLL_INTERVAL = 0.25          # Jeda Worker saat Master menjawab WAIT

# ---------------------------------------------------------------------------
# Path Direktori Dataset, Cache, Output, dan Log
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
CACHE_DIR = os.path.join(DATA_DIR, "frames_cache")
LOG_DIR = os.path.join(BASE_DIR, "logs")
RUNS_DIR = os.path.join(LOG_DIR, "runs")

DEFAULT_INPUT_VIDEO = os.path.join(DATA_DIR, "input_video.mp4")
DEFAULT_OUTPUT_VIDEO = os.path.join(DATA_DIR, "output_video.mp4")
LOG_CSV_PATH = os.path.join(LOG_DIR, "experiment_results.csv")


def parse_resolution(value: str) -> tuple[int, int]:
    """Parse string 'WIDTHxHEIGHT' (mis. '1920x1080') menjadi tuple (width, height)."""
    text = str(value).lower().replace(" ", "")
    if "x" not in text:
        raise ValueError(f"Format resolusi harus WIDTHxHEIGHT, contoh 1920x1080 (diberikan: {value!r})")
    width_str, height_str = text.split("x", 1)
    width, height = int(width_str), int(height_str)
    if width <= 0 or height <= 0:
        raise ValueError(f"Resolusi harus positif (diberikan: {value!r})")
    return width, height


def format_resolution(resolution: tuple[int, int]) -> str:
    return f"{int(resolution[0])}x{int(resolution[1])}"
