# utils/metrics_logger.py
"""Pencatat metrik performa ke CSV + sampler utilisasi CPU/RAM."""
from __future__ import annotations

import csv
import os
import platform
import socket
import threading
import time

import psutil

from config import LOG_CSV_PATH, LOG_DIR

# Skema kolom CSV. Kolom baru akan dimigrasikan otomatis tanpa menghapus data lama.
FIELDS = [
    "timestamp",
    "tag",
    "scenario",
    "scenario_name",
    "status",
    "lb_mode",
    "workers",
    "num_workers",
    "workers_seen",
    "cores_per_worker",
    "total_cores",
    "batch_size",
    "target_res",
    "total_frames",
    "exec_time_sec",
    "baseline_t1_sec",
    "throughput_fps",
    "speedup",
    "efficiency_pct",
    "node_efficiency_pct",
    "avg_cpu_pct",
    "max_cpu_pct",
    "avg_ram_pct",
    "max_ram_pct",
    "recovery_time_sec",
    "requeued_tasks",
    "failed_tasks",
    "lost_frames",
    "worker_task_distribution",
    "hostname",
    "cpu_cores",
    "notes",
]


class MetricsLogger:
    """Append-only CSV logger yang aman dipakai lintas proses/run."""

    def __init__(self, csv_path: str = LOG_CSV_PATH):
        self.csv_path = os.path.abspath(csv_path)
        os.makedirs(os.path.dirname(self.csv_path), exist_ok=True)
        self.fields = list(FIELDS)
        self._migrate_if_needed()

    def _migrate_if_needed(self) -> None:
        if not os.path.exists(self.csv_path) or os.path.getsize(self.csv_path) == 0:
            return
        try:
            with open(self.csv_path, "r", newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                existing_fields = reader.fieldnames or []
                rows = list(reader)
        except OSError:
            return
        if existing_fields == self.fields:
            return

        merged = list(FIELDS) + [field for field in existing_fields if field not in FIELDS]
        backup_path = f"{self.csv_path}.bak_{int(time.time())}"
        try:
            os.replace(self.csv_path, backup_path)
            with open(self.csv_path, "w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=merged, extrasaction="ignore")
                writer.writeheader()
                for row in rows:
                    writer.writerow(row)
            self.fields = merged
            print(
                f"[METRICS] Skema CSV lama dimigrasikan (backup: {backup_path}).",
                flush=True,
            )
        except OSError:
            # Jika migrasi gagal, tetap pakai skema baru tanpa menyentuh file lama.
            self.fields = list(FIELDS)

    def log(self, record: dict) -> None:
        row = {field: "" for field in self.fields}
        row.update({key: value for key, value in record.items() if key in row})
        if not row.get("timestamp"):
            row["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
        if not row.get("hostname"):
            row["hostname"] = socket.gethostname()
        if not row.get("cpu_cores"):
            row["cpu_cores"] = os.cpu_count() or 1

        file_exists = os.path.exists(self.csv_path) and os.path.getsize(self.csv_path) > 0
        with open(self.csv_path, "a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=self.fields, extrasaction="ignore")
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)

    @staticmethod
    def read_all(csv_path: str = LOG_CSV_PATH) -> list[dict]:
        if not os.path.exists(csv_path):
            return []
        with open(csv_path, "r", newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))


def _to_float(value, default: float = 0.0) -> float:
    try:
        text = str(value).strip()
        if not text:
            return default
        return float(text)
    except (TypeError, ValueError):
        return default


def _to_int(value, default: int = 0) -> int:
    try:
        text = str(value).strip()
        if not text:
            return default
        return int(float(text))
    except (TypeError, ValueError):
        return default


def get_latest_baseline_t1(
    csv_path: str = LOG_CSV_PATH,
    total_frames: int | None = None,
    target_res: str | None = None,
    tag: str | None = None,
) -> float | None:
    """Ambil T1 sequential terbaru yang cocok dengan frame/resolusi/tag.

    Jika `tag` diberikan, baseline WAJIB berasal dari tag yang sama agar hasil
    antar-eksperimen tidak tercampur. Bila belum ada, fungsi mengembalikan None
    dan pengguna dapat mengisi `--baseline-time` secara eksplisit.
    """
    rows = MetricsLogger.read_all(csv_path)
    candidates: list[dict] = []
    for row in rows:
        if "sequential" not in str(row.get("scenario", "")).lower():
            continue
        exec_time = _to_float(row.get("exec_time_sec"))
        if exec_time <= 0:
            continue
        if total_frames is not None and _to_int(row.get("total_frames")) != int(total_frames):
            continue
        if target_res and str(row.get("target_res", "")) != str(target_res):
            continue
        candidates.append(row)
    if tag is not None:
        candidates = [row for row in candidates if row.get("tag") == tag]
    if not candidates:
        return None
    return _to_float(candidates[-1].get("exec_time_sec")) or None


def print_report(csv_path: str = LOG_CSV_PATH, tag: str | None = None) -> None:
    """Cetak tabel ringkas + hitung Speedup/Efficiency terhadap baseline sequential."""
    rows = MetricsLogger.read_all(csv_path)
    if tag:
        rows = [row for row in rows if row.get("tag") == tag]
    rows = [row for row in rows if _to_float(row.get("exec_time_sec")) > 0]
    if not rows:
        print(f"[REPORT] Tidak ada data metrik di {csv_path}" + (f" untuk tag={tag!r}." if tag else "."))
        return

    # Baseline sequential terbaru per (tag, jumlah frame, resolusi target).
    baselines: dict[tuple, float] = {}
    for row in rows:
        scenario = str(row.get("scenario", "")).lower()
        if "sequential" not in scenario:
            continue
        key = (row.get("tag", ""), row.get("total_frames", ""), row.get("target_res", ""))
        exec_time = _to_float(row.get("exec_time_sec"))
        if exec_time > 0:
            baselines[key] = exec_time

    print("\n" + "=" * 140)
    print("RINGKASAN METRIK PERFORMA DISTRIBUTED VIDEO PROCESSING")
    print("=" * 140)
    header = (
        f"{'Tag':<12} {'Scenario':<24} {'LB':<8} {'W':>2} {'C':>3} {'Frames':>7} "
        f"{'Exec(s)':>9} {'Speedup':>8} {'Eff(%)':>7} {'NodeEff':>8} {'FPS':>8} "
        f"{'CPU%':>6} {'RAM%':>6} {'Recov(s)':>9} {'Lost':>5} {'Status':<10}"
    )
    print(header)
    print("-" * 140)
    for row in rows:
        key = (row.get("tag", ""), row.get("total_frames", ""), row.get("target_res", ""))
        exec_time = _to_float(row.get("exec_time_sec"))
        workers = max(1, _to_int(row.get("workers"), 1))
        total_cores = max(1, _to_int(row.get("total_cores"), workers))
        speedup = _to_float(row.get("speedup"))
        if speedup <= 0 and key in baselines and exec_time > 0:
            speedup = baselines[key] / exec_time
        efficiency = _to_float(row.get("efficiency_pct"))
        if efficiency <= 0 and speedup > 0:
            efficiency = (speedup / total_cores) * 100.0
        node_efficiency = _to_float(row.get("node_efficiency_pct"))
        if node_efficiency <= 0 and speedup > 0:
            node_efficiency = (speedup / workers) * 100.0
        speedup_text = f"{speedup:>8.3f}" if speedup > 0 else f"{'-':>8}"
        efficiency_text = f"{efficiency:>7.2f}" if efficiency > 0 else f"{'-':>7}"
        node_efficiency_text = (
            f"{node_efficiency:>8.2f}" if node_efficiency > 0 else f"{'-':>8}"
        )
        print(
            f"{row.get('tag', ''):<12} {row.get('scenario', ''):<24} "
            f"{row.get('lb_mode', ''):<8} {workers:>2} {total_cores:>3} "
            f"{_to_int(row.get('total_frames')):>7} {exec_time:>9.2f} "
            f"{speedup_text} {efficiency_text} {node_efficiency_text} "
            f"{_to_float(row.get('throughput_fps')):>8.2f} "
            f"{_to_float(row.get('avg_cpu_pct')):>6.1f} "
            f"{_to_float(row.get('avg_ram_pct')):>6.1f} "
            f"{_to_float(row.get('recovery_time_sec')):>9.3f} "
            f"{_to_int(row.get('lost_frames')):>5} {row.get('status', ''):<10}"
        )
    print("=" * 140)
    print(f"Sumber data: {os.path.abspath(csv_path)}")
    print(
        "Speedup = T1_sequential / Tp | Eff = (Speedup / total_core) x 100% | "
        "NodeEff = (Speedup / jumlah Worker) x 100% | Throughput = total_frame / Tp"
    )


class ResourceSampler(threading.Thread):
    """Thread sampler CPU/RAM proses lokal (Master atau benchmark)."""

    def __init__(self, interval: float = 1.0):
        super().__init__(daemon=True, name="ResourceSampler")
        self.interval = max(0.1, float(interval))
        self.samples: list[tuple[float, float]] = []
        self._stop_event = threading.Event()
        psutil.cpu_percent(interval=None)  # prime baseline

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                cpu = psutil.cpu_percent(interval=None)
                ram = psutil.virtual_memory().percent
                self.samples.append((cpu, ram))
            except Exception:  # noqa: BLE001 - sampler tidak boleh mematikan program
                pass
            self._stop_event.wait(self.interval)

    def stop(self) -> None:
        self._stop_event.set()
        if self.is_alive():
            self.join(timeout=max(2.0, self.interval * 2))

    def summary(self) -> dict:
        # Run sangat singkat (<1 interval) bisa belum punya sampel; ambil snapshot singkat.
        if not self.samples or (len(self.samples) == 1 and self.samples[0][0] == 0.0):
            try:
                cpu = psutil.cpu_percent(interval=0.2)
                ram = psutil.virtual_memory().percent
                return {
                    "avg_cpu_pct": round(cpu, 2),
                    "max_cpu_pct": round(cpu, 2),
                    "avg_ram_pct": round(ram, 2),
                    "max_ram_pct": round(ram, 2),
                }
            except Exception:  # noqa: BLE001
                pass
        if not self.samples:
            return {"avg_cpu_pct": 0.0, "max_cpu_pct": 0.0, "avg_ram_pct": 0.0, "max_ram_pct": 0.0}
        cpu_values = [sample[0] for sample in self.samples]
        ram_values = [sample[1] for sample in self.samples]
        return {
            "avg_cpu_pct": round(sum(cpu_values) / len(cpu_values), 2),
            "max_cpu_pct": round(max(cpu_values), 2),
            "avg_ram_pct": round(sum(ram_values) / len(ram_values), 2),
            "max_ram_pct": round(max(ram_values), 2),
        }


def write_dict_csv(path: str, rows: list[dict], fieldnames: list[str] | None = None) -> None:
    """Tulis list of dict ke CSV (dipakai untuk distribusi worker & timeline task)."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    if not rows:
        return
    if fieldnames is None:
        fieldnames = []
        for row in rows:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def system_info() -> dict:
    return {
        "hostname": socket.gethostname(),
        "cpu_cores": os.cpu_count() or 1,
        "platform": platform.platform(),
    }


def ensure_log_dir() -> None:
    os.makedirs(LOG_DIR, exist_ok=True)
