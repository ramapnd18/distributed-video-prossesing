# scripts/local_cluster.py
"""Jalankan 1 Master + N Worker di satu komputer untuk pengembangan/uji cepat.

Catatan: ini BUKAN pengujian 10 node fisik. Untuk UAS, jalankan master.py dan
worker.py secara terpisah di masing-masing PC sesuai docs/DEPLOYMENT_10_NODE.md.
"""
from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import (  # noqa: E402
    BATCH_SIZE,
    CACHE_DIR,
    CHUNKS_PER_CORE,
    DEFAULT_INPUT_VIDEO,
    DEFAULT_OUTPUT_VIDEO,
    HEARTBEAT_INTERVAL,
    JPEG_QUALITY,
    LOG_CSV_PATH,
    LOG_DIR,
    MAX_TASKS_PER_ASSIGNMENT,
    RUNS_DIR,
    STARTUP_TIMEOUT,
    TARGET_RESOLUTION,
    TASK_TIMEOUT,
    WORKER_TIMEOUT,
    format_resolution,
    parse_resolution,
)
from utils.metrics_logger import print_report  # noqa: E402
from utils.video_utils import count_video_frames  # noqa: E402


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for_port(port: int, process: subprocess.Popen, timeout: float = 30.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if process.poll() is not None:
            return False
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.2)
    return False


def terminate_process(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    try:
        process.terminate()
        process.wait(timeout=5)
    except (subprocess.TimeoutExpired, OSError):
        try:
            process.kill()
        except OSError:
            pass


def main() -> int:
    parser = argparse.ArgumentParser(description="Local dev cluster: 1 Master + N Worker")
    parser.add_argument("--workers", type=int, default=2, help="Jumlah Worker lokal")
    parser.add_argument("--lb", choices=["dynamic", "static"], default="dynamic")
    parser.add_argument("--video", type=str, default=DEFAULT_INPUT_VIDEO)
    parser.add_argument("--output", type=str, default=DEFAULT_OUTPUT_VIDEO)
    parser.add_argument("--frame-limit", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--chunks-per-core", type=int, default=CHUNKS_PER_CORE, help="Target task = total_core x nilai ini")
    parser.add_argument("--task-frames", type=int, default=None, help="Paksa ukuran task (frame)")
    parser.add_argument("--task-timeout", type=float, default=TASK_TIMEOUT, help="Timeout satu task (detik)")
    parser.add_argument(
        "--local-worker",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Master ikut komputasi (default: tidak, agar jumlah node sesuai parameter uji)",
    )
    parser.add_argument("--target-res", type=str, default=format_resolution(TARGET_RESOLUTION))
    parser.add_argument("--cores-per-worker", type=int, default=2)
    parser.add_argument("--cache-dir", type=str, default=CACHE_DIR)
    parser.add_argument("--rebuild-cache", action="store_true")
    parser.add_argument("--jpeg-quality", type=int, default=JPEG_QUALITY)
    parser.add_argument("--csv", type=str, default=LOG_CSV_PATH)
    parser.add_argument("--tag", type=str, default="local")
    parser.add_argument("--scenario-name", type=str, default=None, help="Nama skenario pada CSV")
    parser.add_argument("--startup-timeout", type=float, default=STARTUP_TIMEOUT)
    parser.add_argument("--hb-timeout", type=float, default=WORKER_TIMEOUT)
    parser.add_argument("--no-worker-timeout", type=float, default=60.0)
    parser.add_argument("--baseline-time", type=float, default=None, help="T1 sequential (detik) untuk Speedup")
    parser.add_argument("--max-tasks-per-assignment", type=int, default=MAX_TASKS_PER_ASSIGNMENT)
    parser.add_argument("--log-lb", action="store_true", help="Log capacity score tiap assignment")
    parser.add_argument("--timeout", type=float, default=3600.0, help="Batas total run (detik)")
    parser.add_argument("--crash-worker-index", type=int, default=None, help="Index Worker (0-based) yang crash")
    parser.add_argument("--crash-on-task", type=int, default=1, help="Crash saat menerima task ke-N")
    parser.add_argument("--port", type=int, default=0, help="Port Master (0 = pilih otomatis)")
    args = parser.parse_args()

    if args.workers < 1:
        raise SystemExit("[ERROR] --workers minimal 1")
    if not os.path.exists(args.video):
        raise SystemExit(
            f"[ERROR] Video tidak ditemukan: {args.video}\n"
            "Buat sampel: python scripts/make_sample_video.py"
        )
    try:
        target_res = parse_resolution(args.target_res)
    except ValueError as exc:
        raise SystemExit(f"[ERROR] {exc}") from exc

    port = args.port or find_free_port()
    stamp = time.strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(RUNS_DIR, f"{stamp}_{args.tag}")
    os.makedirs(run_dir, exist_ok=True)

    master_cmd = [
        sys.executable,
        "-u",
        str(ROOT / "master.py"),
        "--video", os.path.abspath(args.video),
        "--output", os.path.abspath(args.output),
        "--workers", str(args.workers),
        "--lb", args.lb,
        "--batch-size", str(args.batch_size),
        "--target-res", format_resolution(target_res),
        "--cache-dir", os.path.abspath(args.cache_dir),
        "--jpeg-quality", str(args.jpeg_quality),
        "--csv", os.path.abspath(args.csv),
        "--tag", args.tag,
        "--port", str(port),
        "--hb-timeout", str(args.hb_timeout),
        "--startup-timeout", str(args.startup_timeout),
        "--no-worker-timeout", str(args.no_worker_timeout),
        "--max-tasks-per-assignment", str(args.max_tasks_per_assignment),
        "--chunks-per-core", str(args.chunks_per_core),
        "--task-timeout", str(args.task_timeout),
        "--no-discovery",
        "--no-confirm-start",
    ]
    master_cmd.append("--local-worker" if args.local_worker else "--no-local-worker")
    if args.task_frames:
        master_cmd.extend(["--task-frames", str(args.task_frames)])
    if args.log_lb:
        master_cmd.append("--log-lb")
    if args.scenario_name:
        master_cmd.extend(["--scenario-name", args.scenario_name])
    if args.frame_limit:
        master_cmd.extend(["--frame-limit", str(args.frame_limit)])
    if args.rebuild_cache:
        master_cmd.append("--rebuild-cache")
    if args.baseline_time is not None:
        master_cmd.extend(["--baseline-time", str(args.baseline_time)])

    master_log_path = os.path.join(run_dir, "master.log")
    master_log = open(master_log_path, "w", encoding="utf-8", errors="replace")
    print(f"[LOCAL] Menjalankan Master di port {port} ...", flush=True)
    master_proc = subprocess.Popen(
        master_cmd,
        cwd=str(ROOT),
        stdout=master_log,
        stderr=subprocess.STDOUT,
    )

    worker_procs: list[tuple[subprocess.Popen, object, str]] = []
    exit_code = 1
    try:
        if not wait_for_port(port, master_proc, timeout=30.0):
            print("[LOCAL] Master gagal start. Lihat log:", master_log_path)
            exit_code = master_proc.poll() or 1
        else:
            for index in range(args.workers):
                worker_id = f"WORKER-{index + 1:02d}"
                worker_cmd = [
                    sys.executable,
                    "-u",
                    str(ROOT / "worker.py"),
                    "--master-ip", "127.0.0.1",
                    "--port", str(port),
                    "--id", worker_id,
                    "--cores", str(max(1, args.cores_per_worker)),
                    "--target-res", format_resolution(target_res),
                    "--hb-interval", str(HEARTBEAT_INTERVAL),
                ]
                if args.crash_worker_index == index:
                    worker_cmd.extend(["--crash-on-task", str(max(1, args.crash_on_task))])
                    print(
                        f"[LOCAL] {worker_id} dijadwalkan CRASH pada task ke-{args.crash_on_task}.",
                        flush=True,
                    )
                worker_log_path = os.path.join(run_dir, f"{worker_id.lower()}.log")
                worker_log = open(worker_log_path, "w", encoding="utf-8", errors="replace")
                worker_proc = subprocess.Popen(
                    worker_cmd,
                    cwd=str(ROOT),
                    stdout=worker_log,
                    stderr=subprocess.STDOUT,
                )
                worker_procs.append((worker_proc, worker_log, worker_id))
                print(f"[LOCAL] {worker_id} start (PID {worker_proc.pid}).", flush=True)
                time.sleep(0.2)

            print("[LOCAL] Menunggu eksperimen selesai ...", flush=True)
            try:
                exit_code = master_proc.wait(timeout=args.timeout)
            except subprocess.TimeoutExpired:
                print(f"[LOCAL] Timeout {args.timeout:.0f}s; menghentikan semua proses.")
                exit_code = 124
    finally:
        terminate_process(master_proc)
        # Beri kesempatan Worker menerima SHUTDOWN dan keluar rapi.
        for worker_proc, worker_log, worker_id in worker_procs:
            if worker_proc.poll() is None:
                try:
                    worker_proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    terminate_process(worker_proc)
            try:
                worker_log.close()
            except OSError:
                pass
            print(f"[LOCAL] {worker_id} stop (exit {worker_proc.returncode}).", flush=True)
        try:
            master_log.close()
        except OSError:
            pass

    print("\n" + "=" * 64)
    print("LOCAL CLUSTER SELESAI")
    print("=" * 64)
    print(f"Master exit code : {exit_code}")
    print(f"Log directory    : {run_dir}")
    if os.path.exists(args.output):
        frames = count_video_frames(args.output)
        print(f"Output video     : {args.output} ({frames} frame)")
    print(f"Metrik CSV       : {os.path.abspath(args.csv)}")
    print("=" * 64)
    print_report(args.csv, tag=args.tag)
    return 0 if exit_code == 0 else int(exit_code or 1)


if __name__ == "__main__":
    raise SystemExit(main())
