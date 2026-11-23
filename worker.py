# worker.py
"""Worker Node: koneksi ke Master, heartbeat terpisah, dan eksekusi batch
frame secara paralel dengan seluruh core CPU lokal."""
from __future__ import annotations

import argparse
import multiprocessing
import os
import socket
import sys
import threading
import time
import traceback
from concurrent.futures import ProcessPoolExecutor
from functools import partial

import psutil

from config import (
    CONNECT_TIMEOUT,
    DEFAULT_MASTER_IP,
    DISCOVERY_PORT,
    DISCOVERY_TIMEOUT,
    HEARTBEAT_INTERVAL,
    MASTER_PORT,
    SOCKET_TIMEOUT,
    TARGET_RESOLUTION,
    WAIT_POLL_INTERVAL,
    format_resolution,
    parse_resolution,
)
from discovery import discover_master
from processor import process_encoded_frame, warmup
from protocol import (
    MSG_ASSIGN_TASK,
    MSG_ERROR,
    MSG_HEARTBEAT,
    MSG_REGISTER,
    MSG_REQUEST_TASK,
    MSG_SHUTDOWN,
    MSG_SUBMIT_RESULT,
    MSG_WAIT,
    ProtocolError,
    configure_socket,
    connect_with_retry,
    recv_packet,
    send_packet,
)


class WorkerNode:
    def __init__(
        self,
        master_ip: str,
        master_port: int,
        worker_id: str,
        cores: int,
        target_res: tuple[int, int] = TARGET_RESOLUTION,
        hb_interval: float = HEARTBEAT_INTERVAL,
        connect_timeout: float = CONNECT_TIMEOUT,
        crash_on_task: int | None = None,
        discovery_port: int = DISCOVERY_PORT,
        discovery_timeout: float = DISCOVERY_TIMEOUT,
    ):
        self.master_ip = master_ip
        self.master_port = int(master_port)
        self.discovery_port = int(discovery_port)
        self.discovery_timeout = max(1.0, float(discovery_timeout))
        self.used_discovery = False
        self.worker_id = worker_id
        self.cores = max(1, int(cores))
        self.target_res = target_res
        self.hb_interval = max(0.2, float(hb_interval))
        self.connect_timeout = max(1.0, float(connect_timeout))
        self.crash_on_task = crash_on_task

        self.running = True
        self.task_sock: socket.socket | None = None
        self.hb_sock: socket.socket | None = None
        self.hb_failures = 0
        self.tasks_received = 0
        self.tasks_done = 0
        self.pool: ProcessPoolExecutor | None = None
        self.stop_event = threading.Event()
        psutil.cpu_percent(interval=None)  # prime baseline

    def log(self, message: str) -> None:
        print(f"[{self.worker_id}] {message}", flush=True)

    # ------------------------------------------------------------------
    # Auto-discovery Master
    # ------------------------------------------------------------------
    def _resolve_master(self) -> tuple[str, int] | None:
        """Kembalikan (ip, port) Master; 'auto' memicu UDP broadcast + TCP sweep."""
        if self.master_ip and self.master_ip.lower() not in ("auto", "discover"):
            return self.master_ip, self.master_port

        self.log(
            f"Mencari Master di LAN (UDP broadcast port {self.discovery_port}) ..."
        )
        self.used_discovery = True
        found = discover_master(
            discovery_port=self.discovery_port,
            tcp_port=self.master_port,
            timeout=self.discovery_timeout,
            stop_event=self.stop_event,
            label=self.worker_id,
        )
        if found is None:
            return None
        return found[0], found[1]

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------
    def _connect_heartbeat_socket(self) -> socket.socket | None:
        sock = connect_with_retry(
            self.master_ip,
            self.master_port,
            timeout=min(self.connect_timeout, 10.0),
            stop_event=self.stop_event,
            label=self.worker_id,
        )
        return sock

    def heartbeat_sender(self) -> None:
        """Kirim heartbeat + telemetri CPU/RAM lewat koneksi terpisah.

        Koneksi terpisah mencegah heartbeat terblokir saat Worker sedang
        mengirim hasil batch yang besar di koneksi task.
        """
        while self.running and not self.stop_event.is_set():
            try:
                if self.hb_sock is None:
                    self.hb_sock = self._connect_heartbeat_socket()
                    if self.hb_sock is None:
                        self.stop_event.wait(self.hb_interval)
                        continue
                telemetry = {
                    "worker_id": self.worker_id,
                    "cpu_usage": psutil.cpu_percent(interval=None),
                    "ram_usage": psutil.virtual_memory().percent,
                    "tasks_done": self.tasks_done,
                }
                send_packet(self.hb_sock, MSG_HEARTBEAT, telemetry)
                self.hb_failures = 0
            except OSError:
                self.hb_failures += 1
                try:
                    if self.hb_sock is not None:
                        self.hb_sock.close()
                except OSError:
                    pass
                self.hb_sock = None
                if self.hb_failures >= 3 and self.task_sock is not None:
                    self.log("Koneksi heartbeat gagal 3x; menghentikan Worker.")
                    self.running = False
                    try:
                        self.task_sock.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    break
            self.stop_event.wait(self.hb_interval)

    # ------------------------------------------------------------------
    # Eksekusi task
    # ------------------------------------------------------------------
    def _create_pool(self) -> None:
        if self.cores <= 1:
            self.log("Mode 1 core: memakai eksekusi sekuensial (tanpa process pool).")
            return
        self.pool = ProcessPoolExecutor(
            max_workers=self.cores,
            mp_context=multiprocessing.get_context("spawn"),
        )
        self.log(f"ProcessPoolExecutor siap dengan {self.cores} core.")

    def _process_encoded_frames(self, encoded_frames: list[bytes], target_res: tuple[int, int]) -> list[bytes]:
        """Proses byte JPEG -> byte PNG di proses anak (IPC tetap kecil)."""
        worker_fn = partial(process_encoded_frame, target_res=target_res)
        if self.pool is not None:
            return list(self.pool.map(worker_fn, encoded_frames, chunksize=1))
        return [process_encoded_frame(blob, target_res) for blob in encoded_frames]

    def _crash_now(self) -> None:
        """Simulasi node failure: hentikan process pool lalu keluar tanpa cleanup socket."""
        for child in multiprocessing.active_children():
            try:
                child.terminate()
            except OSError:
                pass
        os._exit(137)

    def _execute_task(self, payload: dict) -> None:
        task_id = int(payload["task_id"])
        encoded_frames = payload.get("frames") or []
        target_res = tuple(payload.get("target_res") or self.target_res)

        if self.crash_on_task is not None and self.tasks_received == self.crash_on_task:
            self.log(
                f"SIMULASI FAULT TOLERANCE: crash paksa saat menerima task #{task_id:03d} "
                f"(crash-on-task={self.crash_on_task})."
            )
            self._crash_now()

        self.log(
            f"Memproses Task #{task_id:03d} "
            f"({len(encoded_frames)} frame) di {self.cores} core..."
        )
        started = time.time()
        try:
            encoded_results = self._process_encoded_frames(encoded_frames, target_res)
            proc_time = time.time() - started
        except Exception as exc:  # noqa: BLE001 - laporkan ke Master agar task di-requeue
            error_text = f"{type(exc).__name__}: {exc}"
            self.log(f"Task #{task_id:03d} GAGAL: {error_text}")
            traceback.print_exc()
            send_packet(
                self.task_sock,
                MSG_ERROR,
                {"worker_id": self.worker_id, "task_id": task_id, "error": error_text},
            )
            return

        send_packet(
            self.task_sock,
            MSG_SUBMIT_RESULT,
            {
                "worker_id": self.worker_id,
                "task_id": task_id,
                "frames": encoded_results,
                "frame_count": len(encoded_results),
                "proc_time": proc_time,
            },
        )
        self.tasks_done += 1
        self.log(f"Task #{task_id:03d} terkirim ({proc_time:.2f}s, {len(encoded_results)} frame).")

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------
    def start(self) -> int:
        resolved = self._resolve_master()
        if resolved is None:
            self.log(
                "Master tidak ditemukan. Pastikan Master sudah berjalan, port UDP "
                f"{self.discovery_port} tidak diblokir, atau jalankan manual: "
                "python worker.py --master-ip <IP_MASTER> --id WORKER-01 --cores 4"
            )
            return 1
        self.master_ip, self.master_port = resolved

        self.task_sock = connect_with_retry(
            self.master_ip,
            self.master_port,
            timeout=self.connect_timeout,
            stop_event=self.stop_event,
            label=self.worker_id,
        )
        if self.task_sock is None:
            self.log("Tidak dapat terhubung ke Master.")
            return 1

        self.task_sock.settimeout(SOCKET_TIMEOUT)

        # Siapkan pool SEBELUM registrasi. Master menganggap Worker siap begitu
        # REGISTER diterima, jadi biaya spawn + import OpenCV tidak masuk Tp.
        self._create_pool()
        if self.pool is not None:
            try:
                self.pool.submit(warmup, self.target_res).result(timeout=180)
                self.log("Warmup pool selesai (OpenCV siap di semua proses).")
            except Exception as exc:  # noqa: BLE001 - warmup gagal tidak fatal
                self.log(f"Peringatan: warmup pool gagal ({exc}).")

        ram_gb = psutil.virtual_memory().total / (1024 ** 3)
        send_packet(
            self.task_sock,
            MSG_REGISTER,
            {
                "worker_id": self.worker_id,
                "cpu_cores": self.cores,
                "ram_gb": ram_gb,
                "hostname": socket.gethostname(),
            },
        )
        mode = "auto-discovery" if self.used_discovery else "manual"
        self.log(
            f"Terhubung ke Master {self.master_ip}:{self.master_port} "
            f"({self.cores} CPU cores, {mode})."
        )
        # Heartbeat memakai socket terpisah agar tidak terblokir transfer hasil besar.
        self.hb_sock = self._connect_heartbeat_socket()
        if self.hb_sock is None:
            self.log("Peringatan: koneksi heartbeat gagal; Master bisa menganggap Worker mati.")
        hb_thread = threading.Thread(
            target=self.heartbeat_sender, daemon=True, name=f"Heartbeat-{self.worker_id}"
        )
        hb_thread.start()

        try:
            while self.running:
                send_packet(self.task_sock, MSG_REQUEST_TASK, {"worker_id": self.worker_id})
                response = recv_packet(self.task_sock)
                if not response:
                    self.log("Koneksi ke Master terputus.")
                    break

                msg_type = response.get("type")
                payload = response.get("payload") or {}

                if msg_type == MSG_ASSIGN_TASK:
                    task_payloads = payload.get("tasks")
                    if not task_payloads and "task_id" in payload:
                        task_payloads = [payload]
                    for task_payload in task_payloads or []:
                        self.tasks_received += 1
                        self._execute_task(task_payload)
                elif msg_type == MSG_WAIT:
                    self.stop_event.wait(WAIT_POLL_INTERVAL)
                elif msg_type == MSG_SHUTDOWN:
                    self.log("Seluruh pekerjaan selesai. Worker berhenti.")
                    self.stop_event.set()
                    break
                elif msg_type == MSG_ERROR:
                    self.log(f"Master melaporkan error: {payload}")
                else:
                    self.log(f"Pesan tidak dikenal dari Master: {msg_type}")

        except KeyboardInterrupt:
            self.log("Dihentikan paksa (simulasi Fault Tolerance).")
        except (ConnectionResetError, BrokenPipeError, ProtocolError, OSError) as exc:
            self.log(f"Koneksi bermasalah: {exc}")
        finally:
            self.running = False
            self.stop_event.set()
            if self.pool is not None:
                self.pool.shutdown(wait=False, cancel_futures=True)
            for sock in (self.task_sock, self.hb_sock):
                if sock is not None:
                    try:
                        sock.close()
                    except OSError:
                        pass
            self.log("Worker berhenti.")
        return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Worker Node - Distributed Video Processing")
    parser.add_argument(
        "--master-ip",
        type=str,
        default=DEFAULT_MASTER_IP,
        help="IP Master, atau 'auto' untuk mencari via UDP broadcast (default)",
    )
    parser.add_argument("--port", type=int, default=MASTER_PORT, help="Port TCP Master")
    parser.add_argument("--discovery-port", type=int, default=DISCOVERY_PORT, help="Port UDP auto-discovery Master")
    parser.add_argument("--discovery-timeout", type=float, default=DISCOVERY_TIMEOUT, help="Batas waktu mencari Master (detik)")
    parser.add_argument("--id", type=str, default=socket.gethostname(), help="Nama unik Worker (mis. WORKER-01)")
    parser.add_argument("--cores", type=int, default=os.cpu_count() or 1, help="Jumlah CPU core yang digunakan")
    parser.add_argument("--target-res", type=str, default=format_resolution(TARGET_RESOLUTION), help="Fallback resolusi target WIDTHxHEIGHT")
    parser.add_argument("--hb-interval", type=float, default=HEARTBEAT_INTERVAL, help="Interval heartbeat (detik)")
    parser.add_argument("--connect-timeout", type=float, default=CONNECT_TIMEOUT, help="Batas waktu menyambung ke Master")
    parser.add_argument(
        "--crash-on-task",
        type=int,
        default=None,
        help="Simulasi fault tolerance: os._exit saat menerima task ke-N (1-based)",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        target_res = parse_resolution(args.target_res)
    except ValueError as exc:
        raise SystemExit(f"[ERROR] {exc}") from exc
    if args.crash_on_task is not None and args.crash_on_task < 1:
        raise SystemExit("[ERROR] --crash-on-task minimal 1")

    worker = WorkerNode(
        master_ip=args.master_ip,
        master_port=args.port,
        worker_id=args.id,
        cores=args.cores,
        target_res=target_res,
        hb_interval=args.hb_interval,
        connect_timeout=args.connect_timeout,
        crash_on_task=args.crash_on_task,
        discovery_port=args.discovery_port,
        discovery_timeout=args.discovery_timeout,
    )
    return worker.start()


if __name__ == "__main__":
    sys.exit(main())
