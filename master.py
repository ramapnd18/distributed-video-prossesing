# master.py
"""Master Node: task decomposition, dynamic/static load balancing,
fault tolerance (heartbeat + timeout task), result aggregation, dan metrik.

Alur penjadwalan mengikuti pola praktikum:
    DISCOVER  -> Worker menemukan Master via UDP broadcast
    DECOMPOSE -> Video dipecah jadi ~total_core x CHUNKS_PER_CORE task kecil
    DISPATCH  -> 1 task per Worker per round-trip (pull-based)
    GUARD     -> heartbeat + timeout per task -> requeue + nonaktifkan Worker
    GATHER    -> dedupe task_id, cek kelengkapan, rekonstruksi video
"""
from __future__ import annotations

import argparse
import math
import multiprocessing
import os
import socket
import threading
import time
from collections import deque
from concurrent.futures import ProcessPoolExecutor
from functools import partial

import psutil

from config import (
    BATCH_SIZE,
    CACHE_DIR,
    CHUNKS_PER_CORE,
    DEFAULT_INPUT_VIDEO,
    DEFAULT_OUTPUT_VIDEO,
    DISCOVERY_ENABLED,
    DISCOVERY_PORT,
    HEARTBEAT_INTERVAL,
    JPEG_QUALITY,
    LOCAL_WORKER_CORES,
    LOCAL_WORKER_ENABLED,
    LOCAL_WORKER_ID,
    LOG_CSV_PATH,
    LOG_DIR,
    MASTER_BIND_HOST,
    MASTER_PORT,
    MAX_TASK_ATTEMPTS,
    MAX_TASKS_PER_ASSIGNMENT,
    MIN_TASK_FRAMES,
    NO_WORKER_TIMEOUT,
    STARTUP_TIMEOUT,
    TARGET_RESOLUTION,
    TASK_TIMEOUT,
    WAIT_POLL_INTERVAL,
    WORKER_TIMEOUT,
    format_resolution,
    parse_resolution,
)
from discovery import get_local_ips, start_master_responder
from processor import process_encoded_frame
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
    recv_packet,
    send_packet,
)
from utils.metrics_logger import (
    MetricsLogger,
    ResourceSampler,
    get_latest_baseline_t1,
    print_report,
    write_dict_csv,
)
from utils.video_utils import (
    build_task_descriptors,
    prepare_frame_cache,
    read_task_frames,
    reconstruct_video,
)


class MasterServer:
    def __init__(
        self,
        video_path: str,
        output_path: str,
        expected_workers: int,
        lb_mode: str,
        batch_size: int,
        target_res: tuple[int, int] = TARGET_RESOLUTION,
        frame_limit: int | None = None,
        cache_dir: str = CACHE_DIR,
        rebuild_cache: bool = False,
        jpeg_quality: int = JPEG_QUALITY,
        hb_timeout: float = WORKER_TIMEOUT,
        startup_timeout: float = STARTUP_TIMEOUT,
        no_worker_timeout: float = NO_WORKER_TIMEOUT,
        max_attempts: int = MAX_TASK_ATTEMPTS,
        max_tasks_per_assignment: int = MAX_TASKS_PER_ASSIGNMENT,
        task_frames: int | None = None,
        chunks_per_core: int = CHUNKS_PER_CORE,
        min_task_frames: int = MIN_TASK_FRAMES,
        task_timeout: float = TASK_TIMEOUT,
        local_worker: bool = LOCAL_WORKER_ENABLED,
        local_cores: int = LOCAL_WORKER_CORES,
        discovery: bool = DISCOVERY_ENABLED,
        discovery_port: int = DISCOVERY_PORT,
        log_lb: bool = False,
        csv_path: str = LOG_CSV_PATH,
        tag: str = "",
        scenario: str | None = None,
        baseline_time: float | None = None,
        bind_host: str = MASTER_BIND_HOST,
        port: int = MASTER_PORT,
    ):
        self.video_path = video_path
        self.output_path = output_path
        self.expected_workers = max(0, int(expected_workers))
        self.lb_mode = lb_mode  # "dynamic" atau "static"
        self.batch_size = max(1, int(batch_size))
        self.target_res = target_res
        self.frame_limit = frame_limit
        self.cache_dir = cache_dir
        self.rebuild_cache = rebuild_cache
        self.jpeg_quality = jpeg_quality
        self.hb_timeout = max(0.5, float(hb_timeout))
        self.startup_timeout = max(0.0, float(startup_timeout))
        self.no_worker_timeout = max(0.0, float(no_worker_timeout))
        self.max_attempts = max(1, int(max_attempts))
        self.max_tasks_per_assignment = max(1, int(max_tasks_per_assignment))
        self.task_frames = int(task_frames) if task_frames else None
        self.chunks_per_core = max(1, int(chunks_per_core))
        self.min_task_frames = max(1, int(min_task_frames))
        self.task_timeout = max(1.0, float(task_timeout))
        self.local_worker = bool(local_worker)
        self.local_cores = (
            max(1, int(local_cores)) if int(local_cores) > 0 else (os.cpu_count() or 1)
        )
        self.discovery = bool(discovery)
        self.discovery_port = int(discovery_port)
        self.discovery_thread: threading.Thread | None = None
        self.master_ips: list[str] = []

        self.log_lb = bool(log_lb)
        self.tag = tag
        self.scenario = scenario or f"distributed_{lb_mode}"
        self.baseline_time = baseline_time
        self.bind_host = bind_host
        self.port = int(port)

        self.meta = None
        self.tasks: list[dict] = []
        self.total_tasks = 0
        self.total_frames = 0
        self.video_fps = 30.0

        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.all_workers_ready = threading.Event()
        self.running = True
        self.processing_started = False
        self.shutting_down = False
        self.shutdown_done = False
        self.status = "initializing"
        self.notes: list[str] = []

        self.first_worker_time: float | None = None
        self.start_time: float | None = None
        self.end_time: float | None = None
        self.last_progress_time: float | None = None

        # Manajemen task
        self.pending_queue: deque[dict] = deque()
        self.static_queues: dict[str, deque[dict]] = {}
        self.in_progress: dict[int, dict] = {}
        self.completed_results: dict[int, list[bytes]] = {}
        self.failed_tasks: dict[int, str] = {}
        self.requeued_count = 0
        self.recovery_events: list[dict] = []
        self.recovery_counter = 0

        # Registry Worker & telemetri
        self.workers: dict[str, dict] = {}
        self.connections: dict[str, socket.socket] = {}
        self.worker_cpu_samples: list[float] = []
        self.worker_ram_samples: list[float] = []
        self.lb_score_rows: list[dict] = []

        # Timeline & output
        self.timeline: list[dict] = []
        self.written_frames = 0
        self.lost_frames = 0

        self.server_sock: socket.socket | None = None
        self.hb_thread: threading.Thread | None = None
        self.local_thread: threading.Thread | None = None
        self.local_pool: ProcessPoolExecutor | None = None
        self.task_frames_used = 0
        self.local_last_sample = 0.0
        self.sampler = ResourceSampler(interval=1.0)
        self.metrics = MetricsLogger(csv_path)
        self.csv_path = os.path.abspath(csv_path)

        stamp = time.strftime("%Y%m%d_%H%M%S")
        safe_tag = self.tag or "run"
        self.distribution_path = os.path.join(LOG_DIR, f"distribution_{safe_tag}_{stamp}.csv")
        self.timeline_path = os.path.join(LOG_DIR, f"task_timeline_{safe_tag}_{stamp}.csv")
        self.lb_scores_path = os.path.join(LOG_DIR, f"lb_scores_{safe_tag}_{stamp}.csv")

    # ------------------------------------------------------------------
    # Logging & persiapan task
    # ------------------------------------------------------------------
    def log(self, message: str) -> None:
        print(message, flush=True)

    def decompose_video(self) -> None:
        """Siapkan cache JPEG di disk dan metadata video.

        Task descriptor belum dibuat karena ukurannya bergantung pada jumlah core
        Worker yang benar-benar terdaftar (`_build_tasks_locked`).
        """
        self.meta = prepare_frame_cache(
            video_path=self.video_path,
            cache_dir=self.cache_dir,
            frame_limit=self.frame_limit,
            jpeg_quality=self.jpeg_quality,
            rebuild=self.rebuild_cache,
        )
        self.video_fps = self.meta.fps
        self.total_frames = self.meta.total_frames
        self.tasks = []
        self.total_tasks = 0
        self.log(
            f"[CACHE] Video siap: {self.total_frames} frame "
            f"({self.meta.width}x{self.meta.height}, {self.video_fps:.2f} FPS)."
        )

    # ------------------------------------------------------------------
    # Dekomposisi task (gaya referensi: task kecil mengikuti jumlah core)
    # ------------------------------------------------------------------
    def _remote_worker_count_locked(self) -> int:
        return sum(1 for info in self.workers.values() if not info.get("is_local"))

    def _total_cores_locked(self) -> int:
        total = sum(
            int(info["cores"])
            for info in self.workers.values()
            if info["status"] == "ALIVE"
        )
        return max(1, total)

    def _compute_task_frames_locked(self) -> int:
        """Ukuran task = total_frame / (total_core * chunks_per_core), dibatasi batch_size."""
        if self.task_frames:
            return max(1, min(self.batch_size, int(self.task_frames)))
        target_tasks = max(1, self._total_cores_locked() * self.chunks_per_core)
        frames_per_task = math.ceil(self.total_frames / target_tasks)
        frames_per_task = max(self.min_task_frames, frames_per_task)
        return max(1, min(self.batch_size, frames_per_task))

    def _build_tasks_locked(self) -> None:
        self.task_frames_used = self._compute_task_frames_locked()
        self.tasks = build_task_descriptors(self.meta, self.task_frames_used)
        self.total_tasks = len(self.tasks)
        self.pending_queue = deque(self.tasks)
        self.log(
            f"\n[DECOMPOSE] Total data: {self.total_frames} frame | "
            f"Total core: {self._total_cores_locked()} core | "
            f"Ukuran task: {self.task_frames_used} frame | "
            f"Jumlah task: {self.total_tasks}"
        )
        self.log("[DISPATCH] Worker yang selesai lebih cepat langsung mengambil task berikutnya.")
        self.log("[GUARD] Task gagal/timeout di-requeue ke Worker lain (maks. retry tetap).\n")

    # ------------------------------------------------------------------
    # Load balancing
    # ------------------------------------------------------------------
    def _alive_worker_ids_locked(self) -> list[str]:
        return [wid for wid, info in self.workers.items() if info["status"] == "ALIVE"]

    @staticmethod
    def _capacity_score(info: dict) -> float:
        """Skor kapasitas Worker dari jumlah core + beban CPU/RAM real-time.

        Pull-based tetap dipakai; skor ini menentukan prioritas prefetch task
        pada mode dynamic (Worker berkapasitas tinggi boleh mengambil >1 batch
        per round-trip).
        """
        cores = max(1, int(info.get("cores", 1)))
        cpu = min(100.0, max(0.0, float(info.get("cpu_usage", 0.0))))
        ram = min(100.0, max(0.0, float(info.get("ram_usage", 0.0))))
        score = cores * (1.0 - 0.60 * cpu / 100.0) * (1.0 - 0.40 * ram / 100.0)
        return round(max(0.01, score), 4)

    def _log_lb_score_locked(self, worker_id: str, event: str) -> None:
        info = self.workers.get(worker_id)
        if not info:
            return
        self.lb_score_rows.append(
            {
                "timestamp": round(time.time(), 6),
                "worker_id": worker_id,
                "event": event,
                "cpu_pct": round(float(info.get("cpu_usage", 0.0)), 2),
                "ram_pct": round(float(info.get("ram_usage", 0.0)), 2),
                "cores": info.get("cores", 1),
                "capacity_score": self._capacity_score(info),
            }
        )

    def _start_processing_locked(self, reason: str) -> None:
        if self.processing_started or not self.workers:
            return
        self.processing_started = True
        self.start_time = time.time()
        self.last_progress_time = self.start_time
        self._build_tasks_locked()
        if self.lb_mode == "static":
            self._prepare_static_locked()
        self.all_workers_ready.set()
        active = len(self._alive_worker_ids_locked())
        remote = self._remote_worker_count_locked()
        local_note = "1 Master lokal" if self.local_worker else "tanpa Master lokal"
        scores_text = ", ".join(
            f"{wid}={self._capacity_score(info):.2f}"
            for wid, info in self.workers.items()
            if info["status"] == "ALIVE"
        )
        self.log(
            f"\n[MASTER] Pemrosesan dimulai ({self.lb_mode.upper()} LB) - {reason}."
        )
        self.log(
            f"[MASTER] Node aktif: {active} ({remote} Worker remote + {local_note}), "
            f"total {self._total_cores_locked()} core."
        )
        self.log(f"[LB] Capacity score awal: {scores_text}\n")

    def _maybe_start_processing_locked(self) -> None:
        if self.processing_started or not self.workers:
            return
        now = time.time()
        remote = self._remote_worker_count_locked()
        if remote >= self.expected_workers:
            reason = (
                f"seluruh {self.expected_workers} Worker remote terdaftar"
                if self.expected_workers > 0
                else "hanya Master lokal (tanpa Worker remote)"
            )
            self._start_processing_locked(reason)
            return
        if (
            self.startup_timeout > 0
            and self.first_worker_time is not None
            and now - self.first_worker_time >= self.startup_timeout
        ):
            self.notes.append(
                f"Startup timeout: hanya {remote}/{self.expected_workers} Worker remote terdaftar"
            )
            self.log(
                f"[WARNING] Startup timeout {self.startup_timeout:.0f}s: "
                f"hanya {remote}/{self.expected_workers} Worker remote terdaftar. "
                "Pemrosesan tetap dimulai."
            )
            self._start_processing_locked("startup timeout tercapai")

    def _prepare_static_locked(self) -> None:
        """Bagi rata task di awal ke seluruh Worker (mode static)."""
        worker_ids = self._alive_worker_ids_locked()
        self.static_queues = {wid: deque() for wid in worker_ids}
        if not worker_ids:
            return
        index = 0
        while self.pending_queue:
            task = self.pending_queue.popleft()
            target_wid = worker_ids[index % len(worker_ids)]
            self.static_queues[target_wid].append(task)
            index += 1
        self.log(
            f"[MASTER] Static Load Balancing: {self.total_tasks} task dibagi rata "
            f"ke {len(worker_ids)} Worker."
        )

    def _pick_tasks_locked(self, worker_id: str) -> tuple[str, list[dict]]:
        if not self.processing_started:
            return MSG_WAIT, []
        if len(self.completed_results) + len(self.failed_tasks) >= self.total_tasks:
            return MSG_SHUTDOWN, []

        # 1. Prioritaskan task hasil requeue (fault tolerance) pada mode apa pun.
        if self.pending_queue:
            tasks = [self.pending_queue.popleft()]
            if self.lb_mode == "dynamic" and self.max_tasks_per_assignment > 1:
                info = self.workers.get(worker_id)
                alive = self._alive_worker_ids_locked()
                # Prefetch hanya jika antrean masih cukup untuk Worker lain.
                if info and len(self.pending_queue) > len(alive):
                    scores = [self._capacity_score(self.workers[wid]) for wid in alive]
                    max_score = max(scores) if scores else 0.0
                    if max_score > 0 and self._capacity_score(info) >= 0.75 * max_score:
                        extra = min(
                            self.max_tasks_per_assignment - 1,
                            len(self.pending_queue),
                        )
                        for _ in range(extra):
                            tasks.append(self.pending_queue.popleft())
            return MSG_ASSIGN_TASK, tasks

        # 2. Mode static: ambil dari antrean partisi milik Worker ini.
        if self.lb_mode == "static":
            worker_queue = self.static_queues.get(worker_id)
            if worker_queue:
                return MSG_ASSIGN_TASK, [worker_queue.popleft()]

        # 3. Belum selesai, tetapi sisa task sedang dikerjakan Worker lain.
        return MSG_WAIT, []

    def _assign_task_locked(self, worker_id: str, task: dict) -> None:
        task["attempts"] = int(task.get("attempts", 0)) + 1
        now = time.time()
        task["assigned_at"] = now
        task["worker_id"] = worker_id
        self.in_progress[int(task["task_id"])] = {
            "worker_id": worker_id,
            "task": task,
            "assigned_at": now,
        }
        info = self.workers.get(worker_id)
        score = self._capacity_score(info) if info else 0.0
        task["assigned_score"] = score
        if info:
            info["tasks_assigned"] += 1
        self.last_progress_time = now
        self._record_redispatch_locked(task, now)
        self._log_lb_score_locked(worker_id, "assign")
        self.timeline.append(
            {
                "timestamp": round(now, 6),
                "timestamp_iso": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now)),
                "task_id": task["task_id"],
                "event": "assigned",
                "worker_id": worker_id,
                "attempt": task["attempts"],
                "proc_time_sec": "",
                "detail": (
                    f"frame {task['start_frame']}-{task['end_frame'] - 1}; "
                    f"score={score:.2f}"
                ),
            }
        )
        if self.log_lb:
            self.log(
                f"[LB] Task #{task['task_id']:03d} -> {worker_id} "
                f"(score={score:.2f}, attempt={task['attempts']})"
            )

    def _record_redispatch_locked(self, task: dict, now: float) -> None:
        """Catat waktu task hasil requeue diambil Worker sehat (Recovery Time)."""
        event_id = task.get("_failure_event_id")
        if event_id is None:
            return
        for event in self.recovery_events:
            if event["event_id"] == event_id:
                event["redispatch"].append(now)
                if len(event["redispatch"]) >= event["tasks_total"]:
                    event["recovery_time_sec"] = round(
                        max(event["redispatch"]) - event["detected_at"], 4
                    )
                break

    def _requeue_task_locked(
        self,
        task: dict,
        reason: str,
        failure_event_id: int | None = None,
        detected_at: float | None = None,
    ) -> None:
        task_id = int(task["task_id"])
        self.in_progress.pop(task_id, None)
        self.requeued_count += 1
        if failure_event_id is not None:
            task["_failure_event_id"] = failure_event_id
            task["_failure_detected_at"] = detected_at
        now = time.time()
        self.last_progress_time = now

        if int(task.get("attempts", 0)) >= self.max_attempts:
            self.failed_tasks[task_id] = f"melebihi {self.max_attempts} attempt: {reason}"
            self.timeline.append(
                {
                    "timestamp": round(now, 6),
                    "timestamp_iso": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now)),
                    "task_id": task_id,
                    "event": "failed",
                    "worker_id": task.get("worker_id", ""),
                    "attempt": task.get("attempts", 0),
                    "proc_time_sec": "",
                    "detail": reason,
                }
            )
            self.log(f"[FAULT TOLERANCE] Task #{task_id:03d} GAGAL permanen: {reason}")
        else:
            self.pending_queue.appendleft(task)
            self.timeline.append(
                {
                    "timestamp": round(now, 6),
                    "timestamp_iso": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now)),
                    "task_id": task_id,
                    "event": "requeued",
                    "worker_id": task.get("worker_id", ""),
                    "attempt": task.get("attempts", 0),
                    "proc_time_sec": "",
                    "detail": reason,
                }
            )
            self.log(
                f"[FAULT TOLERANCE] Task #{task_id:03d} dikembalikan ke antrean "
                f"(attempt {task.get('attempts', 0)}/{self.max_attempts}): {reason}"
            )
        self._check_completion_locked()

    def _check_completion_locked(self) -> None:
        if self.total_tasks <= 0:
            return
        if len(self.completed_results) + len(self.failed_tasks) >= self.total_tasks:
            if self.end_time is None:
                self.end_time = time.time()
                self.status = "completed" if not self.failed_tasks else "completed_with_failures"
                self.log(
                    f"\n[MASTER] Seluruh task selesai "
                    f"({len(self.completed_results)} sukses, {len(self.failed_tasks)} gagal)."
                )
            self.running = False
            self.stop_event.set()
            self.all_workers_ready.set()

    def _accept_result_locked(
        self,
        worker_id: str,
        task_id: int,
        frames: list[bytes],
        proc_time: float,
    ) -> bool:
        """Terima hasil task (dipakai Worker remote maupun Master lokal).

        Dedupe: hasil yang task_id-nya sudah tidak IN_PROGRESS diabaikan agar
        tidak ada frame yang dihitung dua kali.
        """
        item = self.in_progress.get(task_id)
        if not item or item["worker_id"] != worker_id:
            self.log(f"[MASTER] Hasil duplikat/asing untuk task #{task_id:03d} diabaikan.")
            return False

        task = item["task"]
        del self.in_progress[task_id]
        expected = int(task["expected_frames"])
        if len(frames) != expected:
            self._requeue_task_locked(
                task,
                f"jumlah frame hasil tidak sesuai ({len(frames)}/{expected})",
            )
            return False

        self.completed_results[task_id] = frames
        info = self.workers.get(worker_id)
        if info:
            info["tasks_done"] += 1
            info["total_proc_time"] += proc_time
            info["last_hb"] = time.time()
        now = time.time()
        self.last_progress_time = now
        self.timeline.append(
            {
                "timestamp": round(now, 6),
                "timestamp_iso": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now)),
                "task_id": task_id,
                "event": "completed",
                "worker_id": worker_id,
                "attempt": task.get("attempts", 0),
                "proc_time_sec": round(proc_time, 6),
                "detail": "",
            }
        )
        done = len(self.completed_results)
        self.log(
            f"[OK] Task #{task_id:03d} <- {worker_id} ({proc_time:.2f}s) | "
            f"Selesai: {done}/{self.total_tasks}"
        )
        self._check_completion_locked()
        return True

    # ------------------------------------------------------------------
    # Master sebagai Worker lokal (is_local, seperti referensi praktikum)
    # ------------------------------------------------------------------
    def _register_local_worker_locked(self) -> None:
        if not self.local_worker or LOCAL_WORKER_ID in self.workers:
            return
        now = time.time()
        self.workers[LOCAL_WORKER_ID] = {
            "addr": ("127.0.0.1", self.port),
            "cores": self.local_cores,
            "ram_gb": round(psutil.virtual_memory().total / (1024 ** 3), 2),
            "cpu_usage": 0.0,
            "ram_usage": 0.0,
            "cpu_samples": [],
            "ram_samples": [],
            "capacity_score": float(self.local_cores),
            "last_hb": now,
            "status": "ALIVE",
            "tasks_done": 0,
            "tasks_assigned": 0,
            "total_proc_time": 0.0,
            "death_reason": "",
            "is_local": True,
        }
        if self.first_worker_time is None:
            self.first_worker_time = now
        self.log(
            f"[LOCAL] Master ikut komputasi sebagai {LOCAL_WORKER_ID} "
            f"({self.local_cores} core, is_local=True)."
        )

    def _local_process(self, encoded_frames: list[bytes]) -> list[bytes]:
        if self.local_cores <= 1 or self.local_pool is None:
            return [
                process_encoded_frame(blob, self.target_res) for blob in encoded_frames
            ]
        worker_fn = partial(process_encoded_frame, target_res=self.target_res)
        return list(self.local_pool.map(worker_fn, encoded_frames, chunksize=1))

    def _local_execute_task(self, task: dict) -> None:
        task_id = int(task["task_id"])
        try:
            started = time.time()
            encoded_frames = read_task_frames(self.meta, task)
            encoded_results = self._local_process(encoded_frames)
            proc_time = time.time() - started
        except Exception as exc:  # noqa: BLE001 - task harus di-requeue, bukan mematikan Master
            self.log(f"[ERROR] {LOCAL_WORKER_ID} gagal task #{task_id:03d}: {exc}")
            with self.lock:
                if task_id in self.in_progress:
                    self._requeue_task_locked(task, f"Master lokal error: {exc}")
            return
        with self.lock:
            self._accept_result_locked(LOCAL_WORKER_ID, task_id, encoded_results, proc_time)

    def local_worker_loop(self) -> None:
        """Loop Worker lokal: tarik task dari antrean yang sama dengan Worker remote."""
        if self.local_cores > 1:
            try:
                self.local_pool = ProcessPoolExecutor(
                    max_workers=self.local_cores,
                    mp_context=multiprocessing.get_context("spawn"),
                )
                self.log(
                    f"[LOCAL] ProcessPoolExecutor {self.local_cores} core siap."
                )
            except Exception as exc:  # noqa: BLE001 - fallback ke eksekusi sekuensial
                self.local_pool = None
                self.log(f"[LOCAL] Process pool gagal dibuat ({exc}); fallback sekuensial.")

        while self.running and not self.stop_event.is_set():
            with self.lock:
                info = self.workers.get(LOCAL_WORKER_ID)
                if not info or info["status"] != "ALIVE":
                    break
                if not self.processing_started:
                    action, tasks = MSG_WAIT, []
                else:
                    action, tasks = self._pick_tasks_locked(LOCAL_WORKER_ID)
                    for task in tasks:
                        self._assign_task_locked(LOCAL_WORKER_ID, task)
            if action != MSG_ASSIGN_TASK or not tasks:
                self.stop_event.wait(WAIT_POLL_INTERVAL)
                continue
            for task in tasks:
                self._local_execute_task(task)

        if self.local_pool is not None:
            self.local_pool.shutdown(wait=False, cancel_futures=True)
            self.local_pool = None

    # ------------------------------------------------------------------
    # Fault tolerance
    # ------------------------------------------------------------------
    def _mark_worker_dead_locked(self, worker_id: str, reason: str) -> None:
        info = self.workers.get(worker_id)
        if not info or info["status"] == "DEAD":
            return
        now = time.time()
        info["status"] = "DEAD"
        info["death_time"] = now
        info["death_reason"] = reason
        self._log_lb_score_locked(worker_id, "dead")

        self.recovery_counter += 1
        event = {
            "event_id": self.recovery_counter,
            "worker_id": worker_id,
            "detected_at": now,
            "reason": reason,
            "requeued_tasks": [],
            "tasks_total": 0,
            "redispatch": [],
            "recovery_time_sec": None,
        }

        tasks_to_requeue: list[dict] = []
        for task_id, item in list(self.in_progress.items()):
            if item["worker_id"] == worker_id:
                tasks_to_requeue.append(item["task"])
                del self.in_progress[task_id]

        # Pada mode static, sisa task yang belum dikerjakan juga harus diselamatkan.
        if self.lb_mode == "static":
            static_queue = self.static_queues.pop(worker_id, deque())
            tasks_to_requeue.extend(static_queue)

        for task in tasks_to_requeue:
            event["requeued_tasks"].append(task["task_id"])
            event["tasks_total"] += 1
            self._requeue_task_locked(
                task,
                reason=reason,
                failure_event_id=event["event_id"],
                detected_at=now,
            )

        if event["tasks_total"] == 0:
            event["recovery_time_sec"] = 0.0
        self.recovery_events.append(event)
        self.last_progress_time = now
        self.log(
            f"\n[FAULT TOLERANCE] Worker '{worker_id}' terdeteksi MATI "
            f"({reason}). {event['tasks_total']} task di-requeue.\n"
        )

    def mark_worker_dead(self, worker_id: str, reason: str) -> None:
        with self.lock:
            self._mark_worker_dead_locked(worker_id, reason)

    def _sample_local_telemetry_locked(self, now: float) -> None:
        """Master lokal tidak punya heartbeat socket; telemetri diambil langsung."""
        info = self.workers.get(LOCAL_WORKER_ID)
        if not info or info["status"] != "ALIVE":
            return
        info["last_hb"] = now
        if now - self.local_last_sample < max(1.0, HEARTBEAT_INTERVAL / 2.0):
            return
        self.local_last_sample = now
        info["cpu_usage"] = psutil.cpu_percent(interval=None)
        info["ram_usage"] = psutil.virtual_memory().percent
        info["cpu_samples"].append(info["cpu_usage"])
        info["ram_samples"].append(info["ram_usage"])
        info["capacity_score"] = self._capacity_score(info)
        self.worker_cpu_samples.append(info["cpu_usage"])
        self.worker_ram_samples.append(info["ram_usage"])

    def heartbeat_monitor(self) -> None:
        """Thread pengawas: heartbeat, timeout task, startup timeout, dan watchdog."""
        interval = max(0.2, min(1.0, self.hb_timeout / 3.0))
        while self.running and not self.stop_event.wait(interval):
            with self.lock:
                now = time.time()
                if not self.processing_started:
                    self._maybe_start_processing_locked()
                    if not self.processing_started:
                        continue

                self._sample_local_telemetry_locked(now)

                for worker_id, info in list(self.workers.items()):
                    if info.get("is_local"):
                        continue
                    if info["status"] == "ALIVE" and (now - info["last_hb"] > self.hb_timeout):
                        self._mark_worker_dead_locked(
                            worker_id,
                            f"tidak ada heartbeat > {self.hb_timeout:.1f}s",
                        )

                # GUARD: satu task tidak boleh menggantung melebihi TASK_TIMEOUT.
                if self.processing_started:
                    for task_id, item in list(self.in_progress.items()):
                        if task_id not in self.in_progress:
                            continue
                        elapsed = now - item["assigned_at"]
                        if elapsed <= self.task_timeout:
                            continue
                        worker_id = item["worker_id"]
                        self.log(
                            f"[TIMEOUT] Task #{task_id:03d} pada {worker_id} "
                            f"({elapsed:.1f}s > {self.task_timeout:.0f}s) "
                            "-> requeue + Worker dinonaktifkan."
                        )
                        self._mark_worker_dead_locked(
                            worker_id,
                            f"timeout task #{task_id:03d} > {self.task_timeout:.0f}s",
                        )

                alive = self._alive_worker_ids_locked()
                has_unfinished = bool(self.pending_queue or self.in_progress or any(self.static_queues.values()))
                if self.processing_started and not alive and has_unfinished:
                    reference = self.last_progress_time or self.start_time or now
                    if self.no_worker_timeout > 0 and now - reference > self.no_worker_timeout:
                        self.notes.append("Tidak ada Worker hidup; eksperimen dibatalkan")
                        self.status = "aborted_no_workers"
                        self.log(
                            f"\n[MASTER] Tidak ada Worker hidup selama "
                            f"{self.no_worker_timeout:.0f}s. Eksperimen dibatalkan."
                        )
                        self.end_time = now
                        self.running = False
                        self.stop_event.set()
                        break

                if len(self.completed_results) + len(self.failed_tasks) >= self.total_tasks:
                    self._check_completion_locked()
                    break

    # ------------------------------------------------------------------
    # Handler koneksi
    # ------------------------------------------------------------------
    def handle_client(self, conn: socket.socket, addr: tuple) -> None:
        worker_id: str | None = None
        try:
            configure_socket(conn)
            first_packet = recv_packet(conn)
            if not first_packet:
                return
            if first_packet["type"] == MSG_HEARTBEAT:
                self._heartbeat_loop(conn, first_packet)
                return

            worker_id = self._handle_message(conn, addr, first_packet, worker_id)
            # Tetap membaca sampai Master mengirim SHUTDOWN / Worker menutup koneksi.
            # Jangan keluar hanya karena stop_event, agar SHUTDOWN sempat terkirim.
            while worker_id and not self.shutting_down:
                packet = recv_packet(conn)
                if not packet:
                    break
                worker_id = self._handle_message(conn, addr, packet, worker_id)
        except (ConnectionResetError, BrokenPipeError, EOFError, OSError, ProtocolError) as exc:
            if not self.shutting_down and not self.stop_event.is_set():
                self.log(f"[MASTER] Koneksi {addr} bermasalah: {exc}")
        finally:
            with self.lock:
                is_current_connection = bool(worker_id and self.connections.get(worker_id) is conn)
                if is_current_connection:
                    del self.connections[worker_id]
            if (
                worker_id
                and is_current_connection
                and not self.shutting_down
                and not self.stop_event.is_set()
            ):
                self.mark_worker_dead(worker_id, "koneksi socket terputus")
            try:
                conn.close()
            except OSError:
                pass

    def _heartbeat_loop(self, conn: socket.socket, first_packet: dict) -> None:
        packet = first_packet
        while self.running and not self.stop_event.is_set():
            if packet.get("type") == MSG_HEARTBEAT:
                payload = packet.get("payload") or {}
                worker_id = payload.get("worker_id")
                with self.lock:
                    info = self.workers.get(worker_id)
                    if info:
                        info["last_hb"] = time.time()
                        info["cpu_usage"] = float(payload.get("cpu_usage", 0.0))
                        info["ram_usage"] = float(payload.get("ram_usage", 0.0))
                        info["cpu_samples"].append(info["cpu_usage"])
                        info["ram_samples"].append(info["ram_usage"])
                        info["capacity_score"] = self._capacity_score(info)
                        info["tasks_done"] = max(
                            int(info["tasks_done"]),
                            int(payload.get("tasks_done", info["tasks_done"])),
                        )
                        self.worker_cpu_samples.append(info["cpu_usage"])
                        self.worker_ram_samples.append(info["ram_usage"])
                        self._log_lb_score_locked(worker_id, "heartbeat")
            packet = recv_packet(conn)
            if not packet:
                break

    def _handle_message(
        self,
        conn: socket.socket,
        addr: tuple,
        packet: dict,
        worker_id: str | None,
    ) -> str | None:
        msg_type = packet.get("type")
        payload = packet.get("payload") or {}

        if msg_type == MSG_REGISTER:
            worker_id = str(payload["worker_id"])
            now = time.time()
            with self.lock:
                previous = self.workers.get(worker_id)
                if previous and previous["status"] == "ALIVE":
                    self.log(f"[REGISTER] Peringatan: Worker '{worker_id}' mendaftar ulang.")
                self.workers[worker_id] = {
                    "addr": addr,
                    "cores": int(payload.get("cpu_cores", 1)),
                    "ram_gb": float(payload.get("ram_gb", 0.0)),
                    "cpu_usage": 0.0,
                    "ram_usage": 0.0,
                    "cpu_samples": [],
                    "ram_samples": [],
                    "capacity_score": float(max(1, int(payload.get("cpu_cores", 1)))),
                    "last_hb": now,
                    "status": "ALIVE",
                    "tasks_done": 0,
                    "tasks_assigned": 0,
                    "total_proc_time": 0.0,
                    "death_reason": "",
                }
                self.connections[worker_id] = conn
                if self.first_worker_time is None:
                    self.first_worker_time = now
                registered = self._remote_worker_count_locked()
                self._log_lb_score_locked(worker_id, "register")
                self.log(
                    f"[REGISTER] {worker_id} bergabung "
                    f"({payload.get('cpu_cores', '?')} cores, "
                    f"{float(payload.get('ram_gb', 0.0)):.1f} GB RAM) "
                    f"[{registered}/{self.expected_workers}]"
                )
                self._maybe_start_processing_locked()
            return worker_id

        if worker_id is None:
            return None

        if msg_type == MSG_HEARTBEAT:
            with self.lock:
                info = self.workers.get(worker_id)
                if info:
                    info["last_hb"] = time.time()
                    info["cpu_usage"] = float(payload.get("cpu_usage", 0.0))
                    info["ram_usage"] = float(payload.get("ram_usage", 0.0))
                    info["cpu_samples"].append(info["cpu_usage"])
                    info["ram_samples"].append(info["ram_usage"])
                    info["capacity_score"] = self._capacity_score(info)
                    self.worker_cpu_samples.append(info["cpu_usage"])
                    self.worker_ram_samples.append(info["ram_usage"])
                    self._log_lb_score_locked(worker_id, "heartbeat")
            return worker_id

        if msg_type == MSG_REQUEST_TASK:
            with self.lock:
                info = self.workers.get(worker_id)
                if info:
                    info["last_hb"] = time.time()
                self._maybe_start_processing_locked()
                action, tasks = self._pick_tasks_locked(worker_id)
                for task in tasks:
                    self._assign_task_locked(worker_id, task)

            if action == MSG_ASSIGN_TASK and tasks:
                assignments = []
                try:
                    for task in tasks:
                        frames = read_task_frames(self.meta, task)
                        assignments.append(
                            {
                                "task_id": task["task_id"],
                                "start_frame": task["start_frame"],
                                "end_frame": task["end_frame"],
                                "expected_frames": task["expected_frames"],
                                "attempt": task["attempts"],
                                "target_res": list(self.target_res),
                                "frames": frames,
                            }
                        )
                except Exception as exc:  # noqa: BLE001 - cache rusak harus di-requeue
                    with self.lock:
                        for task in tasks:
                            if int(task["task_id"]) in self.in_progress:
                                self._requeue_task_locked(
                                    task, f"gagal membaca cache frame: {exc}"
                                )
                    send_packet(conn, MSG_WAIT, {"reason": "cache_error"})
                    return worker_id

                # Sebagian task bisa saja sudah di-requeue monitor saat cache dibaca.
                with self.lock:
                    still_assigned = {
                        int(task["task_id"])
                        for task in tasks
                        if int(task["task_id"]) in self.in_progress
                    }
                assignments = [
                    item for item in assignments if int(item["task_id"]) in still_assigned
                ]
                if not assignments:
                    send_packet(conn, MSG_WAIT, {"reason": "requeued"})
                    return worker_id

                try:
                    send_packet(conn, MSG_ASSIGN_TASK, {"tasks": assignments})
                except OSError:
                    with self.lock:
                        for task in tasks:
                            if int(task["task_id"]) in self.in_progress:
                                self._requeue_task_locked(
                                    task, "gagal mengirim task ke Worker"
                                )
                    raise
            elif action == MSG_SHUTDOWN:
                send_packet(conn, MSG_SHUTDOWN, {"reason": "seluruh task selesai"})
            else:
                send_packet(conn, MSG_WAIT, {"reason": "menunggu task"})
            return worker_id

        if msg_type == MSG_SUBMIT_RESULT:
            task_id = int(payload["task_id"])
            frames = payload.get("frames") or []
            proc_time = float(payload.get("proc_time", 0.0))
            with self.lock:
                self._accept_result_locked(worker_id, task_id, frames, proc_time)
            return worker_id

        if msg_type == MSG_ERROR:
            try:
                task_id = int(payload.get("task_id", -1))
            except (TypeError, ValueError):
                task_id = -1
            reason = str(payload.get("error", "Worker error"))
            with self.lock:
                item = self.in_progress.get(task_id)
                if item and item["worker_id"] == worker_id:
                    self._requeue_task_locked(item["task"], f"Worker error: {reason}")
            return worker_id

        self.log(f"[MASTER] Tipe pesan tidak dikenal dari {worker_id}: {msg_type}")
        return worker_id

    # ------------------------------------------------------------------
    # Shutdown, rekonstruksi, metrik
    # ------------------------------------------------------------------
    def _send_shutdown_to_workers(self) -> None:
        with self.lock:
            connections = list(self.connections.items())
        for worker_id, conn in connections:
            try:
                conn.settimeout(2.0)
                send_packet(conn, MSG_SHUTDOWN, {"reason": "seluruh task selesai"})
                self.log(f"[MASTER] SHUTDOWN dikirim ke {worker_id}.")
            except OSError:
                pass

    def _finalize_recovery_events(self) -> None:
        for event in self.recovery_events:
            if event["recovery_time_sec"] is None:
                if event["redispatch"]:
                    event["recovery_time_sec"] = round(
                        max(event["redispatch"]) - event["detected_at"], 4
                    )
                else:
                    event["recovery_time_sec"] = 0.0

    def _collect_metrics(self) -> dict:
        exec_time = 0.0
        if self.start_time is not None and self.end_time is not None:
            exec_time = max(0.0, self.end_time - self.start_time)
        throughput = self.total_frames / exec_time if exec_time > 0 else 0.0

        # Utamakan telemetri Worker. Jika run terlalu singkat (semua sampel 0),
        # pakai sampler lokal Master sebagai fallback.
        if self.worker_cpu_samples and max(self.worker_cpu_samples) > 0:
            avg_cpu = sum(self.worker_cpu_samples) / len(self.worker_cpu_samples)
            max_cpu = max(self.worker_cpu_samples)
            avg_ram = (
                sum(self.worker_ram_samples) / len(self.worker_ram_samples)
                if self.worker_ram_samples
                else 0.0
            )
            max_ram = max(self.worker_ram_samples) if self.worker_ram_samples else 0.0
        else:
            local = self.sampler.summary()
            avg_cpu = local["avg_cpu_pct"]
            max_cpu = local["max_cpu_pct"]
            avg_ram = local["avg_ram_pct"]
            max_ram = local["max_ram_pct"]

        speedup = ""
        efficiency = ""
        node_efficiency = ""
        total_cores = (
            sum(int(info["cores"]) for info in self.workers.values())
            or self.expected_workers
            or 1
        )
        nodes_total = max(1, len(self.workers))
        baseline = self.baseline_time
        if baseline is None:
            baseline = get_latest_baseline_t1(
                self.csv_path,
                total_frames=self.total_frames,
                target_res=format_resolution(self.target_res),
                tag=self.tag,
            )
        if baseline and baseline > 0 and exec_time > 0:
            speedup_value = baseline / exec_time
            speedup = round(speedup_value, 4)
            efficiency = round((speedup_value / max(1, total_cores)) * 100.0, 2)
            node_efficiency = round((speedup_value / nodes_total) * 100.0, 2)

        recovery_times = [
            event["recovery_time_sec"]
            for event in self.recovery_events
            if event["recovery_time_sec"] is not None
        ]
        recovery_time = max(recovery_times) if recovery_times else 0.0

        cores_values = sorted({int(info["cores"]) for info in self.workers.values()})
        cores_text = "|".join(str(value) for value in cores_values) if cores_values else ""
        distribution_text = "|".join(
            f"{worker_id}:{info['tasks_done']}" for worker_id, info in self.workers.items()
        )

        return {
            "tag": self.tag,
            "scenario": self.scenario,
            "scenario_name": self.scenario,
            "status": self.status,
            "lb_mode": self.lb_mode,
            "workers": self.expected_workers,
            "num_workers": self.expected_workers,
            "workers_seen": self._remote_worker_count_locked(),
            "cores_per_worker": cores_text,
            "total_cores": total_cores,
            "nodes_total": nodes_total,
            "batch_size": self.batch_size,
            "task_frames": self.task_frames_used,
            "target_res": format_resolution(self.target_res),
            "total_frames": self.total_frames,
            "exec_time_sec": round(exec_time, 4),
            "baseline_t1_sec": round(baseline, 4) if baseline else "",
            "throughput_fps": round(throughput, 4),
            "speedup": speedup,
            "efficiency_pct": efficiency,
            "node_efficiency_pct": node_efficiency,
            "avg_cpu_pct": round(avg_cpu, 2),
            "max_cpu_pct": round(max_cpu, 2),
            "avg_ram_pct": round(avg_ram, 2),
            "max_ram_pct": round(max_ram, 2),
            "recovery_time_sec": round(recovery_time, 4),
            "task_timeout_sec": self.task_timeout,
            "requeued_tasks": self.requeued_count,
            "failed_tasks": len(self.failed_tasks),
            "lost_frames": self.lost_frames,
            "worker_task_distribution": distribution_text,
            "local_worker": 1 if self.local_worker else 0,
            "local_cores": self.local_cores if self.local_worker else 0,
            "hostname": socket.gethostname(),
            "cpu_cores": os.cpu_count() or 1,
            "notes": "; ".join(self.notes),
        }

    def _write_auxiliary_logs(self) -> None:
        distribution_rows = []
        for worker_id, info in self.workers.items():
            cpu_samples = info.get("cpu_samples") or []
            ram_samples = info.get("ram_samples") or []
            distribution_rows.append(
                {
                    "worker_id": worker_id,
                    "status": info["status"],
                    "cores": info["cores"],
                    "ram_gb": round(info["ram_gb"], 2),
                    "capacity_score": round(self._capacity_score(info), 4),
                    "tasks_assigned": info["tasks_assigned"],
                    "tasks_done": info["tasks_done"],
                    "total_proc_time_sec": round(info["total_proc_time"], 4),
                    "avg_cpu_pct": round(sum(cpu_samples) / len(cpu_samples), 2) if cpu_samples else 0.0,
                    "avg_ram_pct": round(sum(ram_samples) / len(ram_samples), 2) if ram_samples else 0.0,
                    "last_cpu_pct": round(float(info.get("cpu_usage", 0.0)), 2),
                    "last_ram_pct": round(float(info.get("ram_usage", 0.0)), 2),
                    "death_reason": info.get("death_reason", ""),
                }
            )
        write_dict_csv(self.distribution_path, distribution_rows)

        timeline_rows = sorted(self.timeline, key=lambda row: row["timestamp"])
        write_dict_csv(self.timeline_path, timeline_rows)
        write_dict_csv(self.lb_scores_path, self.lb_score_rows)

    def _print_summary(self, record: dict) -> None:
        print("\n" + "=" * 68)
        print("HASIL EKSEKUSI DISTRIBUTED VIDEO PROCESSING")
        print("=" * 68)
        print(f"Status              : {self.status}")
        print(f"Mode Load Balancing : {self.lb_mode.upper()}")
        print(
            f"Node Komputasi      : {record['nodes_total']} "
            f"({record['workers_seen']}/{self.expected_workers} Worker remote"
            f"{' + Master lokal' if self.local_worker else ''})"
        )
        print(f"Ukuran Task         : {record['task_frames']} frame/task "
              f"(timeout {record['task_timeout_sec']:.0f}s)")
        print(f"Total Task          : {len(self.completed_results)}/{self.total_tasks} task selesai")
        print(f"Total Frame Output  : {self.written_frames}/{self.total_frames} frame -> {self.output_path}")
        print(f"Execution Time (Tp) : {record['exec_time_sec']:.2f} detik")
        print(f"Throughput          : {record['throughput_fps']:.2f} FPS")
        print(
            f"CPU / RAM rata-rata : {record['avg_cpu_pct']:.1f}% / "
            f"{record['avg_ram_pct']:.1f}%"
        )
        if record["speedup"] != "":
            print(
                f"Speedup / Efficiency: {record['speedup']:.3f}x / "
                f"{record['efficiency_pct']:.2f}% (per core) / "
                f"{record['node_efficiency_pct']:.2f}% (per Worker node) "
                f"(T1={record['baseline_t1_sec']}s)"
            )
        if record["lost_frames"]:
            print(f"Frame Hilang        : {record['lost_frames']} (PERIKSA HASIL!)")
        print("-" * 68)
        print("Distribusi Task per Worker:")
        for worker_id, info in self.workers.items():
            label = " (lokal)" if info.get("is_local") else ""
            print(
                f"  - {worker_id:<18}{label:<8} | Status: {info['status']:<5} | "
                f"Cores: {info['cores']:<2} | Task: {info['tasks_done']}/{info['tasks_assigned']}"
            )
        print("-" * 68)
        missing_frames = max(0, self.total_frames - self.written_frames)
        if missing_frames or self.failed_tasks:
            print(
                f"[WARNING] {missing_frames} frame belum terselesaikan "
                f"({len(self.failed_tasks)} task gagal permanen). Periksa log!"
            )
        else:
            print("[OK] Seluruh workload berhasil diselesaikan (0 frame hilang).")
        if self.recovery_events:
            print("-" * 68)
            print("Log Fault Tolerance (Recovery Time):")
            for event in self.recovery_events:
                recovery = event["recovery_time_sec"]
                recovery_text = f"{recovery:.4f}s" if recovery is not None else "n/a"
                print(
                    f"  - {event['worker_id']} mati ({event['reason']}) -> "
                    f"{event['tasks_total']} task di-requeue, recovery {recovery_text}"
                )
        print("-" * 68)
        print(f"Metrik CSV          : {self.csv_path}")
        print(f"Distribusi Worker   : {self.distribution_path}")
        print(f"Timeline Task       : {self.timeline_path}")
        print(f"Skor Load Balancing : {self.lb_scores_path}")
        print("=" * 68)

    def _shutdown(self) -> None:
        if self.shutdown_done:
            return
        self.shutdown_done = True
        self.shutting_down = True
        self.running = False
        self.stop_event.set()
        self.all_workers_ready.set()

        if self.server_sock is not None:
            try:
                self.server_sock.close()
            except OSError:
                pass

        # Beri kesempatan Worker lokal menyelesaikan task yang sedang berjalan
        # sebelum video direkonstruksi, agar tidak ada frame yang hilang.
        if self.local_thread is not None and self.local_thread.is_alive():
            self.local_thread.join(timeout=max(5.0, min(self.task_timeout, 30.0)))
        if self.local_pool is not None:
            try:
                self.local_pool.shutdown(wait=False, cancel_futures=True)
            except Exception:  # noqa: BLE001 - shutdown pool tidak boleh menggagalkan cleanup
                pass
            self.local_pool = None
        if self.discovery_thread is not None and self.discovery_thread.is_alive():
            self.discovery_thread.join(timeout=1.0)

        self._send_shutdown_to_workers()

        if self.hb_thread is not None:
            self.hb_thread.join(timeout=2.0)
        self.sampler.stop()

        if self.end_time is None:
            self.end_time = time.time()
            if self.status in ("initializing", "running"):
                self.status = "interrupted"

        self._finalize_recovery_events()

        try:
            if self.meta is not None:
                self.written_frames, self.lost_frames = reconstruct_video(
                    completed_results=self.completed_results,
                    output_path=self.output_path,
                    fps=self.video_fps,
                    target_size=self.target_res,
                    total_tasks=self.total_tasks,
                    expected_frames=self.total_frames,
                )
        except Exception as exc:  # noqa: BLE001 - tetap tulis metrik walau rekonstruksi gagal
            self.notes.append(f"Rekonstruksi gagal: {exc}")
            self.status = "reconstruction_failed"
            self.log(f"[ERROR] Rekonstruksi video gagal: {exc}")

        record = self._collect_metrics()
        try:
            self.metrics.log(record)
            self._write_auxiliary_logs()
        except OSError as exc:
            self.log(f"[WARNING] Gagal menulis metrik: {exc}")

        self._print_summary(record)

    # ------------------------------------------------------------------
    # Server loop
    # ------------------------------------------------------------------
    def start(self) -> None:
        self.decompose_video()
        self.sampler.start()

        with self.lock:
            self._register_local_worker_locked()

        if self.discovery:
            self.master_ips = get_local_ips()
            self.discovery_thread = start_master_responder(
                discovery_port=self.discovery_port,
                tcp_port=self.port,
                expected_workers=self.expected_workers,
                stop_event=self.stop_event,
                bind_host="" if self.bind_host in ("0.0.0.0", "") else self.bind_host,
            )
            self.log(
                f"[DISCOVER] Auto-discovery aktif di UDP {self.discovery_port}. "
                f"IP Master: {', '.join(self.master_ips)}"
            )
        else:
            self.log(
                "[DISCOVER] Auto-discovery dimatikan; Worker harus memakai --master-ip."
            )

        self.hb_thread = threading.Thread(
            target=self.heartbeat_monitor, daemon=True, name="HeartbeatMonitor"
        )
        self.hb_thread.start()

        if self.local_worker:
            self.local_thread = threading.Thread(
                target=self.local_worker_loop, daemon=True, name="LocalWorker"
            )
            self.local_thread.start()

        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            server_sock.bind((self.bind_host, self.port))
        except OSError as exc:
            self.log(
                f"[ERROR] Gagal bind {self.bind_host}:{self.port}: {exc}. "
                "Port mungkin dipakai proses lain; gunakan --port lain."
            )
            server_sock.close()
            self._shutdown()
            raise RuntimeError(f"Gagal bind {self.bind_host}:{self.port}") from exc
        server_sock.listen(64)
        server_sock.settimeout(0.5)
        self.server_sock = server_sock

        self.status = "running"
        self.log(
            f"[MASTER] Mendengarkan koneksi di {self.bind_host}:{self.port} "
            f"(Menunggu {self.expected_workers} Worker remote, LB={self.lb_mode.upper()})."
        )
        if self.discovery:
            self.log(
                f"[DISCOVER] Di tiap PC Worker cukup jalankan: "
                f"python worker.py --id WORKER-01 --cores 4"
            )
        else:
            self.log(
                f"[MASTER] Di tiap PC Worker: "
                f"python worker.py --master-ip {self.master_ips[0] if self.master_ips else '<IP>'} "
                "--id WORKER-01 --cores 4"
            )

        try:
            while self.running and not self.stop_event.is_set():
                try:
                    conn, addr = server_sock.accept()
                    thread = threading.Thread(
                        target=self.handle_client,
                        args=(conn, addr),
                        daemon=True,
                        name=f"Client-{addr[0]}:{addr[1]}",
                    )
                    thread.start()
                except socket.timeout:
                    continue
                except OSError:
                    break
        finally:
            self._shutdown()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Master Node - Distributed Video Processing")
    parser.add_argument("--video", type=str, default=DEFAULT_INPUT_VIDEO, help="Path video input")
    parser.add_argument("--output", type=str, default=DEFAULT_OUTPUT_VIDEO, help="Path video output")
    parser.add_argument("--workers", type=int, default=9, help="Jumlah Worker remote yang ditunggu (0 = hanya Master lokal; 1,2,4,6,9)")
    parser.add_argument("--lb", type=str, choices=["dynamic", "static"], default="dynamic", help="Mode Load Balancing")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="Batas atas jumlah frame per task")
    parser.add_argument("--task-frames", type=int, default=None, help="Paksa ukuran task (frame); default otomatis dari total core")
    parser.add_argument("--chunks-per-core", type=int, default=CHUNKS_PER_CORE, help="Target task = total_core x nilai ini (default 4)")
    parser.add_argument("--min-task-frames", type=int, default=MIN_TASK_FRAMES, help="Ukuran task minimum (frame)")
    parser.add_argument("--task-timeout", type=float, default=TASK_TIMEOUT, help="Batas satu task di Worker sebelum di-requeue (detik)")
    parser.add_argument(
        "--local-worker",
        action=argparse.BooleanOptionalAction,
        default=LOCAL_WORKER_ENABLED,
        help="Master ikut komputasi sebagai Worker lokal (default aktif; --no-local-worker untuk koordinator murni)",
    )
    parser.add_argument("--local-cores", type=int, default=LOCAL_WORKER_CORES, help="Core untuk Worker lokal (0 = semua core)")
    parser.add_argument(
        "--discovery",
        action=argparse.BooleanOptionalAction,
        default=DISCOVERY_ENABLED,
        help="Jawab broadcast auto-discovery Worker (default aktif)",
    )
    parser.add_argument("--discovery-port", type=int, default=DISCOVERY_PORT, help="Port UDP auto-discovery")
    parser.add_argument("--target-res", type=str, default=format_resolution(TARGET_RESOLUTION), help="Resolusi output WIDTHxHEIGHT")
    parser.add_argument("--frame-limit", type=int, default=None, help="Batasi jumlah frame (untuk uji cepat)")
    parser.add_argument("--cache-dir", type=str, default=CACHE_DIR, help="Direktori cache frame JPEG")
    parser.add_argument("--rebuild-cache", action="store_true", help="Paksa ekstrak ulang cache frame")
    parser.add_argument("--jpeg-quality", type=int, default=JPEG_QUALITY, help="Kualitas JPEG frame input (1-100)")
    parser.add_argument("--bind", type=str, default=MASTER_BIND_HOST, help="Alamat bind Master")
    parser.add_argument("--port", type=int, default=MASTER_PORT, help="Port TCP Master")
    parser.add_argument("--hb-timeout", type=float, default=WORKER_TIMEOUT, help="Timeout heartbeat Worker (detik)")
    parser.add_argument("--startup-timeout", type=float, default=STARTUP_TIMEOUT, help="Batas menunggu semua Worker (0=tunggu selamanya)")
    parser.add_argument("--no-worker-timeout", type=float, default=NO_WORKER_TIMEOUT, help="Batas tanpa Worker hidup sebelum abort")
    parser.add_argument("--max-attempts", type=int, default=MAX_TASK_ATTEMPTS, help="Batas retry per task")
    parser.add_argument(
        "--max-tasks-per-assignment",
        type=int,
        default=MAX_TASKS_PER_ASSIGNMENT,
        help="Dynamic LB: maksimum batch per satu assignment (1 = satu task sekali kirim)",
    )
    parser.add_argument("--log-lb", action="store_true", help="Log capacity score setiap assignment task")
    parser.add_argument("--csv", type=str, default=LOG_CSV_PATH, help="Path CSV metrik")
    parser.add_argument("--tag", type=str, default="", help="Label eksperimen (mis. uas-final)")
    parser.add_argument("--scenario", "--scenario-name", dest="scenario", type=str, default=None, help="Nama skenario pada CSV")
    parser.add_argument("--baseline-time", type=float, default=None, help="T1 sequential (detik) untuk hitung Speedup")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    try:
        target_res = parse_resolution(args.target_res)
    except ValueError as exc:
        raise SystemExit(f"[ERROR] {exc}") from exc
    if args.workers < 0:
        raise SystemExit("[ERROR] --workers minimal 0")
    if args.batch_size < 1:
        raise SystemExit("[ERROR] --batch-size minimal 1")
    if args.task_frames is not None and args.task_frames < 1:
        raise SystemExit("[ERROR] --task-frames minimal 1")
    if args.chunks_per_core < 1:
        raise SystemExit("[ERROR] --chunks-per-core minimal 1")

    master = MasterServer(
        video_path=args.video,
        output_path=args.output,
        expected_workers=args.workers,
        lb_mode=args.lb,
        batch_size=args.batch_size,
        target_res=target_res,
        frame_limit=args.frame_limit,
        cache_dir=args.cache_dir,
        rebuild_cache=args.rebuild_cache,
        jpeg_quality=args.jpeg_quality,
        hb_timeout=args.hb_timeout,
        startup_timeout=args.startup_timeout,
        no_worker_timeout=args.no_worker_timeout,
        max_attempts=args.max_attempts,
        max_tasks_per_assignment=args.max_tasks_per_assignment,
        task_frames=args.task_frames,
        chunks_per_core=args.chunks_per_core,
        min_task_frames=args.min_task_frames,
        task_timeout=args.task_timeout,
        local_worker=args.local_worker,
        local_cores=args.local_cores,
        discovery=args.discovery,
        discovery_port=args.discovery_port,
        log_lb=args.log_lb,
        csv_path=args.csv,
        tag=args.tag,
        scenario=args.scenario,
        baseline_time=args.baseline_time,
        bind_host=args.bind,
        port=args.port,
    )
    try:
        master.start()
    except KeyboardInterrupt:
        master.log("\n[MASTER] Dihentikan oleh pengguna.")
        master._shutdown()  # noqa: SLF001 - cleanup terkontrol
    except Exception as exc:  # noqa: BLE001 - tampilkan error ringkas
        master.log(f"[ERROR] Master gagal: {exc}")
        master._shutdown()  # noqa: SLF001
        raise

    if args.csv:
        print_report(args.csv, tag=args.tag or None)


if __name__ == "__main__":
    main()
