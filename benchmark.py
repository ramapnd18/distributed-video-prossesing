# benchmark.py
"""Benchmark baseline: Sequential (T1) dan Local Parallel (multiprocessing)."""
from __future__ import annotations

import argparse
import multiprocessing
import os
import socket
import time
from concurrent.futures import ProcessPoolExecutor
from functools import partial

import cv2
import numpy as np

from config import (
    BATCH_SIZE,
    CACHE_DIR,
    DEFAULT_INPUT_VIDEO,
    JPEG_QUALITY,
    LOG_CSV_PATH,
    TARGET_RESOLUTION,
    format_resolution,
    parse_resolution,
)
from processor import process_batch_parallel, process_batch_sequential, process_encoded_frame
from utils.frame_codec import decode_image, encode_jpeg
from utils.metrics_logger import (
    MetricsLogger,
    ResourceSampler,
    get_latest_baseline_t1,
    print_report,
)
from utils.video_utils import (
    build_task_descriptors,
    count_video_frames,
    prepare_frame_cache,
    read_task_frames,
)


def run_benchmark(
    mode: str,
    video_path: str,
    output_path: str | None,
    frame_limit: int | None,
    batch_size: int,
    target_res: tuple[int, int],
    cores: int,
    cache_dir: str,
    rebuild_cache: bool,
    jpeg_quality: int,
    csv_path: str,
    tag: str,
) -> dict:
    meta = prepare_frame_cache(
        video_path=video_path,
        cache_dir=cache_dir,
        frame_limit=frame_limit,
        jpeg_quality=jpeg_quality,
        rebuild=rebuild_cache,
    )
    tasks = build_task_descriptors(meta, batch_size)
    total_frames = meta.total_frames

    writer = None
    if output_path:
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        writer = cv2.VideoWriter(
            output_path,
            cv2.VideoWriter_fourcc(*"mp4v"),
            float(meta.fps),
            target_res,
        )
        if not writer.isOpened():
            raise RuntimeError(f"Gagal membuka VideoWriter: {output_path}")

    pool: ProcessPoolExecutor | None = None
    if mode == "local-parallel" and cores > 1:
        pool = ProcessPoolExecutor(
            max_workers=cores,
            mp_context=multiprocessing.get_context("spawn"),
        )

    worker_fn = partial(process_encoded_frame, target_res=target_res)
    # Warmup di luar timer: load OpenCV + spawn process pool.
    dummy_blob = encode_jpeg(np.zeros((64, 64, 3), dtype=np.uint8))
    process_encoded_frame(dummy_blob, target_res)
    if pool is not None:
        list(pool.map(worker_fn, [dummy_blob], chunksize=1))

    sampler = ResourceSampler(interval=1.0)
    sampler.start()
    processed_count = 0
    processing_seconds = 0.0
    try:
        for task in tasks:
            blobs = read_task_frames(meta, task)
            if pool is not None:
                started = time.perf_counter()
                processed = process_batch_parallel(
                    blobs,
                    num_workers=cores,
                    target_res=target_res,
                    executor=pool,
                    encoded=True,
                )
                processing_seconds += time.perf_counter() - started
            else:
                started = time.perf_counter()
                processed = process_batch_sequential(
                    blobs,
                    target_res=target_res,
                    encoded=True,
                )
                processing_seconds += time.perf_counter() - started
            processed_count += len(processed)
            if writer is not None:
                for blob in processed:
                    writer.write(decode_image(blob))
    finally:
        elapsed = processing_seconds
        sampler.stop()
        if pool is not None:
            pool.shutdown(wait=True)
        if writer is not None:
            writer.release()

    written = count_video_frames(output_path) if output_path else 0
    lost = max(0, total_frames - written) if output_path else 0
    throughput = processed_count / elapsed if elapsed > 0 else 0.0
    resources = sampler.summary()

    scenario = "sequential" if mode == "sequential" else "local_parallel"
    workers = 1 if mode == "sequential" else max(1, cores)
    baseline = elapsed if mode == "sequential" else get_latest_baseline_t1(
        csv_path,
        total_frames=total_frames,
        target_res=format_resolution(target_res),
        tag=tag,
    )
    speedup = ""
    efficiency = ""
    if baseline and baseline > 0 and elapsed > 0:
        speedup_value = baseline / elapsed
        speedup = round(speedup_value, 4)
        efficiency = round((speedup_value / workers) * 100.0, 2)

    record = {
        "tag": tag,
        "scenario": scenario,
        "scenario_name": scenario,
        "lb_mode": "none" if mode == "sequential" else "local",
        "workers": workers,
        "num_workers": workers,
        "workers_seen": workers,
        "cores_per_worker": str(workers),
        "total_cores": workers,
        "batch_size": batch_size,
        "target_res": format_resolution(target_res),
        "total_frames": total_frames,
        "exec_time_sec": round(elapsed, 4),
        "baseline_t1_sec": round(baseline, 4) if baseline else "",
        "throughput_fps": round(throughput, 4),
        "speedup": speedup,
        "efficiency_pct": efficiency,
        "node_efficiency_pct": efficiency,
        "avg_cpu_pct": resources["avg_cpu_pct"],
        "max_cpu_pct": resources["max_cpu_pct"],
        "avg_ram_pct": resources["avg_ram_pct"],
        "max_ram_pct": resources["max_ram_pct"],
        "recovery_time_sec": 0,
        "requeued_tasks": 0,
        "failed_tasks": 0,
        "lost_frames": lost,
        "worker_task_distribution": f"single:{total_frames}" if mode == "sequential" else f"local:{total_frames}",
        "hostname": socket.gethostname(),
        "cpu_cores": os.cpu_count() or 1,
        "status": "completed",
        "notes": "timer = decode JPEG + pipeline OpenCV + encode PNG; I/O cache & tulis video di luar timer",
    }
    MetricsLogger(csv_path).log(record)

    print("\n" + "=" * 64)
    print(f"BENCHMARK {mode.upper()}")
    print("=" * 64)
    print(f"Frame diproses     : {processed_count}/{total_frames}")
    print(f"Execution Time     : {elapsed:.2f} detik")
    print(f"Throughput         : {throughput:.2f} FPS")
    print(f"CPU / RAM rata-rata: {resources['avg_cpu_pct']:.1f}% / {resources['avg_ram_pct']:.1f}%")
    if output_path:
        print(f"Output video       : {output_path} ({written} frame, hilang {lost})")
    print(f"Metrik CSV         : {os.path.abspath(csv_path)}")
    print("=" * 64)
    return record


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Benchmark Sequential & Local Parallel")
    parser.add_argument("--mode", choices=["sequential", "local-parallel"], help="Mode benchmark")
    parser.add_argument("--report", action="store_true", help="Tampilkan tabel ringkasan dari CSV")
    parser.add_argument("--video", type=str, default=DEFAULT_INPUT_VIDEO, help="Path video input")
    parser.add_argument("--output", type=str, default=None, help="Simpan video output (opsional)")
    parser.add_argument("--frame-limit", type=int, default=None, help="Batasi jumlah frame")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="Jumlah frame per batch")
    parser.add_argument("--target-res", type=str, default=format_resolution(TARGET_RESOLUTION), help="Resolusi target WIDTHxHEIGHT")
    parser.add_argument("--cores", type=int, default=os.cpu_count() or 1, help="Jumlah core untuk local-parallel")
    parser.add_argument("--cache-dir", type=str, default=CACHE_DIR, help="Direktori cache frame")
    parser.add_argument("--rebuild-cache", action="store_true", help="Paksa ekstrak ulang cache frame")
    parser.add_argument("--jpeg-quality", type=int, default=JPEG_QUALITY, help="Kualitas JPEG cache frame")
    parser.add_argument("--csv", type=str, default=LOG_CSV_PATH, help="Path CSV metrik")
    parser.add_argument("--tag", type=str, default="", help="Label eksperimen")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.report:
        print_report(args.csv, tag=args.tag or None)
        return 0
    if not args.mode:
        raise SystemExit("[ERROR] Pilih --mode sequential|local-parallel atau --report")
    try:
        target_res = parse_resolution(args.target_res)
    except ValueError as exc:
        raise SystemExit(f"[ERROR] {exc}") from exc
    if args.batch_size < 1:
        raise SystemExit("[ERROR] --batch-size minimal 1")
    if args.cores < 1:
        raise SystemExit("[ERROR] --cores minimal 1")

    run_benchmark(
        mode=args.mode,
        video_path=args.video,
        output_path=args.output,
        frame_limit=args.frame_limit,
        batch_size=args.batch_size,
        target_res=target_res,
        cores=args.cores,
        cache_dir=args.cache_dir,
        rebuild_cache=args.rebuild_cache,
        jpeg_quality=args.jpeg_quality,
        csv_path=args.csv,
        tag=args.tag,
    )
    print_report(args.csv, tag=args.tag or None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
