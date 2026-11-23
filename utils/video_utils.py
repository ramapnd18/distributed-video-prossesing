# utils/video_utils.py
"""Utilitas video: cache frame di disk, pembacaan task, dan rekonstruksi output.

Cache frame mencegah Master memuat seluruh video ke RAM. Video 1080p 2 menit
(7200 frame) membutuhkan ~40 GB jika disimpan sebagai array mentah; dengan
cache JPEG di disk kebutuhan RAM Master tetap kecil.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import asdict, dataclass

import cv2

from config import CACHE_DIR, JPEG_QUALITY
from utils.frame_codec import decode_image, encode_jpeg


@dataclass
class VideoMeta:
    """Metadata video + lokasi cache frame."""

    video_path: str
    cache_dir: str
    fps: float
    width: int
    height: int
    total_frames: int
    source_frame_count: int
    frame_limit: int | None
    jpeg_quality: int
    source_size: int
    source_mtime_ns: int

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "VideoMeta":
        return cls(**data)


def _cache_key(video_path: str, stat: os.stat_result, frame_limit: int | None, jpeg_quality: int) -> str:
    raw = (
        f"{os.path.abspath(video_path)}|{stat.st_size}|{stat.st_mtime_ns}|"
        f"{frame_limit if frame_limit is not None else 'all'}|{jpeg_quality}"
    )
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10]
    stem = os.path.splitext(os.path.basename(video_path))[0]
    return f"{stem}_{digest}"


def frame_path(cache_dir: str, index: int) -> str:
    return os.path.join(cache_dir, f"frame_{index:06d}.jpg")


def _cache_is_valid(meta: VideoMeta, meta_path: str) -> bool:
    if not os.path.exists(meta_path):
        return False
    if meta.total_frames <= 0:
        return False
    if not os.path.exists(frame_path(meta.cache_dir, 0)):
        return False
    if not os.path.exists(frame_path(meta.cache_dir, meta.total_frames - 1)):
        return False
    try:
        frame_files = sum(
            1 for name in os.listdir(meta.cache_dir) if name.startswith("frame_") and name.endswith(".jpg")
        )
    except OSError:
        return False
    return frame_files == meta.total_frames


def prepare_frame_cache(
    video_path: str,
    cache_dir: str = CACHE_DIR,
    frame_limit: int | None = None,
    jpeg_quality: int = JPEG_QUALITY,
    rebuild: bool = False,
    verbose: bool = True,
) -> VideoMeta:
    """Ekstrak frame video menjadi JPEG di disk (sekali saja) dan kembalikan metadata."""
    video_path = os.path.abspath(video_path)
    if not os.path.exists(video_path):
        raise FileNotFoundError(
            f"Video input tidak ditemukan: {video_path}\n"
            "Buat video contoh dengan: python scripts/make_sample_video.py"
        )
    if frame_limit is not None and frame_limit <= 0:
        frame_limit = None

    stat = os.stat(video_path)
    key = _cache_key(video_path, stat, frame_limit, jpeg_quality)
    target_dir = os.path.join(cache_dir, key)
    meta_path = os.path.join(target_dir, "meta.json")

    if not rebuild and os.path.exists(meta_path):
        try:
            with open(meta_path, "r", encoding="utf-8") as handle:
                meta = VideoMeta.from_dict(json.load(handle))
            if _cache_is_valid(meta, meta_path):
                if verbose:
                    print(
                        f"[CACHE] Memakai cache frame: {target_dir} "
                        f"({meta.total_frames} frame).",
                        flush=True,
                    )
                return meta
        except (OSError, ValueError, TypeError, KeyError):
            pass

    os.makedirs(target_dir, exist_ok=True)
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise RuntimeError(f"Gagal membuka video: {video_path}")

    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    source_frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)

    if verbose:
        print(
            f"[CACHE] Mengekstrak frame dari {os.path.basename(video_path)} "
            f"({width}x{height}, {fps:.2f} FPS, ~{source_frame_count} frame) ...",
            flush=True,
        )

    count = 0
    started = time.time()
    try:
        while True:
            ret, frame = cap.read()
            if not ret:
                break
            with open(frame_path(target_dir, count), "wb") as handle:
                handle.write(encode_jpeg(frame, jpeg_quality))
            count += 1
            if frame_limit is not None and count >= frame_limit:
                break
            if verbose and count % 200 == 0:
                print(f"[CACHE]   {count} frame ...", flush=True)
    finally:
        cap.release()

    if count == 0:
        raise RuntimeError(f"Tidak ada frame yang terbaca dari video: {video_path}")

    meta = VideoMeta(
        video_path=video_path,
        cache_dir=target_dir,
        fps=float(fps),
        width=width,
        height=height,
        total_frames=count,
        source_frame_count=source_frame_count or count,
        frame_limit=frame_limit,
        jpeg_quality=jpeg_quality,
        source_size=stat.st_size,
        source_mtime_ns=stat.st_mtime_ns,
    )
    with open(meta_path, "w", encoding="utf-8") as handle:
        json.dump(meta.to_dict(), handle, indent=2)

    if verbose:
        elapsed = time.time() - started
        print(
            f"[CACHE] Selesai: {count} frame dalam {elapsed:.1f}s -> {target_dir}",
            flush=True,
        )
    return meta


def build_task_descriptors(meta: VideoMeta, batch_size: int) -> list[dict]:
    """Pecah metadata frame menjadi task independen (hanya metadata, tanpa frame)."""
    if batch_size <= 0:
        raise ValueError("batch_size harus > 0")
    tasks = []
    task_id = 0
    start = 0
    while start < meta.total_frames:
        end = min(start + batch_size, meta.total_frames)
        tasks.append(
            {
                "task_id": task_id,
                "start_frame": start,
                "end_frame": end,
                "expected_frames": end - start,
                "attempts": 0,
            }
        )
        task_id += 1
        start = end
    return tasks


def read_task_frames(meta: VideoMeta, task: dict) -> list[bytes]:
    """Baca byte JPEG untuk satu task dari cache disk (aman untuk requeue)."""
    frames = []
    for index in range(int(task["start_frame"]), int(task["end_frame"])):
        path = frame_path(meta.cache_dir, index)
        with open(path, "rb") as handle:
            frames.append(handle.read())
    if len(frames) != int(task["expected_frames"]):
        raise RuntimeError(
            f"Cache frame tidak lengkap untuk task #{task['task_id']}: "
            f"{len(frames)}/{task['expected_frames']} frame."
        )
    return frames


def reconstruct_video(
    completed_results: dict[int, list[bytes]],
    output_path: str,
    fps: float,
    target_size: tuple[int, int],
    total_tasks: int,
    expected_frames: int,
    verbose: bool = True,
) -> tuple[int, int]:
    """Susun hasil task (byte PNG) menjadi video output. Return (frame_ditulis, frame_hilang)."""
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    writer = cv2.VideoWriter(output_path, fourcc, float(fps), target_size)
    if not writer.isOpened():
        raise RuntimeError(
            f"Gagal membuka VideoWriter untuk {output_path}. "
            "Pastikan codec mp4v tersedia."
        )

    written = 0
    missing_tasks = 0
    try:
        for task_id in range(total_tasks):
            frames = completed_results.get(task_id)
            if not frames:
                missing_tasks += 1
                continue
            for blob in frames:
                frame = decode_image(blob)
                if frame.shape[1] != target_size[0] or frame.shape[0] != target_size[1]:
                    frame = cv2.resize(frame, target_size, interpolation=cv2.INTER_LINEAR)
                writer.write(frame)
                written += 1
    finally:
        writer.release()

    lost = max(0, expected_frames - written)
    if verbose:
        print(
            f"[RECONSTRUCT] {written}/{expected_frames} frame ditulis ke {output_path} "
            f"(hilang: {lost}, task kosong: {missing_tasks}).",
            flush=True,
        )
    return written, lost


def count_video_frames(video_path: str) -> int:
    """Hitung jumlah frame aktual pada file video."""
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        return 0
    count = 0
    try:
        while True:
            ret, _ = cap.read()
            if not ret:
                break
            count += 1
    finally:
        cap.release()
    return count
