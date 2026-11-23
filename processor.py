# processor.py
"""Pipeline pemrosesan frame OpenCV (CPU-bound) + helper multiprocessing lokal."""
from __future__ import annotations

import os
from concurrent.futures import ProcessPoolExecutor
from functools import partial

import cv2
import numpy as np

from config import TARGET_RESOLUTION
from utils.frame_codec import decode_image, encode_png


def process_single_frame(
    frame: np.ndarray,
    target_res: tuple[int, int] = TARGET_RESOLUTION,
) -> np.ndarray:
    """
    Pipeline pemrosesan 1 frame video (CPU-bound):
    1. Resize ke resolusi target
    2. Konversi ke Grayscale
    3. Reduksi noise dengan Gaussian Blur 7x7
    4. Deteksi tepi Canny & ekstraksi kontur objek
    """
    resized = cv2.resize(frame, target_res, interpolation=cv2.INTER_CUBIC)
    gray = cv2.cvtColor(resized, cv2.COLOR_BGR2GRAY)
    blurred = cv2.GaussianBlur(gray, (7, 7), 0)
    edges = cv2.Canny(blurred, threshold1=50, threshold2=150)

    # Cari kontur dan gambar bounding box di atas frame hasil edge
    contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    output_frame = cv2.cvtColor(edges, cv2.COLOR_GRAY2BGR)

    for contour in contours:
        if cv2.contourArea(contour) > 200:
            x, y, w, h = cv2.boundingRect(contour)
            cv2.rectangle(output_frame, (x, y), (x + w, y + h), (0, 255, 0), 2)

    return output_frame


def process_batch_sequential(
    frames: list[np.ndarray] | list[bytes],
    target_res: tuple[int, int] = TARGET_RESOLUTION,
    encoded: bool = False,
) -> list[np.ndarray] | list[bytes]:
    """Memproses satu batch frame secara sekuensial (1 core) untuk baseline T1.

    `encoded=True` menerima byte JPEG dan mengembalikan byte PNG. Mode ini
    menyamakan operasi dengan Worker (decode + pipeline + encode) dan dipakai
    benchmark agar perbandingan adil.
    """
    if encoded:
        return [process_encoded_frame(blob, target_res) for blob in frames]  # type: ignore[arg-type]
    return [process_single_frame(frame, target_res) for frame in frames]  # type: ignore[arg-type]


def process_encoded_frame(
    blob: bytes,
    target_res: tuple[int, int] = TARGET_RESOLUTION,
) -> bytes:
    """Decode JPEG -> pipeline OpenCV -> encode PNG.

    Dipakai Worker agar transfer antar-process (pickle IPC) hanya membawa byte
    terkompresi, bukan array 1080p mentah (~6 MB per frame).
    """
    frame = decode_image(blob)
    processed = process_single_frame(frame, target_res)
    return encode_png(processed)


def process_batch_parallel(
    frames: list[np.ndarray] | list[bytes],
    num_workers: int | None = None,
    target_res: tuple[int, int] = TARGET_RESOLUTION,
    executor: ProcessPoolExecutor | None = None,
    encoded: bool = False,
) -> list[np.ndarray] | list[bytes]:
    """Memproses satu batch frame secara paralel menggunakan seluruh core CPU Worker.

    `executor` boleh dipakai ulang (persistent pool) agar Worker tidak membuat
    process pool baru untuk setiap task. `encoded=True` memakai byte JPEG/PNG
    agar IPC hanya membawa data terkompresi.
    """
    workers = num_workers or os.cpu_count() or 1
    worker_fn = partial(
        process_encoded_frame if encoded else process_single_frame,
        target_res=target_res,
    )
    if executor is not None:
        return list(executor.map(worker_fn, frames, chunksize=1))
    with ProcessPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(worker_fn, frames, chunksize=1))


def warmup(target_res: tuple[int, int] = TARGET_RESOLUTION) -> None:
    """Panaskan OpenCV sebelum pengukuran waktu agar import/lazy-init tidak masuk metrik."""
    dummy = np.zeros((64, 64, 3), dtype=np.uint8)
    process_single_frame(dummy, target_res)
