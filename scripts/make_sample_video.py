# scripts/make_sample_video.py
"""Buat video sampel sintetis untuk pengembangan & smoke test.

Video asli berlisensi terbuka (Big Buck Bunny / Pexels / Pixabay) tetap
disarankan untuk laporan UAS. Lihat data/SOURCES.txt.
"""
from __future__ import annotations

import argparse
import math
import os
import sys
from pathlib import Path

import cv2
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import DEFAULT_INPUT_VIDEO  # noqa: E402


def build_background(width: int, height: int) -> np.ndarray:
    y = np.linspace(0, 255, height, dtype=np.float32).reshape(height, 1)
    x = np.linspace(0, 255, width, dtype=np.float32).reshape(1, width)
    blue = np.clip(40 + 0.25 * x + 0.10 * y, 0, 255)
    green = np.clip(30 + 0.18 * x + 0.22 * y, 0, 255)
    red = np.clip(90 + 0.05 * x + 0.05 * y, 0, 255)
    background = np.dstack([blue, green, red]).astype(np.uint8)
    return background


def make_video(
    output_path: str,
    width: int,
    height: int,
    seconds: float,
    fps: int,
    force: bool,
) -> None:
    if os.path.exists(output_path) and not force:
        print(f"[SAMPLE] Sudah ada: {output_path} (pakai --force untuk menimpa).", flush=True)
        return
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    total_frames = max(1, int(round(seconds * fps)))
    writer = cv2.VideoWriter(
        output_path,
        cv2.VideoWriter_fourcc(*"mp4v"),
        float(fps),
        (width, height),
    )
    if not writer.isOpened():
        raise RuntimeError(f"Gagal membuat VideoWriter: {output_path}")

    background = build_background(width, height)
    print(
        f"[SAMPLE] Membuat {total_frames} frame {width}x{height} @ {fps} FPS -> {output_path}",
        flush=True,
    )
    try:
        for index in range(total_frames):
            frame = background.copy()
            t = index / max(1, total_frames - 1)

            # Objek bergerak: lingkaran, persegi, dan garis diagonal.
            cx = int(width * (0.12 + 0.76 * t))
            cy = int(height * (0.5 + 0.28 * math.sin(t * math.pi * 4)))
            cv2.circle(frame, (cx, cy), max(18, width // 18), (0, 220, 255), -1)
            cv2.circle(frame, (cx, cy), max(18, width // 18), (0, 0, 0), 3)

            rx = int(width * (0.82 - 0.68 * t))
            ry = int(height * (0.24 + 0.18 * math.cos(t * math.pi * 6)))
            size = max(24, width // 14)
            cv2.rectangle(frame, (rx, ry), (rx + size, ry + size), (255, 80, 40), -1)

            x1 = int(width * (0.05 + 0.9 * t))
            cv2.line(frame, (0, height - 1), (x1, 0), (40, 255, 80), 3)

            for grid_x in range(0, width, max(80, width // 8)):
                cv2.line(frame, (grid_x, 0), (grid_x, height), (255, 255, 255), 1)
            for grid_y in range(0, height, max(60, height // 8)):
                cv2.line(frame, (0, grid_y), (width, grid_y), (255, 255, 255), 1)

            cv2.putText(
                frame,
                f"SAMPLE FRAME {index:05d}",
                (max(12, width // 40), max(40, height // 10)),
                cv2.FONT_HERSHEY_SIMPLEX,
                max(0.6, width / 1400),
                (255, 255, 255),
                2,
                cv2.LINE_AA,
            )
            cv2.putText(
                frame,
                f"t={index / fps:05.2f}s",
                (max(12, width // 40), height - max(20, height // 16)),
                cv2.FONT_HERSHEY_SIMPLEX,
                max(0.5, width / 1800),
                (255, 255, 255),
                2,
                cv2.LINE_AA,
            )
            writer.write(frame)
            if index and index % 200 == 0:
                print(f"[SAMPLE]   {index}/{total_frames} frame ...", flush=True)
    finally:
        writer.release()
    print(f"[SAMPLE] Selesai: {output_path}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Buat video sampel sintetis")
    parser.add_argument("--output", type=str, default=DEFAULT_INPUT_VIDEO)
    parser.add_argument("--width", type=int, default=1280)
    parser.add_argument("--height", type=int, default=720)
    parser.add_argument("--seconds", type=float, default=20.0)
    parser.add_argument("--fps", type=int, default=30)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    if args.width < 16 or args.height < 16:
        raise SystemExit("[ERROR] width/height minimal 16")
    if args.seconds <= 0 or args.fps <= 0:
        raise SystemExit("[ERROR] seconds/fps harus > 0")
    make_video(
        output_path=args.output,
        width=args.width,
        height=args.height,
        seconds=args.seconds,
        fps=args.fps,
        force=args.force,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
