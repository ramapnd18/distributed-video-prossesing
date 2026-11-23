# utils/frame_codec.py
"""Encode/decode frame untuk transfer jaringan.

- Input frame  : JPEG (lossy, ukuran kecil) -> dikirim Master ke Worker.
- Output frame : PNG (lossless) -> dikirim Worker ke Master agar video akhir utuh.
"""
import cv2
import numpy as np

from config import JPEG_QUALITY, PNG_COMPRESSION_LEVEL


def encode_jpeg(frame: np.ndarray, quality: int = JPEG_QUALITY) -> bytes:
    """Encode frame BGR menjadi byte JPEG."""
    ok, buffer = cv2.imencode(
        ".jpg",
        frame,
        [int(cv2.IMWRITE_JPEG_QUALITY), int(quality)],
    )
    if not ok:
        raise ValueError("Gagal encode frame ke JPEG.")
    return buffer.tobytes()


def encode_png(frame: np.ndarray, level: int = PNG_COMPRESSION_LEVEL) -> bytes:
    """Encode frame BGR menjadi byte PNG (lossless)."""
    ok, buffer = cv2.imencode(
        ".png",
        frame,
        [int(cv2.IMWRITE_PNG_COMPRESSION), int(level)],
    )
    if not ok:
        raise ValueError("Gagal encode frame ke PNG.")
    return buffer.tobytes()


def decode_image(blob: bytes, flags: int = cv2.IMREAD_COLOR) -> np.ndarray:
    """Decode byte JPEG/PNG menjadi array frame BGR."""
    if not blob:
        raise ValueError("Byte frame kosong.")
    array = np.frombuffer(blob, dtype=np.uint8)
    frame = cv2.imdecode(array, flags)
    if frame is None:
        raise ValueError("Gagal decode frame (byte bukan JPEG/PNG valid).")
    return frame
