# protocol.py
"""Protokol TCP kustom: header 4 byte + payload pickle terkompresi zlib.

CATATAN KEAMANAN:
    `pickle` hanya boleh dipakai di jaringan lokal yang dipercaya (sesuai
    kebutuhan UAS). Jangan mengekspos port Master ke internet publik.
"""
import pickle
import socket
import struct
import time
import zlib

from config import MAX_PACKET_BYTES, SOCKET_TIMEOUT, ZLIB_COMPRESSION_LEVEL

# Konstanta Tipe Pesan Protokol
MSG_PING = "PING"                    # Validasi kandidat hasil TCP sweep
MSG_PONG = "PONG"
MSG_REGISTER = "REGISTER"
MSG_REQUEST_TASK = "REQUEST_TASK"
MSG_ASSIGN_TASK = "ASSIGN_TASK"
MSG_WAIT = "WAIT"                  # Antrean kosong sementara, tunggu jika ada requeue
MSG_SUBMIT_RESULT = "SUBMIT_RESULT"
MSG_HEARTBEAT = "HEARTBEAT"
MSG_ERROR = "ERROR"                # Worker gagal memproses task -> Master requeue
MSG_SHUTDOWN = "SHUTDOWN"

HEADER_FORMAT = "!I"               # 4-byte unsigned int (big-endian) untuk panjang payload
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


class ProtocolError(Exception):
    """Paket rusak / tidak sesuai protokol."""


def configure_socket(sock: socket.socket) -> None:
    """Set opsi socket untuk latensi rendah dan deteksi koneksi mati."""
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.settimeout(SOCKET_TIMEOUT)


def send_packet(sock: socket.socket, msg_type: str, payload=None, compress: bool = True) -> None:
    """Serialisasi, kompresi zlib, dan kirim paket lengkap melalui TCP socket."""
    message = {"type": msg_type, "payload": payload}
    raw_bytes = pickle.dumps(message, protocol=pickle.HIGHEST_PROTOCOL)
    if compress:
        body = zlib.compress(raw_bytes, level=ZLIB_COMPRESSION_LEVEL)
    else:
        body = raw_bytes
    if len(body) > MAX_PACKET_BYTES:
        raise ProtocolError(
            f"Paket terlalu besar: {len(body)} byte > batas {MAX_PACKET_BYTES} byte. "
            "Kurangi batch size."
        )
    header = struct.pack(HEADER_FORMAT, len(body))
    sock.sendall(header + body)


def recv_exact(sock: socket.socket, num_bytes: int) -> bytes | None:
    """Membaca tepat num_bytes dari stream TCP hingga lengkap atau koneksi terputus."""
    chunks = []
    bytes_received = 0
    while bytes_received < num_bytes:
        chunk = sock.recv(min(num_bytes - bytes_received, 65536))
        if not chunk:
            return None
        chunks.append(chunk)
        bytes_received += len(chunk)
    return b"".join(chunks)


def recv_packet(sock: socket.socket) -> dict | None:
    """Menerima satu paket utuh, dekompresi, dan kembalikan dictionary pesan."""
    header_data = recv_exact(sock, HEADER_SIZE)
    if not header_data:
        return None
    (payload_length,) = struct.unpack(HEADER_FORMAT, header_data)
    if payload_length <= 0 or payload_length > MAX_PACKET_BYTES:
        raise ProtocolError(
            f"Panjang payload tidak valid: {payload_length} byte "
            f"(batas 1..{MAX_PACKET_BYTES})."
        )
    body = recv_exact(sock, payload_length)
    if not body:
        return None
    try:
        raw_bytes = zlib.decompress(body)
    except zlib.error as exc:
        raise ProtocolError(f"Gagal dekompresi zlib: {exc}") from exc
    try:
        message = pickle.loads(raw_bytes)
    except Exception as exc:  # noqa: BLE001 - pesan dari jaringan harus divalidasi
        raise ProtocolError(f"Gagal unpickle pesan: {exc}") from exc
    if not isinstance(message, dict) or "type" not in message:
        raise ProtocolError("Format pesan tidak valid (harus dict dengan key 'type').")
    return message


def connect_with_retry(
    host: str,
    port: int,
    timeout: float = 60.0,
    stop_event=None,
    label: str = "",
) -> socket.socket | None:
    """Menyambung ke host:port dengan retry sampai timeout. Return None jika menyerah."""
    deadline = time.time() + timeout
    prefix = f"[{label}] " if label else ""
    last_error = None
    while True:
        if stop_event is not None and stop_event.is_set():
            return None
        try:
            sock = socket.create_connection((host, port), timeout=5.0)
            configure_socket(sock)
            return sock
        except OSError as exc:
            last_error = exc
            if time.time() >= deadline:
                break
            print(f"{prefix}Menunggu Master {host}:{port} ... ({exc})", flush=True)
            time.sleep(1.0)
    print(f"{prefix}Gagal terhubung ke {host}:{port}: {last_error}", flush=True)
    return None
