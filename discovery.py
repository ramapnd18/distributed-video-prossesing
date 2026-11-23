# discovery.py
"""Auto-discovery Master-Worker berbasis UDP broadcast (+ fallback TCP sweep).

Alur (mengacu pola praktikum):

    Worker  --UDP broadcast DISCOVER_MASTER-->  Master (port 9999)
    Worker  <--JSON {role, host, port}--------  Master
    Worker  --TCP REGISTER------------------->  Master (port 5050)

Master tetap menjadi TCP server dan Worker tetap menjadi TCP client, sehingga
protokol struct + pickle + zlib di `protocol.py` tidak berubah. Discovery hanya
menghilangkan kebutuhan mengetik IP Master secara manual di 9 PC lab.

Jika broadcast diblokir WiFi kampus, `discover_master()` melakukan fallback
TCP sweep pada /24 lokal. Bila keduanya gagal, pengguna tetap dapat memakai
`--master-ip <IP>` seperti sebelumnya.
"""
from __future__ import annotations

import json
import os
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

DISCOVER_MASTER = b"DISCOVER_MASTER"
DISCOVER_WORKERS = b"DISCOVER_WORKERS"


def get_local_ips() -> list[str]:
    """Daftar IPv4 lokal yang bukan loopback/link-local."""
    ips: list[str] = []
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            probe.connect(("8.8.8.8", 80))
            first = probe.getsockname()[0]
            if first and not first.startswith("127."):
                ips.append(first)
    except OSError:
        pass
    try:
        for ip in socket.gethostbyname_ex(socket.gethostname())[2]:
            if ip.startswith(("127.", "169.254.")):
                continue
            if ip not in ips:
                ips.append(ip)
    except OSError:
        pass
    if not ips:
        ips.append("127.0.0.1")
    return ips


def get_broadcast_targets() -> list[str]:
    """Alamat tujuan broadcast: global, loopback, dan broadcast tiap /24 lokal."""
    targets = {"255.255.255.255", "127.0.0.1"}
    if os.name != "nt":
        targets.add("<broadcast>")
    for ip in get_local_ips():
        parts = ip.split(".")
        if len(parts) == 4:
            targets.add(".".join(parts[:3]) + ".255")
    return sorted(targets)


def _make_udp_socket() -> socket.socket | None:
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.settimeout(0.5)
        return sock
    except OSError:
        return None


def start_master_responder(
    discovery_port: int,
    tcp_port: int,
    expected_workers: int,
    stop_event: threading.Event,
    bind_host: str = "",
    hostname: str | None = None,
) -> threading.Thread:
    """Jalankan thread yang menjawab broadcast DISCOVER_MASTER dari Worker."""

    def run() -> None:
        sock = _make_udp_socket()
        if sock is None:
            return
        try:
            sock.bind((bind_host, int(discovery_port)))
        except OSError:
            try:
                sock.close()
            except OSError:
                pass
            return
        node_name = hostname or socket.gethostname()
        while not stop_event.is_set():
            try:
                data, addr = sock.recvfrom(4096)
            except socket.timeout:
                continue
            except OSError:
                break
            if data not in (DISCOVER_MASTER, DISCOVER_WORKERS):
                continue
            reply = {
                "role": "master",
                "host": node_name,
                "port": int(tcp_port),
                "workers": int(expected_workers),
            }
            try:
                sock.sendto(json.dumps(reply).encode("utf-8"), addr)
            except OSError:
                pass
        try:
            sock.close()
        except OSError:
            pass

    thread = threading.Thread(target=run, daemon=True, name="DiscoveryResponder")
    thread.start()
    return thread


def _probe_tcp_node(ip: str, port: int, timeout: float = 0.2) -> str | None:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(timeout)
            if sock.connect_ex((ip, int(port))) == 0:
                return ip
    except OSError:
        pass
    return None


def _tcp_sweep(tcp_port: int, timeout: float) -> tuple[str, int] | None:
    """Fallback: cari host dengan port Master terbuka di /24 lokal."""
    targets: list[str] = []
    for ip in get_local_ips():
        parts = ip.split(".")
        if len(parts) != 4:
            continue
        prefix = ".".join(parts[:3])
        for host in range(1, 255):
            candidate = f"{prefix}.{host}"
            if candidate != ip:
                targets.append(candidate)
    if not targets:
        return None
    deadline = time.time() + max(1.0, timeout)
    with ThreadPoolExecutor(max_workers=80) as executor:
        for ip in executor.map(lambda ip: _probe_tcp_node(ip, tcp_port), targets):
            if ip:
                return ip, int(tcp_port)
            if time.time() >= deadline:
                break
    return None


def discover_master(
    discovery_port: int,
    tcp_port: int,
    timeout: float = 3.0,
    stop_event: threading.Event | None = None,
    label: str = "",
) -> tuple[str, int] | None:
    """Cari Master di LAN. Return (ip, port) atau None.

    Tahap 1: UDP broadcast + tunggu balasan (cepat, ~0.5-1 detik).
    Tahap 2: TCP sweep port Master di /24 lokal (jika broadcast diblokir).
    """
    prefix = f"[{label}] " if label else ""
    sock = _make_udp_socket()
    found: tuple[str, int] | None = None
    if sock is not None:
        deadline = time.time() + max(0.5, float(timeout))
        try:
            while time.time() < deadline and found is None:
                if stop_event is not None and stop_event.is_set():
                    break
                for target in get_broadcast_targets():
                    try:
                        sock.sendto(DISCOVER_MASTER, (target, int(discovery_port)))
                    except OSError:
                        pass
                try:
                    data, addr = sock.recvfrom(4096)
                except socket.timeout:
                    continue
                except OSError:
                    break
                try:
                    info = json.loads(data.decode("utf-8"))
                except (ValueError, UnicodeDecodeError):
                    continue
                if info.get("role") != "master":
                    continue
                found = (addr[0], int(info.get("port", tcp_port)))
                print(
                    f"{prefix}Master ditemukan via UDP: {info.get('host', addr[0])} "
                    f"({addr[0]}:{found[1]}).",
                    flush=True,
                )
        finally:
            try:
                sock.close()
            except OSError:
                pass

    if found is not None:
        return found

    print(f"{prefix}Broadcast tidak terjawab; mencoba TCP sweep port {tcp_port} ...", flush=True)
    swept = _tcp_sweep(int(tcp_port), timeout=timeout)
    if swept is not None:
        print(f"{prefix}Master ditemukan via TCP sweep: {swept[0]}:{swept[1]}.", flush=True)
    return swept
