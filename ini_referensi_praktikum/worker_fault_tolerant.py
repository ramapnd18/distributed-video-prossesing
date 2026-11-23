# worker.py
import socket
import json
import threading
import os
import time
import math
from concurrent.futures import ProcessPoolExecutor

DISCOVERY_PORT = 9999
TCP_PORT = 9998


def count_primes_chunk(args):
    """Menghitung bilangan prima dalam interval [start, end)."""
    start, end = args
    count = 0

    for n in range(max(2, start), end):
        is_p = True
        limit = int(math.isqrt(n)) + 1

        for i in range(2, limit):
            if n % i == 0:
                is_p = False
                break

        if is_p:
            count += 1

    return count


def compute_task(start_num, end_num):
    """
    Setiap request menggunakan seluruh core worker.
    Master mengatur load balancing antar-worker melalui chunk dinamis.
    """
    cores = os.cpu_count() or 2
    total_items = end_num - start_num

    step = max(1, total_items // cores)
    ranges = []

    for i in range(cores):
        s = start_num + i * step
        e = start_num + (i + 1) * step if i < cores - 1 else end_num

        if s < e:
            ranges.append((s, e))

    t0 = time.time()

    with ProcessPoolExecutor(max_workers=cores) as executor:
        results = list(executor.map(count_primes_chunk, ranges))

    duration = time.time() - t0

    return {
        "host": socket.gethostname(),
        "cores_used": cores,
        "primes_found": sum(results),
        "duration": duration,
    }


def udp_discovery_responder():
    """Menjawab discovery dari Master."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("", DISCOVERY_PORT))

        while True:
            try:
                data, addr = s.recvfrom(4096)

                if data == b"DISCOVER_WORKERS":
                    info = {
                        "host": socket.gethostname(),
                        "cores": os.cpu_count() or 2,
                        "port": TCP_PORT,
                    }
                    s.sendto(
                        json.dumps(info).encode("utf-8"),
                        addr,
                    )

            except Exception:
                pass


def handle_task(conn, addr):
    """
    Menangani satu task dari Master.
    action=info digunakan saat discovery TCP.
    action=compute digunakan untuk komputasi.
    """
    with conn:
        try:
            raw_data = conn.recv(65536).decode("utf-8")

            if not raw_data:
                return

            req = json.loads(raw_data)
            action = req.get("action", "compute")

            if action == "info":
                response = {
                    "host": socket.gethostname(),
                    "cores": os.cpu_count() or 2,
                    "port": TCP_PORT,
                    "status": "ready",
                }
                conn.sendall(json.dumps(response).encode("utf-8"))
                return

            task_id = req.get("task_id", "unknown")
            start_num = int(req.get("start", 2))
            end_num = int(req.get("end", 1000))

            if end_num <= start_num:
                raise ValueError("Rentang task tidak valid.")

            print("\n" + "=" * 60)
            print(f"[+] TASK {task_id} DITERIMA DARI MASTER {addr[0]}")
            print("=" * 60)
            print(
                f"[*] Rentang : {start_num:,} s.d. {end_num - 1:,} "
                f"({end_num - start_num:,} angka)"
            )
            print(
                f"[*] CPU     : menggunakan {os.cpu_count() or 2} core"
            )
            print("[*] Status  : COMPUTING...")

            result = compute_task(start_num, end_num)
            result["task_id"] = task_id

            print(
                f"[✓] TASK {task_id} SELESAI | "
                f"prima={result['primes_found']:,} | "
                f"{result['duration']:.2f}s"
            )

            conn.sendall(json.dumps(result).encode("utf-8"))

        except Exception as e:
            error_response = {
                "task_id": req.get("task_id", "unknown") if "req" in locals() else "unknown",
                "error": str(e),
            }

            try:
                conn.sendall(json.dumps(error_response).encode("utf-8"))
            except Exception:
                pass

            print(f"[-] Error proses task dari {addr[0]}: {e}")


def main():
    threading.Thread(
        target=udp_discovery_responder,
        daemon=True,
    ).start()

    hostname = socket.gethostname()
    cores = os.cpu_count() or 2

    print("=" * 60)
    print("      HEAVY PARALLEL WORKER NODE")
    print("      Dynamic Load Balancing + Fault Tolerance")
    print("=" * 60)
    print(f"[*] Hostname : {hostname}")
    print(f"[*] Kapasitas: {cores} Core CPU")
    print(f"[*] Status   : Menunggu Master di port {TCP_PORT}...")
    print("=" * 60)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp_srv:
        tcp_srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_srv.bind(("", TCP_PORT))
        tcp_srv.listen(10)

        while True:
            conn, addr = tcp_srv.accept()
            threading.Thread(
                target=handle_task,
                args=(conn, addr),
                daemon=True,
            ).start()


if __name__ == "__main__":
    main()
