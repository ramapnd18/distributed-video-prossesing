# master.py
import socket
import json
import time
import os
import math
import uuid
from concurrent.futures import ThreadPoolExecutor

DISCOVERY_PORT = 9999
TCP_PORT = 9998

# Parameter fault tolerance / scheduling
TASK_TIMEOUT = 45.0          # ftimeout komunikasi + komputasi per chunk
MAX_RETRIES = 2               # maksimal pengiriman ulang chunk
MIN_CHUNK_SIZE = 100_000
CHUNKS_PER_CORE = 4           # semakin besar -> load balancing semakin halus


def count_primes_chunk_local(args):
    """Komputasi lokal pada Master: interval [start, end)."""
    start, end = args
    count = 0
    samples_first = []
    samples_last = []

    for n in range(max(2, start), end):
        is_p = True
        limit = int(math.isqrt(n)) + 1
        for i in range(2, limit):
            if n % i == 0:
                is_p = False
                break
        if is_p:
            count += 1
            if len(samples_first) < 5:
                samples_first.append(n)
            else:
                if len(samples_last) >= 5:
                    samples_last.pop(0)
                samples_last.append(n)

    return count, samples_first, samples_last


def run_local_task(start_num, end_num):
    """Menjalankan satu chunk pada Master menggunakan seluruh core lokal."""
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
    from concurrent.futures import ProcessPoolExecutor
    with ProcessPoolExecutor(max_workers=cores) as executor:
        results = list(executor.map(count_primes_chunk_local, ranges))
    duration = time.time() - t0

    total_primes = sum(r[0] for r in results)

    first = []
    for r in results:
        first.extend(r[1])
        if len(first) >= 5:
            break

    last = []
    for r in reversed(results):
        last = r[2] + last
        if len(last) >= 5:
            break

    return {
        "host": socket.gethostname() + " (PC Master)",
        "cores_used": cores,
        "primes_found": total_primes,
        "sample_first": first[:5],
        "sample_last": last[-5:] if last else first[:5],
        "duration": duration,
    }


def check_tcp_node(ip):
    """Mengecek apakah worker aktif melalui TCP port 9998."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.5)
            if s.connect_ex((ip, TCP_PORT)) == 0:
                try:
                    s.sendall(json.dumps({"action": "info"}).encode("utf-8"))
                    s.settimeout(1.0)
                    raw = s.recv(4096).decode("utf-8")
                    info = json.loads(raw)
                    info["ip"] = ip
                    return info
                except Exception:
                    try:
                        name = socket.gethostbyaddr(ip)[0]
                    except Exception:
                        name = "Worker-Node"
                    return {
                        "host": name,
                        "cores": 4,
                        "port": TCP_PORT,
                        "ip": ip,
                    }
    except Exception:
        pass
    return None


def get_local_subnets():
    """Mendeteksi subnet IPv4 lokal."""
    subnets = []
    ips = []

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            ips.append(s.getsockname()[0])
    except Exception:
        pass

    try:
        host = socket.gethostname()
        for ip in socket.gethostbyname_ex(host)[2]:
            if not ip.startswith("127.") and not ip.startswith("169.254."):
                if ip not in ips:
                    ips.append(ip)
    except Exception:
        pass

    for ip in ips:
        parts = ip.split(".")
        if len(parts) == 4 and not parts[0].startswith("127"):
            prefix = ".".join(parts[:3])
            if prefix not in [s[0] for s in subnets]:
                subnets.append((prefix, f"{prefix}.255", ip))

    if not subnets:
        subnets.append(("192.168.0", "192.168.0.255", "127.0.0.1"))

    return subnets


def discover_workers(timeout=1.0):
    """Discovery worker melalui UDP broadcast + TCP sweep."""
    workers = {}
    local_subnets = get_local_subnets()

    detected_ips = [s[2] for s in local_subnets]
    print(f"[*] IP Master Terdeteksi: {', '.join(detected_ips)}")

    broadcast_list = ["255.255.255.255", "<broadcast>"] + [
        s[1] for s in local_subnets
    ]

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.settimeout(0.2)

        for b_ip in broadcast_list:
            try:
                s.sendto(b"DISCOVER_WORKERS", (b_ip, DISCOVERY_PORT))
            except Exception:
                pass

        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                data, addr = s.recvfrom(4096)
                ip = addr[0]
                info = json.loads(data.decode("utf-8"))
                info["ip"] = ip
                workers[ip] = info
            except socket.timeout:
                continue
            except Exception:
                pass

    # TCP fallback
    targets = []
    for prefix, _, my_ip in local_subnets:
        for i in range(1, 255):
            t_ip = f"{prefix}.{i}"
            if t_ip not in workers and t_ip != my_ip:
                targets.append(t_ip)

    if targets:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=80) as ex:
            tcp_nodes = list(filter(None, ex.map(check_tcp_node, targets)))
            for node in tcp_nodes:
                workers[node["ip"]] = node

    return list(workers.values())


def send_task(worker, task_id, start_num, end_num):
    """
    Mengirim satu chunk.
    Return selalu memiliki status sehingga scheduler dapat melakukan retry.
    """
    t0 = time.time()

    if worker.get("is_local"):
        try:
            result = run_local_task(start_num, end_num)
            return {
                "ok": True,
                "task_id": task_id,
                "worker": worker,
                "start": start_num,
                "end": end_num,
                "result": result,
                "duration": time.time() - t0,
            }
        except Exception as e:
            return {
                "ok": False,
                "task_id": task_id,
                "worker": worker,
                "start": start_num,
                "end": end_num,
                "error": str(e),
                "duration": time.time() - t0,
            }

    ip = worker["ip"]
    port = worker.get("port", TCP_PORT)

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(TASK_TIMEOUT)
            s.connect((ip, port))

            payload = {
                "action": "compute",
                "task_id": task_id,
                "start": start_num,
                "end": end_num,
            }
            s.sendall(json.dumps(payload).encode("utf-8"))

            raw_res = s.recv(65536).decode("utf-8")
            if not raw_res:
                raise ConnectionError("Worker menutup koneksi tanpa mengirim hasil.")

            res = json.loads(raw_res)

            if res.get("task_id") != task_id:
                raise RuntimeError("task_id response tidak cocok.")

            return {
                "ok": "error" not in res,
                "task_id": task_id,
                "worker": worker,
                "start": start_num,
                "end": end_num,
                "result": res,
                "error": res.get("error"),
                "duration": time.time() - t0,
            }

    except Exception as e:
        return {
            "ok": False,
            "task_id": task_id,
            "worker": worker,
            "start": start_num,
            "end": end_num,
            "error": str(e),
            "duration": time.time() - t0,
        }


def parse_node_selection(input_str, total_nodes):
    """Mendukung all, *, enter, koma, rentang, dan spasi."""
    input_str = input_str.strip().lower()

    if not input_str or input_str in ["all", "*"]:
        return list(range(total_nodes))

    indices = []
    parts = input_str.replace(" ", ",").split(",")

    for p in parts:
        p = p.strip()
        if not p:
            continue

        if "-" in p:
            try:
                start, end = p.split("-")
                indices.extend(range(int(start), int(end) + 1))
            except ValueError:
                pass
        elif p.isdigit():
            indices.append(int(p))

    valid = []
    for i in indices:
        if 0 <= i < total_nodes and i not in valid:
            valid.append(i)

    return valid


def make_dynamic_chunks(start_num, end_num, total_cores):
    """
    Dynamic scheduling:
    membuat banyak chunk kecil agar worker cepat dapat pekerjaan berikutnya.
    Setiap worker tetap mengerjakan satu chunk pada satu waktu.
    """
    total_items = end_num - start_num
    target_chunks = max(1, total_cores * CHUNKS_PER_CORE)
    chunk_size = max(MIN_CHUNK_SIZE, math.ceil(total_items / target_chunks))

    chunks = []
    start = start_num
    seq = 1

    while start < end_num:
        end = min(start + chunk_size, end_num)
        chunks.append({
            "task_id": f"T{seq:04d}-{uuid.uuid4().hex[:6]}",
            "start": start,
            "end": end,
            "retry": 0,
        })
        start = end
        seq += 1

    return chunks, chunk_size


def run_dynamic_scheduler(selected_workers, total_target):
    """
    Scheduler load balancing + fault tolerance.

    Load balancing:
      - pekerjaan dibagi menjadi banyak chunk;
      - worker yang selesai lebih cepat langsung mengambil chunk berikutnya.

    Fault tolerance:
      - jika koneksi gagal/timeout/error, chunk dikembalikan ke queue;
      - worker bermasalah dinonaktifkan;
      - chunk dapat dikerjakan worker lain sampai MAX_RETRIES.
    """
    total_cores = sum(w.get("cores", 1) for w in selected_workers)
    chunks, chunk_size = make_dynamic_chunks(2, total_target, total_cores)

    print("\n" + "=" * 68)
    print("   [DYNAMIC SCHEDULING] LOAD BALANCING + FAULT TOLERANCE")
    print("=" * 68)
    print(f"[*] Total data           : {total_target - 2:,} angka")
    print(f"[*] Total core cluster   : {total_cores} core")
    print(f"[*] Ukuran chunk         : {chunk_size:,} angka")
    print(f"[*] Jumlah chunk         : {len(chunks)}")
    print("[*] Worker yang selesai lebih cepat akan mengambil chunk berikutnya.")
    print("[*] Chunk gagal akan di-requeue ke worker lain.\n")

    pending = chunks[:]
    active_workers = {i: w for i, w in enumerate(selected_workers)}
    failed_workers = set()
    completed_task_ids = set()
    accepted_results = []
    in_flight = {}
    executor = ThreadPoolExecutor(max_workers=max(1, len(selected_workers)))

    try:
        while pending or in_flight:
            # Isi semua slot worker yang kosong.
            for worker_idx, worker in list(active_workers.items()):
                if worker_idx in in_flight:
                    continue
                if not pending:
                    break

                task = pending.pop(0)
                future = executor.submit(
                    send_task,
                    worker,
                    task["task_id"],
                    task["start"],
                    task["end"],
                )
                in_flight[worker_idx] = {
                    "future": future,
                    "task": task,
                    "worker": worker,
                    "submitted": time.time(),
                }

                print(
                    f"[DISPATCH] {task['task_id']} -> {worker['host']} "
                    f"({task['start']:,}-{task['end']-1:,})"
                )

            # Periksa task yang selesai / timeout.
            progress = False
            for worker_idx, info in list(in_flight.items()):
                future = info["future"]
                task = info["task"]
                worker = info["worker"]
                elapsed = time.time() - info["submitted"]

                if future.done():
                    progress = True
                    del in_flight[worker_idx]

                    try:
                        result = future.result()
                    except Exception as e:
                        result = {
                            "ok": False,
                            "task_id": task["task_id"],
                            "worker": worker,
                            "start": task["start"],
                            "end": task["end"],
                            "error": str(e),
                            "duration": elapsed,
                        }

                    if (
                        result.get("ok")
                        and task["task_id"] not in completed_task_ids
                    ):
                        completed_task_ids.add(task["task_id"])
                        accepted_results.append(result)
                        r = result["result"]
                        print(
                            f"[OK] {task['task_id']} <- {worker['host']} | "
                            f"prima={r.get('primes_found', 0):,} | "
                            f"{result['duration']:.2f}s"
                        )
                    else:
                        # Jangan menghitung hasil dua kali.
                        if task["retry"] < MAX_RETRIES and worker_idx in active_workers:
                            task["retry"] += 1
                            pending.append(task)
                            print(
                                f"[RETRY] {task['task_id']} | "
                                f"worker={worker['host']} | "
                                f"percobaan={task['retry']}/{MAX_RETRIES}"
                            )
                        else:
                            failed_workers.add(worker_idx)
                            active_workers.pop(worker_idx, None)
                            print(
                                f"[FAIL] {task['task_id']} gagal permanen pada "
                                f"{worker['host']}: {result.get('error')}"
                            )
                            # Requeue sekali lagi agar chunk tidak hilang.
                            if task["retry"] < MAX_RETRIES:
                                task["retry"] += 1
                                pending.append(task)

                elif elapsed > TASK_TIMEOUT:
                    progress = True
                    del in_flight[worker_idx]

                    print(
                        f"[TIMEOUT] {task['task_id']} pada {worker['host']} "
                        f"({elapsed:.1f}s) -> worker dinonaktifkan."
                    )

                    failed_workers.add(worker_idx)
                    active_workers.pop(worker_idx, None)

                    # Future thread mungkin masih berjalan di background.
                    # Hasilnya tidak akan diterima lagi karena task di-requeue.
                    if task["retry"] < MAX_RETRIES:
                        task["retry"] += 1
                        pending.append(task)

            if not active_workers and pending:
                raise RuntimeError(
                    "Semua worker gagal/nonaktif sementara masih ada chunk "
                    "yang belum selesai. Komputasi tidak dapat dilanjutkan."
                )

            if not progress:
                time.sleep(0.05)

    finally:
        executor.shutdown(wait=False)

    return accepted_results, failed_workers, chunks


def main():
    print("=" * 68)
    print("      DISTRIBUTED PARALLEL COMPUTING - MASTER")
    print("      Dynamic Load Balancing + Fault Tolerance")
    print("=" * 68)
    print("[*] Memindai Worker di seluruh jaringan LAN...")

    remote_workers = discover_workers(timeout=1.5)

    local_worker = {
        "host": socket.gethostname(),
        "ip": "127.0.0.1",
        "cores": os.cpu_count() or 4,
        "is_local": True,
        "port": TCP_PORT,
    }

    all_workers = [local_worker] + [
        w for w in remote_workers
        if w.get("host") != socket.gethostname()
    ]

    print(f"\n[+] Terdeteksi {len(all_workers)} Node Komputasi:")
    print("-" * 68)

    for idx, w in enumerate(all_workers):
        label = "Master Lokal" if w.get("is_local") else "Client LAN"
        print(
            f"  [{idx}] {w['host']:<20} | "
            f"{w.get('cores', 1):>2} Core | {label}"
        )

    print("-" * 68)
    print("\nFormat pemilihan:")
    print("  ENTER/all/* : semua node")
    print("  0,1,2       : node tertentu")
    print("  0-2         : rentang node")
    print("  add 192.168.0.50 : tambah worker manual")

    selected_workers = []

    while not selected_workers:
        default_all = f"0-{len(all_workers)-1}" if len(all_workers) > 1 else "0"
        pilihan = input(
            f"\nMasukkan pilihan node [Default: {default_all}]: "
        ).strip()

        if pilihan.lower().startswith("add "):
            parts = pilihan.split()
            if len(parts) < 2:
                print("[-] Format: add IP_ADDRESS")
                continue

            target_ip = parts[1]
            print(f"[*] Menghubungi {target_ip}...")
            manual_node = check_tcp_node(target_ip)

            if not manual_node:
                manual_node = {
                    "host": f"Node-{target_ip}",
                    "cores": 4,
                    "port": TCP_PORT,
                    "ip": target_ip,
                }

            all_workers.append(manual_node)
            print(
                f"[+] Ditambahkan [{len(all_workers)-1}] "
                f"{manual_node['host']} ({target_ip})"
            )
            continue

        selected_idx = parse_node_selection(pilihan, len(all_workers))
        if selected_idx:
            selected_workers = [all_workers[i] for i in selected_idx]
        else:
            print("[-] Pilihan tidak valid.")

    print("\n[✓] Worker terpilih:")
    total_cores = 0
    for i, w in enumerate(selected_workers, 1):
        cores = w.get("cores", 1)
        total_cores += cores
        tipe = "Master" if w.get("is_local") else "Client"
        print(
            f"  {i}. {w['host']:<20} {w['ip']:<15} "
            f"{cores} Core [{tipe}]"
        )
    print(f"  Total kapasitas: {total_cores} Core")

    print("\n" + "-" * 68)
    print("Studi Kasus: Pencarian Bilangan Prima Skala Besar")
    print("Interval menggunakan notasi [start, end), end tidak ikut dihitung.")
    print("-" * 68)

    input_total = input(
        "Masukkan batas atas angka [Default: 30000000]: "
    ).strip()
    total_target = (
        int(input_total)
        if input_total.isdigit() and int(input_total) > 2
        else 30_000_000
    )

    t_start = time.time()

    try:
        hasil_cluster, failed_workers, all_chunks = run_dynamic_scheduler(
            selected_workers,
            total_target,
        )
    except Exception as e:
        print(f"\n[✗] CLUSTER GAGAL: {e}")
        return

    t_end = time.time()
    waktu_cluster = t_end - t_start

    total_primes = sum(
        r["result"].get("primes_found", 0)
        for r in hasil_cluster
    )
    total_items = total_target - 2
    total_completed_items = sum(
        r["end"] - r["start"] for r in hasil_cluster
    )

    print("\n" + "=" * 68)
    print("   [GATHER] HASIL AKHIR CLUSTER")
    print("=" * 68)

    print("Rincian chunk yang berhasil:")
    for r in sorted(hasil_cluster, key=lambda x: x["start"]):
        w = r["worker"]
        result = r["result"]
        print(
            f"  {r['task_id']} | {w['host']:<20} | "
            f"{r['start']:,}-{r['end']-1:,} | "
            f"{result.get('primes_found', 0):,} prima | "
            f"{r['duration']:.2f}s"
        )

    print("-" * 68)
    print(f"Total angka target       : {total_items:,}")
    print(f"Total angka selesai      : {total_completed_items:,}")
    print(f"Total bilangan prima     : {total_primes:,}")
    print(f"Waktu cluster            : {waktu_cluster:.2f} detik")

    if failed_workers:
        print(
            f"Worker dinonaktifkan     : {len(failed_workers)} "
            f"(fault tolerance aktif)"
        )

    if len(hasil_cluster) > 1 and waktu_cluster > 0:
        serial_sum = sum(r["duration"] for r in hasil_cluster)
        speedup = serial_sum / waktu_cluster
        print(f"Speedup estimasi         : {speedup:.2f}x")

    if total_completed_items != total_items:
        print(
            f"[WARNING] Ada {total_items - total_completed_items:,} "
            "angka yang belum terselesaikan."
        )
    else:
        print("[✓] Seluruh workload berhasil diselesaikan.")

    print("=" * 68)


if __name__ == "__main__":
    main()
