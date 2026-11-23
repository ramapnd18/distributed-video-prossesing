# simulate_cluster.py
"""Alias root untuk scripts/local_cluster.py (1 Master + N Worker di satu PC).

Default: 9 Worker x 1 core, agar mendekati skenario 10 node saat uji lokal.
Pengujian UAS tetap wajib memakai 10 node fisik.
"""
from __future__ import annotations

import sys

from scripts.local_cluster import main

if __name__ == "__main__":
    if "--workers" not in sys.argv:
        sys.argv.extend(["--workers", "9"])
    if "--cores-per-worker" not in sys.argv:
        sys.argv.extend(["--cores-per-worker", "1"])
    raise SystemExit(main())
