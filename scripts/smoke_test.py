# scripts/smoke_test.py
"""Smoke test end-to-end: dynamic LB, static LB, dan fault tolerance lokal."""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.metrics_logger import MetricsLogger  # noqa: E402
from utils.video_utils import count_video_frames  # noqa: E402


def ensure_video(path: str, force: bool = False) -> None:
    if os.path.exists(path) and not force:
        return
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "make_sample_video.py"),
        "--output", path,
        "--width", "640",
        "--height", "360",
        "--seconds", "6",
        "--fps", "30",
    ]
    if force:
        cmd.append("--force")
    subprocess.run(cmd, cwd=str(ROOT), check=True)


def latest_row(csv_path: str, tag: str) -> dict | None:
    rows = [row for row in MetricsLogger.read_all(csv_path) if row.get("tag") == tag]
    return rows[-1] if rows else None


def run_case(
    name: str,
    output_path: str,
    video_path: str,
    csv_path: str,
    expected_frames: int,
    extra_args: list[str],
    timeout: float,
    expect_requeue: bool = False,
) -> bool:
    print("\n" + "=" * 72)
    print(f"SMOKE CASE: {name}")
    print("=" * 72, flush=True)
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "local_cluster.py"),
        "--workers", "2",
        "--video", video_path,
        "--output", output_path,
        "--frame-limit", str(expected_frames),
        "--batch-size", "10",
        "--target-res", "640x360",
        "--cores-per-worker", "2",
        "--csv", csv_path,
        "--tag", name,
        "--startup-timeout", "60",
        "--timeout", str(timeout),
        *extra_args,
    ]
    result = subprocess.run(cmd, cwd=str(ROOT), timeout=timeout + 60)
    if result.returncode != 0:
        print(f"[FAIL] {name}: local_cluster exit {result.returncode}")
        return False

    if not os.path.exists(output_path):
        print(f"[FAIL] {name}: output tidak ada: {output_path}")
        return False
    frames = count_video_frames(output_path)
    if frames != expected_frames:
        print(f"[FAIL] {name}: frame output {frames}, seharusnya {expected_frames}")
        return False

    row = latest_row(csv_path, name)
    if not row:
        print(f"[FAIL] {name}: baris metrik tidak ditemukan di {csv_path}")
        return False
    status = row.get("status", "")
    lost = int(float(row.get("lost_frames") or 0))
    requeued = int(float(row.get("requeued_tasks") or 0))
    recovery = float(row.get("recovery_time_sec") or 0)
    if status != "completed":
        print(f"[FAIL] {name}: status {status!r}")
        return False
    if lost != 0:
        print(f"[FAIL] {name}: lost_frames={lost}")
        return False
    if expect_requeue and requeued < 1:
        print(f"[FAIL] {name}: requeued_tasks={requeued}, seharusnya >= 1")
        return False
    print(
        f"[PASS] {name}: {frames} frame, status={status}, "
        f"requeued={requeued}, recovery={recovery:.4f}s, lost={lost}"
    )
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test end-to-end")
    parser.add_argument("--force-video", action="store_true")
    parser.add_argument("--skip-static", action="store_true")
    parser.add_argument("--timeout", type=float, default=240.0)
    args = parser.parse_args()

    data_dir = ROOT / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    video_path = str(data_dir / "input_smoke.mp4")
    ensure_video(video_path, force=args.force_video)

    csv_path = str(ROOT / "logs" / "smoke_results.csv")
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    results = []
    results.append(
        run_case(
            name="smoke_dynamic",
            output_path=str(data_dir / "output_smoke_dynamic.mp4"),
            video_path=video_path,
            csv_path=csv_path,
            expected_frames=40,
            extra_args=["--lb", "dynamic"],
            timeout=args.timeout,
        )
    )
    if not args.skip_static:
        results.append(
            run_case(
                name="smoke_static",
                output_path=str(data_dir / "output_smoke_static.mp4"),
                video_path=video_path,
                csv_path=csv_path,
                expected_frames=40,
                extra_args=["--lb", "static"],
                timeout=args.timeout,
            )
        )
    results.append(
        run_case(
            name="smoke_fault",
            output_path=str(data_dir / "output_smoke_fault.mp4"),
            video_path=video_path,
            csv_path=csv_path,
            expected_frames=40,
            extra_args=[
                "--lb", "dynamic",
                "--crash-worker-index", "0",
                "--crash-on-task", "1",
            ],
            timeout=args.timeout,
            expect_requeue=True,
        )
    )

    print("\n" + "=" * 72)
    passed = sum(1 for item in results if item)
    print(f"SMOKE TEST: {passed}/{len(results)} PASS")
    print("=" * 72)
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
