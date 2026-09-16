#!/usr/bin/env python3
"""Run the release benchmark matrix on one host, retaining logs and samples.

Run separate hosts concurrently, but only one instance per host. Choose nine
distinct physical CPU IDs on Linux. All workers are assigned explicitly. Supply
the wider comparison CPU pool in physical-core-first order; workers exceeding
that pool wrap around and share CPUs. On
macOS the IDs request user-interactive QoS only, not CPU affinity.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cpus", default="0,1,2,3,4,5,6,7,8")
    parser.add_argument("--comparison-cpus", help="complete CPU pool for the raw/real-case harnesses")
    parser.add_argument("--toolchain", default="1.97.1")
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--stage", choices=["all", "validate", "mpsc", "comparison"], default="all")
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    cpus = [int(c) for c in args.cpus.split(",")]
    comparison_cpus = [int(c) for c in args.comparison_cpus.split(",")] if args.comparison_cpus else cpus
    assert len(cpus) == 9 and len(set(cpus)) == 9, "provide nine distinct CPUs"
    assert comparison_cpus and len(set(comparison_cpus)) == len(comparison_cpus)
    if platform.system() == "Linux":
        assert set(cpus) <= os.sched_getaffinity(0), "CPU not allowed by affinity mask"
        assert set(comparison_cpus) <= os.sched_getaffinity(0)
    env = os.environ.copy()
    env.update({"RUSTFLAGS": "-C target-cpu=native", "CARGO_BUILD_JOBS": "4",
                "CARGO_TARGET_DIR": str(output / "target"), "RUSTUP_AUTO_INSTALL": "0"})
    cargo = ["cargo", "+" + args.toolchain]
    manifest_file = output / "manifest.json"
    hashes = {str(p.relative_to(repo)): hashlib.sha256(p.read_bytes()).hexdigest()
              for p in sorted(repo.rglob("*.rs")) if "target" not in p.parts and ".git" not in p.parts}
    for name in ["Cargo.toml", "Cargo.lock", "benches/run_matrix.py"]:
        hashes[name] = hashlib.sha256((repo / name).read_bytes()).hexdigest()
    if manifest_file.exists():
        manifest = json.loads(manifest_file.read_text())
        assert manifest["source_sha256"] == hashes, "source changed between stages"
        assert manifest["cpus"] == cpus, "CPU placement changed between stages"
        assert manifest["comparison_cpus"] == comparison_cpus
    else:
        manifest = {"source_commit": args.source_commit, "source_sha256": hashes,
                    "system": platform.system(), "kernel": platform.release(),
                    "architecture": platform.machine(), "cpus": cpus,
                    "comparison_cpus": comparison_cpus,
                    "placement": "checked affinity" if platform.system() == "Linux" else "QoS only; unpinned",
                    "rustflags": env["RUSTFLAGS"], "commands": []}
        if platform.system() == "Linux":
            topology = json.loads(subprocess.check_output(["lscpu", "-J"], text=True))
            manifest["cpu_topology"] = topology["lscpu"]
        elif platform.system() == "Darwin":
            manifest["cpu_model"] = subprocess.check_output(
                ["sysctl", "-n", "machdep.cpu.brand_string"], text=True).strip()
            manifest["memory_bytes"] = int(subprocess.check_output(["sysctl", "-n", "hw.memsize"]))

    def save():
        temporary = manifest_file.with_suffix(".tmp")
        temporary.write_text(json.dumps(manifest, indent=2) + "\n")
        temporary.replace(manifest_file)

    def run(label, command, extra=None, timeout=3600):
        previous = [r for r in manifest["commands"] if r["label"] == label]
        if previous and previous[-1].get("exit_code") == 0:
            print(label + ": already recorded", flush=True)
            return (output / (label + ".log")).read_text()
        run_env = dict(env, **(extra or {}))
        record = {"label": label, "argv": [str(s).replace(str(output), "$OUTPUT") for s in command],
                  "environment": extra or {}, "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                  "load_before": os.getloadavg()}
        started = time.monotonic()
        manifest["commands"].append(record)
        save()
        print(label + ": started", flush=True)
        logfile = output / (label + ".log")
        with logfile.open("w") as log:
            process = subprocess.Popen(command, cwd=repo, env=run_env, stdout=log,
                                       stderr=subprocess.STDOUT, start_new_session=True)
            try:
                code = process.wait(timeout=timeout)
            except (subprocess.TimeoutExpired, KeyboardInterrupt):
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                code = 124
        record.update({"exit_code": code, "elapsed_seconds": time.monotonic() - started,
                       "load_after": os.getloadavg(), "log_sha256": hashlib.sha256(logfile.read_bytes()).hexdigest()})
        save()
        print(label + ": exit=" + str(code), flush=True)
        if code:
            raise RuntimeError(label + " failed; see " + str(logfile))
        return logfile.read_text()

    run("toolchain", ["rustc", "+" + args.toolchain, "-vV"], timeout=30)
    if args.stage in ("all", "validate"):
        run("native-tests", cargo + ["test", "--locked"], timeout=1200)
        run("small-release-tests", cargo + ["test", "--release", "--locked", "--lib", "--tests"],
            {"RUSTFLAGS": "-C target-cpu=native --cfg fcrs_small_blocks"}, timeout=1800)

    if args.stage in ("all", "mpsc"):
        run("build-mpsc", cargo + ["build", "--release", "--locked", "--example", "mpsc_compare"], timeout=1800)
        binary = output / "target/release/examples/mpsc_compare"
        manifest["mpsc_binary_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
        save()
        for batch in range(3):
            for size in (64, 256, 1024):
                for mode, producers, assigned in [("sync", "4,8", cpus), ("async", "4,40", cpus[:4])]:
                    label = "mpsc-{}-{}-{}".format(batch, mode, size)
                    text = run(label, [str(binary), "500003", "5", producers, mode, str(size)],
                               {"MPSC_PIN": ",".join(map(str, assigned))}, timeout=1800)
                    rows = [json.loads(line) for line in text.splitlines() if line.startswith("{")]
                    assert len(rows) == (12 if mode == "sync" else 24), label + ": incomplete rows"
                    assert all(len(row["samples"]) == 5 for row in rows), label + ": incomplete samples"
        text = run("mpsc-latency", [str(binary), "16003", "5", "4,40", "latency", "64"],
                   {"MPSC_PIN": ",".join(map(str, cpus[:4]))}, timeout=1800)
        rows = [json.loads(line) for line in text.splitlines() if line.startswith("{")]
        assert len(rows) == 24 and all(len(r["samples_p50_p99_max_ns"]) == 5 for r in rows)

    if args.stage in ("all", "comparison"):
        # 128 explicit entries cover every worker, including 40 producers + consumer.
        pins = ",".join(str(comparison_cpus[i % len(comparison_cpus)]) for i in range(128))
        for bench, switch, n in [("raw_bench", "FCRS_RAW_BENCH_FULL", 4000000),
                                 ("real_case", "FCRS_REAL_BENCH", 2000000)]:
            prefix = "FCRS_RAW_BENCH" if bench == "raw_bench" else "FCRS_REAL_BENCH"
            run("build-" + bench, cargo + ["test", "--release", "--locked", "--bench", bench, "--no-run"], timeout=1800)
            text = run(bench, cargo + ["test", "--release", "--locked", "--bench", bench, "--", "--nocapture"],
                       {switch: "1", prefix + "_ARGS": "--json --n=" + str(n) + " --pin=" + pins}, timeout=7200)
            rows = [json.loads(line) for line in text.splitlines() if line.startswith("{")]
            expected = 104 if bench == "raw_bench" else 180
            assert len(rows) == expected, bench + ": incomplete result matrix"
            assert all(len(r["samples_ns_per_op"]) == r["runs"] for r in rows)
            assert any(r["impl"] == "rapidfire_mpsc" for r in rows)
    manifest.setdefault("completed_stages", []).append(args.stage)
    save()
    print("stage complete: " + args.stage, flush=True)


if __name__ == "__main__":
    main()
