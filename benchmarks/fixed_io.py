#!/usr/bin/env python3
"""Reproducible fixed-record I/O benchmark; all methods fsync and rename outputs."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
RECORD_SIZE = 256


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def prepare_cache(path, policy):
    with path.open("rb") as f:
        if policy == "warm":
            while f.read(8 * 1024 * 1024):
                pass
        else:
            os.posix_fadvise(f.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--size-mib", type=int, default=256)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--threads", type=int, nargs="+", default=[1, 4], help="dxt thread counts to compare")
    parser.add_argument("--cache", choices=["warm", "cold-advisory"], default="warm")
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--result", type=Path, default=ROOT / "benchmarks/results/fixed-io.json")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--skip-codec", action="store_true", help="compare raw I/O and dxt only")
    parser.add_argument("--cases", nargs="+", help="run only these case names")
    parser.add_argument("--pause-ms", type=int, default=0, help="idle between trials, outside measured time")
    parser.add_argument("--compare-dxt", type=Path, help="also run a saved pre-change release dxt")
    parser.add_argument("--compare-codec", type=Path, help="also run a saved pre-change codec example")
    args = parser.parse_args()
    if args.size_mib < 1 or args.repeats < 1 or args.warmups < 0:
        parser.error("size/repeats must be positive and warmups nonnegative")
    if any(n < 1 for n in args.threads) or len(set(args.threads)) != len(args.threads):
        parser.error("threads must be distinct positive integers")
    if args.skip_codec and args.compare_codec:
        parser.error("--compare-codec cannot be combined with --skip-codec")
    if args.pause_ms < 0:
        parser.error("pause-ms must be nonnegative")
    release = ROOT / "target/release"
    baseline = release / "fixed-copy-baseline"
    if not args.skip_build:
        command = ["cargo", "build", "--release", "--locked", "--offline", "--bin", "dxt", "-j", "3"]
        if not args.skip_codec:
            command += ["--example", "fixed_codec_copy"]
        subprocess.run(command, cwd=ROOT, check=True)
    subprocess.run(["rustc", "--edition=2024", "-C", "opt-level=3", str(ROOT / "benchmarks/fixed_copy.rs"), "-o", str(baseline)], check=True)
    binaries = {"dxt": release / "dxt"}
    if not args.skip_codec:
        binaries["codec"] = release / "examples/fixed_codec_copy"
    comparisons = {name: path.resolve() for name, path in
                   (("dxt", args.compare_dxt), ("codec", args.compare_codec)) if path}
    for path in [*binaries.values(), *comparisons.values()]:
        if not path.is_file():
            parser.error(f"missing {path}; build without --skip-build first")
    args.result = args.result.resolve()
    args.result.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="dxtran-bench-", dir=args.work_dir) as folder:
        work = Path(folder)
        stages = work / "stages"
        stages.mkdir()
        input_path = work / "input.dat"
        output_path = work / "output.dat"
        stage = stages / "baseline.tmp"
        # 8 ASCII text fields and 8 ASCII unsigned zoned decimal fields, 16 bytes each.
        # The same bytes are also valid as a single 256-byte string.
        records = []
        for i in range(4096):
            records.append(b"".join(f"T{j:02}{i:013}".encode() for j in range(8)) + b"".join(f"{i * 8 + j:016}".encode() for j in range(8)))
        block = b"".join(records)
        target_bytes = args.size_mib * 1024 * 1024
        with input_path.open("wb") as f:
            remaining = target_bytes
            while remaining:
                chunk = block[:remaining]
                f.write(chunk)
                remaining -= len(chunk)
            f.flush()
            os.fsync(f.fileno())
        expected = digest(input_path)
        jobs = {}
        for layout in ("one_field", "sixteen_fields"):
            fields = ([{"name": "payload", "length": 256, "trim": "none"}] if layout == "one_field" else
                      [{"name": f"text_{j}", "length": 16} for j in range(8)] +
                      [{"name": f"amount_{j}", "length": 16, "type": "uzoned", "zone": "ascii", "precision": 16, "scale": 2} for j in range(8)])
            common = {"format": "fixed", "encoding": "utf-8", "fixed": {"record_separator": "none", "record_length": RECORD_SIZE}, "fields": fields}
            job = {"input": [{**common, "id": "data", "path": str(input_path)}], "process": [],
                   "output": [{**common, "id": "result", "path": str(output_path), "sql": "SELECT * FROM data"}]}
            path = work / f"{layout}.json"
            path.write_text(json.dumps(job), encoding="utf-8")
            jobs[layout] = path
        cases = {"block_copy_1mib": [str(baseline), "block", str(input_path), str(output_path), str(stage)],
                 "record_copy_256b": [str(baseline), "record", str(input_path), str(output_path), str(stage)]}
        for layout, job in jobs.items():
            if not args.skip_codec:
                cases[f"codecs_{layout}"] = [str(binaries["codec"]), str(job), str(stage)]
            if "codec" in comparisons:
                cases[f"before_codecs_{layout}"] = [str(comparisons["codec"]), str(job), str(stage)]
            for threads in args.threads:
                cases[f"dxt_{layout}_threads_{threads}"] = [str(binaries["dxt"]), "run", str(job), "--threads", str(threads), "--memory-limit", "512MiB", "--temp-dir", str(stages)]
                if "dxt" in comparisons:
                    cases[f"before_dxt_{layout}_threads_{threads}"] = [str(comparisons["dxt"]), *cases[f"dxt_{layout}_threads_{threads}"][1:]]
        if args.cases:
            if set(args.cases) - cases.keys():
                parser.error(f"unknown cases: {sorted(set(args.cases) - cases.keys())}")
            if "record_copy_256b" not in args.cases:
                parser.error("cases must include record_copy_256b as the reference")
            cases = {name: cases[name] for name in args.cases}
        plans = {}
        for layout, job in jobs.items():
            plans[layout] = subprocess.run([str(binaries["dxt"]), "explain", str(job)], check=True, text=True, capture_output=True).stdout
        results = {"environment": {"platform": platform.platform(), "cpu_count": os.cpu_count(), "affinity": sorted(os.sched_getaffinity(0)),
                                  "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
                                  "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
                                  "dxt_sha256": digest(binaries["dxt"]), "codec_sha256": digest(binaries["codec"]) if "codec" in binaries else None, "baseline_sha256": digest(baseline),
                                  "comparison_sha256": {name: digest(path) for name, path in comparisons.items()},
                                  "filesystem": subprocess.check_output(["stat", "-f", "-c", "%T", str(work)], text=True).strip(),
                                  "cpu_max": Path("/sys/fs/cgroup/cpu.max").read_text().strip(), "memory_max": Path("/sys/fs/cgroup/memory.max").read_text().strip()},
                   "settings": {"size_bytes": target_bytes, "record_bytes": RECORD_SIZE, "rows": target_bytes // RECORD_SIZE, "cache": args.cache,
                                "repeats": args.repeats, "warmups": args.warmups, "threads": args.threads, "build_profile": "release", "fsync": True, "skip_codec": args.skip_codec,
                                "date_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "input_sha256": expected, "order_seed": 20261025,
                                "cases": list(cases), "pause_ms": args.pause_ms},
                   "plans": plans, "runs": [], "summary": {}}
        rng = random.Random(20261025)
        for phase, count in (("warmup", args.warmups), ("measured", args.repeats)):
            for repetition in range(count):
                order = list(cases)
                rng.shuffle(order)
                for name in order:
                    output_path.unlink(missing_ok=True)
                    stage.unlink(missing_ok=True)
                    prepare_cache(input_path, args.cache)
                    metrics_file = work / "time.json"
                    start = time.perf_counter()
                    process = subprocess.run(["/usr/bin/time", "-f", '{"user_s":%U,"system_s":%S,"max_rss_kib":%M,"input_blocks":%I,"output_blocks":%O}', "-o", str(metrics_file), *cases[name]], text=True, capture_output=True)
                    elapsed = time.perf_counter() - start
                    if process.returncode:
                        raise RuntimeError(f"{name} failed: {process.stderr}")
                    if output_path.stat().st_size != target_bytes or digest(output_path) != expected:
                        raise RuntimeError(f"{name}: output differs from input")
                    metrics = json.loads(metrics_file.read_text())
                    events = [json.loads(line) for line in process.stderr.splitlines() if line.startswith("{")]
                    staged = next((e for e in events if e.get("event") == "output_staged"), {})
                    record = {"case": name, "phase": phase, "repetition": repetition + 1, "wall_s": elapsed,
                              "mib_per_s": target_bytes / (1024 * 1024) / elapsed, **metrics,
                              "spill_count": staged.get("spill_count", 0), "spilled_bytes": staged.get("spilled_bytes", 0)}
                    results["runs"].append(record)
                    args.result.write_text(json.dumps(results, indent=2) + "\n")
                    print(f"{phase} {repetition+1}/{count} {name}: {elapsed:.3f}s {record['mib_per_s']:.1f} MiB/s RSS {metrics['max_rss_kib']/1024:.1f} MiB", flush=True)
                    if args.pause_ms:
                        time.sleep(args.pause_ms / 1000)
        for name in cases:
            samples = [r for r in results["runs"] if r["case"] == name and r["phase"] == "measured"]
            median = statistics.median(r["wall_s"] for r in samples)
            results["summary"][name] = {"median_s": median, "min_s": min(r["wall_s"] for r in samples), "max_s": max(r["wall_s"] for r in samples),
                                        "mib_per_s": args.size_mib / median, "median_rss_mib": statistics.median(r["max_rss_kib"] for r in samples) / 1024,
                                        "median_cpu_s": statistics.median(r["user_s"] + r["system_s"] for r in samples), "spill_count": sum(r["spill_count"] for r in samples)}
        reference = results["summary"]["record_copy_256b"]["median_s"]
        for entry in results["summary"].values():
            entry["slowdown_vs_record_copy"] = entry["median_s"] / reference
        assert digest(binaries["dxt"]) == results["environment"]["dxt_sha256"], "dxt changed during measurement"
        args.result.write_text(json.dumps(results, indent=2) + "\n")
        print(json.dumps(results["summary"], indent=2), flush=True)
        print(f"Results: {args.result}", flush=True)


if __name__ == "__main__":
    main()
