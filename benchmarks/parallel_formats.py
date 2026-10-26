#!/usr/bin/env python3
"""Compare CSV/Parquet input and CSV output, including exact independent oracles."""
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

from fixed_io import ROOT, digest, prepare_cache


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--size-mib", type=int, default=256)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--threads", type=int, nargs="+", default=[1, 4, 8])
    parser.add_argument("--compare-dxt", type=Path, required=True)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--result", type=Path, default=ROOT / "benchmarks/results/parallel-formats.json")
    args = parser.parse_args()
    if args.size_mib < 1 or args.repeats < 1 or args.warmups < 0 or any(t < 1 for t in args.threads):
        parser.error("positive size/repeats/threads and nonnegative warmups required")
    if len(set(args.threads)) != len(args.threads):
        parser.error("duplicate thread counts")
    if not args.skip_build:
        subprocess.run(["cargo", "build", "--release", "--locked", "--offline", "--bin", "dxt", "--example", "parquet_fixture", "-j3"], cwd=ROOT, check=True)
    binaries = {"before": args.compare_dxt.resolve(), "after": ROOT / "target/release/dxt"}
    fixture = ROOT / "target/release/examples/parquet_fixture"
    for binary in [*binaries.values(), fixture]:
        if not binary.is_file():
            parser.error(f"missing binary: {binary}")
    args.result = args.result.resolve()
    args.result.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="dxtran-parallel-formats-", dir=args.work_dir) as folder:
        work = Path(folder)
        stages = work / "stages"
        stages.mkdir()
        rows = args.size_mib * 1024 * 1024 // 256
        metadata = json.loads(subprocess.check_output([str(fixture), str(work), str(rows)], text=True))
        csv_path = work / "expected.csv"
        # The fixture writes each row as 8 x 15-byte text + 8 x 15-byte decimal,
        # plus 15 delimiters and LF, independently of dxtran's readers/writers.
        assert csv_path.stat().st_size == rows * 256
        csv_hash = digest(csv_path)
        cents = rows // 100 * 4950 + (rows % 100) * (rows % 100 - 1) // 2
        totals = [((100_000_000_000 + j) * rows + 8 * rows * (rows - 1) // 2) * 100 + cents for j in range(8)]
        aggregate = (",".join(f"{n//100}.{n%100:02}" for n in totals) + "\n").encode()
        fields = [{"name": f"text_{j}"} for j in range(8)] + [
            {"name": f"amount_{j}", "type": "decimal", "precision": 16, "scale": 2} for j in range(8)]
        queries = {"copy": "SELECT * FROM data", "aggregate": "SELECT " + ",".join(f"SUM(amount_{j}) AS amount_{j}" for j in range(8)) + " FROM data"}
        output = work / "output.csv"
        cases, plans, layouts = {}, {}, {}
        for source in ["csv", *metadata]:
            path = csv_path if source == "csv" else work / f"{source}.parquet"
            layouts[source] = {"rows": rows, "csv_equivalent_bytes": rows * 256,
                "file_bytes": path.stat().st_size, "sha256": digest(path), **metadata.get(source, {})}
            inp = {"id": "data", "path": str(path), "format": "csv" if source == "csv" else "parquet"}
            if source == "csv":
                inp.update(encoding="utf-8", fields=fields, csv={"header": False})
            for query, sql in queries.items():
                name = f"{source}_{query}"
                config = {"input": [inp], "output": [{"id": "result", "path": str(output), "format": "csv",
                    "encoding": "utf-8", "csv": {"header": False}, "sql": sql}]}
                job = work / f"{name}.json"
                job.write_text(json.dumps(config))
                plans[name] = {}
                for version, binary in binaries.items():
                    plans[name][version] = subprocess.check_output([str(binary), "explain", str(job)], text=True)
                    for threads in args.threads:
                        command = [str(binary), "run", str(job), "--threads", str(threads), "--memory-limit", "512MiB", "--temp-dir", str(stages)]
                        cases[f"{version}_{name}_threads_{threads}"] = (source, query, path, command)
        results = {"environment": {"platform": platform.platform(), "cpu_count": os.cpu_count(),
            "cpu_model": next(line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")),
            "affinity": sorted(os.sched_getaffinity(0)), "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
            "filesystem": subprocess.check_output(["stat", "-f", "-c", "%T", str(work)], text=True).strip(),
            "binaries": {name: {"path": str(path), "sha256": digest(path)} for name, path in binaries.items()}, "fixture_sha256": digest(fixture)},
            "settings": {"date_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "size_mib": args.size_mib,
                "repeats": args.repeats, "warmups": args.warmups, "threads": args.threads, "cache": "warm", "fsync": True,
                "build_profile": "release", "order_seed": 20261027}, "layouts": layouts, "plans": plans,
            "oracle": {"copy_sha256": csv_hash, "aggregate": aggregate.decode()}, "runs": [], "summary": {}}
        rng = random.Random(20261027)
        for phase, count in [("warmup", args.warmups), ("measured", args.repeats)]:
            for repetition in range(count):
                order = list(cases)
                rng.shuffle(order)
                for name in order:
                    source, query, path, command = cases[name]
                    output.unlink(missing_ok=True)
                    prepare_cache(path, "warm")
                    metrics = work / "time.json"
                    start = time.perf_counter()
                    process = subprocess.run(["/usr/bin/time", "-f", '{"user_s":%U,"system_s":%S,"max_rss_kib":%M,"input_blocks":%I,"output_blocks":%O}',
                        "-o", str(metrics), *command], text=True, capture_output=True)
                    wall = time.perf_counter() - start
                    if process.returncode:
                        raise RuntimeError(f"{name}: {process.stderr}")
                    expected_size = rows * 256 if query == "copy" else len(aggregate)
                    expected_hash = csv_hash if query == "copy" else hashlib.sha256(aggregate).hexdigest()
                    if output.stat().st_size != expected_size or digest(output) != expected_hash:
                        raise RuntimeError(f"{name}: incorrect output")
                    if any(stages.iterdir()):
                        raise RuntimeError(f"{name}: temporary files remain")
                    events = [json.loads(line) for line in process.stderr.splitlines() if line.startswith("{")]
                    staged = next(e for e in events if e.get("event") == "output_staged")
                    run = {"case": name, "phase": phase, "repetition": repetition + 1, "wall_s": wall,
                        **json.loads(metrics.read_text()), "output_verified": True,
                        "spill_count": staged["spill_count"], "spilled_bytes": staged["spilled_bytes"]}
                    results["runs"].append(run)
                    args.result.write_text(json.dumps(results, indent=2) + "\n")
                    print(f"{phase} {repetition+1}/{count} {name}: {wall:.3f}s", flush=True)
        for name in cases:
            samples = [r for r in results["runs"] if r["case"] == name and r["phase"] == "measured"]
            median = statistics.median(r["wall_s"] for r in samples)
            results["summary"][name] = {"median_s": median, "min_s": min(r["wall_s"] for r in samples),
                "max_s": max(r["wall_s"] for r in samples), "csv_equivalent_mib_s": rows * 256 / 2**20 / median,
                "median_cpu_s": statistics.median(r["user_s"]+r["system_s"] for r in samples),
                "median_rss_mib": statistics.median(r["max_rss_kib"] for r in samples)/1024}
        for name, path in binaries.items():
            assert digest(path) == results["environment"]["binaries"][name]["sha256"], "binary changed during measurement"
        args.result.write_text(json.dumps(results, indent=2) + "\n")
        print(json.dumps(results["summary"], indent=2), flush=True)


if __name__ == "__main__":
    main()
