#!/usr/bin/env python3
"""Compare CSV conversion binaries with exact output checks and durable CLI writes."""
import argparse
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


def cell(value):
    if value is None:
        return ""
    if not value or any(c in value for c in ',"\r\n'):
        return '"' + value.replace('"', '""') + '"'
    return value


def fixture(layout):
    fields = ([{"name": "payload"}] if layout == "one_field" else
              [{"name": f"text_{j}", "nullable": True} for j in range(8)] +
              [{"name": f"amount_{j}", "type": "decimal", "precision": 16, "scale": 2} for j in range(8)])
    records = []
    for i in range(4096):
        if layout == "one_field":
            values = [f"T{i:0254}"]
        else:
            values = [f"T{j:02}{i:012}" for j in range(8)]
            if layout == "quoted_multiline":
                values = [None, "", f'日本,{i:06}', f'a"b{i:06}',
                          f"line1\nline2{i:06}", f"a\r\nb{i:06}", values[6], values[7]]
            values += [f"{100000000000+i*8+j}.{i%100:02}" for j in range(8)]
        records.append((",".join(cell(v) for v in values) + "\n").encode())
    return fields, records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--size-mib", type=int, default=256)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--threads", type=int, nargs="+", default=[1, 4])
    parser.add_argument("--compare-dxt", type=Path, required=True)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--result", type=Path, default=ROOT / "benchmarks/results/csv-comparison.json")
    args = parser.parse_args()
    if args.size_mib < 1 or args.repeats < 1 or args.warmups < 0:
        parser.error("size/repeats must be positive and warmups nonnegative")
    if any(n < 1 for n in args.threads) or len(set(args.threads)) != len(args.threads):
        parser.error("threads must be distinct positive integers")
    if not args.skip_build:
        subprocess.run(["cargo", "build", "--release", "--locked", "--offline", "--bin", "dxt", "-j3"], cwd=ROOT, check=True)
    binaries = {"before": args.compare_dxt.resolve(), "after": ROOT / "target/release/dxt"}
    for path in binaries.values():
        if not path.is_file():
            parser.error(f"missing binary: {path}")
    args.result = args.result.resolve()
    args.result.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="dxtran-csv-bench-", dir=args.work_dir) as folder:
        work = Path(folder)
        stages = work / "stages"
        stages.mkdir()
        output = work / "output.csv"
        cases, inputs, layouts, plans = {}, {}, {}, {}
        for layout in ["one_field", "sixteen_fields", "quoted_multiline"]:
            fields, records = fixture(layout)
            block = b"".join(records)
            path = work / f"{layout}.csv"
            remaining = args.size_mib * 1024 * 1024
            rows = 0
            with path.open("wb") as f:
                while remaining >= len(block):
                    f.write(block)
                    remaining -= len(block)
                    rows += len(records)
                for record in records:
                    if remaining < len(record):
                        break
                    f.write(record)
                    remaining -= len(record)
                    rows += 1
                f.flush()
                os.fsync(f.fileno())
            inputs[layout] = path
            layouts[layout] = {"size_bytes": path.stat().st_size, "rows": rows, "sha256": digest(path)}
            common = {"format": "csv", "encoding": "utf-8", "csv": {"header": False}}
            job = {"input": [{**common, "id": "data", "path": str(path), "fields": fields}],
                   "output": [{**common, "id": "result", "path": str(output), "sql": "SELECT * FROM data"}]}
            job_path = work / f"{layout}.json"
            job_path.write_text(json.dumps(job), encoding="utf-8")
            plans[layout] = {}
            for version, binary in binaries.items():
                plans[layout][version] = subprocess.check_output([str(binary), "explain", str(job_path)], text=True)
                for threads in args.threads:
                    cases[f"{version}_{layout}_threads_{threads}"] = (layout, [str(binary), "run", str(job_path),
                        "--threads", str(threads), "--memory-limit", "512MiB", "--temp-dir", str(stages)])
        results = {"environment": {"platform": platform.platform(), "cpu_count": os.cpu_count(),
                       "affinity": sorted(os.sched_getaffinity(0)),
                       "cpu_model": next(line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")),
                       "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
                       "binaries": {name: {"path": str(path), "sha256": digest(path)} for name, path in binaries.items()},
                       "filesystem": subprocess.check_output(["stat", "-f", "-c", "%T", str(work)], text=True).strip()},
                   "settings": {"date_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                       "size_mib": args.size_mib, "repeats": args.repeats, "warmups": args.warmups,
                       "threads": args.threads, "cache": "warm", "fsync": True, "build_profile": "release", "order_seed": 20261026},
                   "layouts": layouts, "plans": plans, "runs": [], "summary": {}}
        rng = random.Random(20261026)
        for phase, count in [("warmup", args.warmups), ("measured", args.repeats)]:
            for repetition in range(count):
                order = list(cases)
                rng.shuffle(order)
                for name in order:
                    layout, command = cases[name]
                    output.unlink(missing_ok=True)
                    prepare_cache(inputs[layout], "warm")
                    metrics_path = work / "time.json"
                    start = time.perf_counter()
                    process = subprocess.run(["/usr/bin/time", "-f", '{"user_s":%U,"system_s":%S,"max_rss_kib":%M,"input_blocks":%I,"output_blocks":%O}',
                        "-o", str(metrics_path), *command], text=True, capture_output=True)
                    elapsed = time.perf_counter() - start
                    if process.returncode:
                        raise RuntimeError(f"{name}: {process.stderr}")
                    if output.stat().st_size != layouts[layout]["size_bytes"] or digest(output) != layouts[layout]["sha256"]:
                        raise RuntimeError(f"{name}: output differs from input")
                    events = [json.loads(line) for line in process.stderr.splitlines() if line.startswith("{")]
                    staged = next((event for event in events if event.get("event") == "output_staged"), {})
                    run = {"case": name, "phase": phase, "repetition": repetition + 1, "wall_s": elapsed,
                           **json.loads(metrics_path.read_text()), "output_verified": True,
                           "spill_count": staged.get("spill_count", 0), "spilled_bytes": staged.get("spilled_bytes", 0)}
                    results["runs"].append(run)
                    args.result.write_text(json.dumps(results, indent=2) + "\n")
                    print(f"{phase} {repetition+1}/{count} {name}: {elapsed:.3f}s", flush=True)
        for name, (layout, _) in cases.items():
            samples = [run for run in results["runs"] if run["case"] == name and run["phase"] == "measured"]
            median = statistics.median(run["wall_s"] for run in samples)
            results["summary"][name] = {"median_s": median, "min_s": min(run["wall_s"] for run in samples),
                "max_s": max(run["wall_s"] for run in samples), "mib_per_s": layouts[layout]["size_bytes"] / (1024*1024) / median,
                "median_cpu_s": statistics.median(run["user_s"]+run["system_s"] for run in samples),
                "median_rss_mib": statistics.median(run["max_rss_kib"] for run in samples)/1024}
        args.result.write_text(json.dumps(results, indent=2) + "\n")
        print(json.dumps(results["summary"], indent=2), flush=True)
        print(f"Results: {args.result}", flush=True)


if __name__ == "__main__":
    main()
