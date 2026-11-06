#!/usr/bin/env python3
"""Compare checks, SQL functions, physical codecs, and process planning."""
import argparse
import copy
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
    parser.add_argument("--rows", type=int, default=65536)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--threads", type=int, nargs="+", default=[1, 4])
    parser.add_argument("--process-counts", type=int, nargs="+", default=[100, 1000])
    parser.add_argument("--profile", choices=["release", "debug"], default="release")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--compare-dxt", type=Path)
    parser.add_argument("--cases", nargs="+", help="run only these case names, including thread/before prefixes")
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--result", type=Path, default=ROOT / "benchmarks/results/review-workloads.json")
    args = parser.parse_args()
    if min(args.rows, args.repeats, *args.threads, *args.process_counts) < 1 or args.warmups < 0:
        parser.error("rows/repeats/threads/process-counts must be positive; warmups nonnegative")
    if len(set(args.threads)) != len(args.threads) or len(set(args.process_counts)) != len(args.process_counts):
        parser.error("duplicate thread or process counts")
    if not args.skip_build:
        command = ["cargo", "build", "--locked", "--offline", "--bin", "dxt", "--example", "parquet_fixture", "-j3"]
        if args.profile == "release":
            command.append("--release")
        subprocess.run(command, cwd=ROOT, check=True)
    binary = ROOT / "target" / args.profile / "dxt"
    binaries = {"": binary}
    if args.compare_dxt:
        args.compare_dxt = args.compare_dxt.resolve()
        binaries["before_"] = args.compare_dxt
    fixture = binary.parent / "examples/parquet_fixture"
    args.result = args.result.resolve()
    args.result.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="dxtran-workloads-", dir=args.work_dir) as folder:
        work = Path(folder)
        stages = work / "stages"
        stages.mkdir()
        output = work / "output.csv"
        cases, layouts = {}, {}

        def add(name, config, source, expected=None, command="run", rows=None):
            job = work / f"{name}.json"
            job.write_text(json.dumps(config))
            for threads in args.threads:
                for prefix, executable in binaries.items():
                    cases[f"{prefix}{name}_threads_{threads}"] = {
                        "command": [str(executable), command, str(job), "--threads", str(threads),
                                    "--memory-limit", "512MiB", "--temp-dir", str(stages)],
                        "source": source, "expected": expected, "rows": rows,
                    }

        for columns in [16, 100]:
            data = work / str(columns)
            data.mkdir()
            metadata = json.loads(subprocess.check_output([str(fixture), str(data), str(args.rows), str(columns)], text=True))
            source = data / "snappy_8192.parquet"
            csv = data / "expected.csv"
            full = (csv.stat().st_size, digest(csv))
            layouts[str(columns)] = {**metadata, "source_sha256": digest(source), "full_csv_bytes": full[0]}
            inp = {"id": "data", "path": str(source), "format": "parquet"}
            dest = {"id": "result", "path": str(output), "format": "csv", "encoding": "utf-8",
                    "csv": {"header": False}, "sql": "SELECT * FROM data"}
            config = {"input": [inp], "output": [dest]}
            if columns == 100:
                add("parquet_100_select_100", config, source, full, rows=args.rows)
                narrow = data / "narrow.csv"
                with csv.open() as reader, narrow.open("w") as writer:
                    for line in reader:
                        writer.write(",".join(line.split(",")[:2]) + "\n")
                projected = copy.deepcopy(config)
                projected["output"][0]["sql"] = "SELECT text_0, text_1 FROM data"
                add("parquet_100_select_2", projected, source, (narrow.stat().st_size, digest(narrow)), rows=args.rows)
            else:
                add("check_none", config, source, full, rows=args.rows)
                for name, sql, expected_line in [
                    ("udf_length", "SELECT icu_length(text_0, 'utf-8') AS n FROM data", "15\n"),
                    ("udf_datetime", "SELECT icu_to_char(DATE '2026-11-06', CASE WHEN amount_0 < 0 THEN 'yyyy-MM' ELSE 'yyyy-MM-dd' END) AS d FROM data", "2026-11-06\n"),
                ]:
                    expected = data / f"{name}.csv"
                    expected.write_text(expected_line * args.rows)
                    transformed = copy.deepcopy(config)
                    transformed["output"][0]["sql"] = sql
                    add(name, transformed, source, (expected.stat().st_size, digest(expected)), rows=args.rows)
                rules = [{"when": when, "level": "error", "message": "fixture check failed"} for when in
                         ["amount_0 < 0", "length(text_0) <> 15", "amount_7 IS NULL"]]
                for location in ["input", "output"]:
                    for count in [1, 3]:
                        checked = copy.deepcopy(config)
                        checked[location][0]["check"] = rules[:count]
                        add(f"check_{location}_{count}", checked, source, full, rows=args.rows)

        for conditional in [False, True]:
            name = "conditional_copy" if conditional else "binary_copy"
            raw = work / f"{name}.dat"
            with raw.open("wb") as writer:
                for row in range(args.rows):
                    writer.write((b"H" if row % 2 == 0 else b"D") + b"x" * 254 + bytes([row % 256]))
            fields = [{"name": "payload", "type": "bstring", "length": 256, "trim": False}]
            if conditional:
                fields = [{"name": "record_type", "length": 1}]
                fields += [{"name": name, "type": "bstring", "offset": 1, "length": 255,
                            "trim": False, "when": f"record_type = '{kind}'"}
                           for name, kind in [("header", "H"), ("detail", "D")]]
            dataset = {"id": "data", "path": str(raw), "format": "fixed", "encoding": "utf-8",
                       "fixed": {"record_separator": "none"}, "fields": fields}
            dest = {**copy.deepcopy(dataset), "id": "result", "path": str(output), "sql": "SELECT * FROM data"}
            add("conditional_copy" if conditional else "binary_copy", {"input": [dataset], "output": [dest]}, raw,
                (raw.stat().st_size, digest(raw)), rows=args.rows)

        seed = work / "seed.csv"
        seed.write_text("n\n1\n")
        for count in args.process_counts:
            config = {"input": [{"id": "seed", "path": str(seed), "format": "csv", "encoding": "utf-8", "fields": [{"name": "n"}]}],
                      "process": [{"id": f"p{i}", "sql": "SELECT * FROM seed"} for i in range(count)],
                      "output": [{"id": "result", "path": str(output), "format": "csv", "encoding": "utf-8", "sql": f"SELECT * FROM p{count-1}"}]}
            add(f"plan_independent_{count}", config, seed, command="check")
            chained = copy.deepcopy(config)
            for i, process in enumerate(chained["process"]):
                process["sql"] = f"SELECT * FROM {'seed' if i == 0 else f'p{i-1}'}"
            add(f"plan_chain_{count}", chained, seed, command="check")

        if args.cases:
            unknown = set(args.cases) - cases.keys()
            if unknown:
                parser.error(f"unknown cases: {sorted(unknown)}")
            cases = {name: cases[name] for name in args.cases}

        results = {
            "environment": {"platform": platform.platform(), "cpu_count": os.cpu_count(),
                            "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
                            "affinity": sorted(os.sched_getaffinity(0)),
                            "binary_sha256": digest(binary), "fixture_sha256": digest(fixture),
                            "comparison_binary_sha256": digest(args.compare_dxt) if args.compare_dxt else None,
                            "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip()},
            "settings": {**vars(args), "work_dir": str(args.work_dir), "result": str(args.result),
                         "compare_dxt": str(args.compare_dxt) if args.compare_dxt else None,
                         "date_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "cache": "warm",
                         "order_seed": 20261106, "parquet_validation": "all columns, including unused columns"},
            "layouts": layouts, "plans": {}, "runs": [], "summary": {},
            "oracles": {name: {"bytes": case["expected"][0], "sha256": case["expected"][1], "rows": case["rows"]}
                        for name, case in cases.items() if case["expected"] is not None},
        }
        for name, case in cases.items():
            if case["expected"] is not None:
                command = case["command"].copy()
                command[1] = "explain"
                results["plans"][name] = subprocess.check_output(command, text=True, stderr=subprocess.DEVNULL)
        rng = random.Random(20261106)
        for phase, count in [("warmup", args.warmups), ("measured", args.repeats)]:
            for repetition in range(count):
                order = list(cases)
                rng.shuffle(order)
                for name in order:
                    case = cases[name]
                    output.unlink(missing_ok=True)
                    prepare_cache(case["source"], "warm")
                    metrics = work / "time.json"
                    start = time.perf_counter()
                    process = subprocess.run(["/usr/bin/time", "-f", '{"user_s":%U,"system_s":%S,"max_rss_kib":%M}',
                                              "-o", str(metrics), *case["command"]], text=True, capture_output=True)
                    wall = time.perf_counter() - start
                    if process.returncode:
                        raise RuntimeError(f"{name}: {process.stderr}")
                    if case["expected"] is not None:
                        if (output.stat().st_size, digest(output)) != case["expected"]:
                            raise RuntimeError(f"{name}: incorrect output")
                    elif output.exists():
                        raise RuntimeError(f"{name}: check created output")
                    if any(stages.iterdir()):
                        raise RuntimeError(f"{name}: temporary files remain")
                    results["runs"].append({"case": name, "phase": phase, "repetition": repetition + 1,
                                            "wall_s": wall, **json.loads(metrics.read_text()), "output_verified": True})
                    args.result.write_text(json.dumps(results, indent=2) + "\n")
                    print(f"{phase} {repetition+1}/{count} {name}: {wall:.3f}s", flush=True)
        for name, case in cases.items():
            samples = [r for r in results["runs"] if r["case"] == name and r["phase"] == "measured"]
            wall = statistics.median(r["wall_s"] for r in samples)
            results["summary"][name] = {"median_s": wall, "min_s": min(r["wall_s"] for r in samples),
                                        "max_s": max(r["wall_s"] for r in samples),
                                        "median_cpu_s": statistics.median(r["user_s"] + r["system_s"] for r in samples),
                                        "median_rss_mib": statistics.median(r["max_rss_kib"] for r in samples) / 1024,
                                        "rows_s": case["rows"] / wall if case["rows"] else None}
        assert digest(binary) == results["environment"]["binary_sha256"], "binary changed during measurement"
        assert digest(fixture) == results["environment"]["fixture_sha256"], "fixture changed during measurement"
        if args.compare_dxt:
            assert digest(args.compare_dxt) == results["environment"]["comparison_binary_sha256"], "comparison binary changed"
        args.result.write_text(json.dumps(results, indent=2) + "\n")
        print(json.dumps(results["summary"], indent=2))


if __name__ == "__main__":
    main()
