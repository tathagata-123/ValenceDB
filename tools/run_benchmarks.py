#!/usr/bin/env python3
"""Offline ValenceDB benchmark sweeps; JSONL is authoritative, summaries use successes only."""
import argparse
import json
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import tempfile


def cases(suite):
    full = suite == "full"
    result = []

    def add(name, family, group, **options):
        result.append(dict(case_id=name, family=family, equivalence_group=group, options=options))

    for ring in ("control", "off", "on"):
        add("clock_" + ring, "clock", "clock_control" if ring == "control" else "clock_scan",
            experiment="clock_working_set" if ring == "control" else "clock_scan_pollution",
            rows=20000 if full else 4096, payload=256, frames=32 if full else 16,
            hot_pages=4, scan_batch=128 if full else 64, ring="off" if ring == "control" else ring,
            control=ring == "control", distribution="unique")

    for distribution in ("unique", "duplicate"):
        n = 20000 if full else 1200
        for scan in ("seq", "index"):
            add(f"point_{distribution}_{scan}", "btree_point", f"point_{distribution}",
                experiment="btree_point", rows=n, domain=3, payload=128, distribution=distribution,
                scan=scan, lookups=8, frames=16)
        for span in ("short", "medium", "long"):
            # All range sizes on unique data; duplicate equality and broad range.
            if distribution == "duplicate" and span != "long":
                continue
            for scan in ("seq", "index"):
                add(f"range_{distribution}_{span}_{scan}", "btree_range", f"range_{distribution}_{span}",
                    experiment="btree_range", rows=n, domain=3, payload=128, distribution=distribution,
                    scan=scan, range=span, frames=16)
    add("btree_insert", "btree_insert", "insert", experiment="btree_insert",
        rows=20000 if full else 1200, payload=128, distribution="unique", frames=16)

    for memory in ([131072, 262144, 524288] if full else [24576, 32768, 65536]):
        add(f"sort_m_{memory}", "sort", "sort", experiment="sort_vary_m",
            rows=12000 if full else 480, payload=128, distribution="unique", frames=16,
            memory=memory, ring="on")
    for frames in (8, 16, 64):
        add(f"sort_f_{frames}", "sort", "sort", experiment="sort_vary_f",
            rows=12000 if full else 480, payload=128, distribution="unique", frames=frames,
            memory=131072 if full else 32768, ring="on")

    shapes = [
        ("balanced", 1000 if full else 120, 1200 if full else 100, "uniform", 400 if full else 40),
        ("size_ratio", 80 if full else 32, 5000 if full else 480, "uniform", 160 if full else 40),
        ("skew", 400 if full else 100, 500 if full else 120, "skewed", 100 if full else 30),
        ("all_equal", 300 if full else 80, 320 if full else 90, "all_equal", 1),
        ("duplicate", 300 if full else 90, 400 if full else 110, "duplicate", 10 if full else 3),
    ]
    for shape, left, right, distribution, domain in shapes:
        for algorithm, outer in (("nlj", "left"), ("bnlj", "left"), ("bnlj", "right"), ("ghj", "auto")):
            add(f"join_{shape}_{algorithm}_{outer}", "join", "join_" + shape,
                experiment="join_" + shape, rows=left, right_rows=right, payload=32,
                distribution=distribution, domain=domain, join=algorithm, outer=outer,
                memory=24576 if shape == "all_equal" else (131072 if full else 32768), ring="off")

    shapes = [
        ("few", 6000 if full else 720, 8 if full else 4, 32, "duplicate", "mixed", False),
        ("high", 6000 if full else 360, 6000 if full else 300, 32, "duplicate", "positive", False),
        ("spill", 12000 if full else 720, 5000 if full else 350, 32, "duplicate", "mixed", False),
        ("uneven_avg", 5009 if full else 359, 5000 if full else 350, 32, "uneven", "mixed", False),
        ("count_star", 12000 if full else 720, 3000 if full else 150, 0, "duplicate", "mixed", True),
        ("wide", 6000 if full else 360, 2000 if full else 180, 256 if full else 128, "duplicate", "positive", False),
        ("spill_large_m", 12000 if full else 720, 5000 if full else 350, 32, "duplicate", "mixed", False),
    ]
    for shape, rows, groups, payload, distribution, measures, count_star in shapes:
        for algorithm, scan in (("hash", "seq"), ("sort", "seq"), ("sort", "index")):
            add(f"aggregate_{shape}_{algorithm}_{scan}", "aggregate",
                "aggregate_spill" if shape == "spill_large_m" else "aggregate_" + shape,
                experiment="aggregate_order_reuse" if scan == "index" else "aggregate_group_cardinality",
                rows=rows, groups=groups, payload=payload, distribution=distribution, measures=measures,
                aggregate=algorithm, scan=scan, count_star=count_star, ring="auto",
                memory=(1048576 if full else 131072) if shape == "spill_large_m" else (131072 if full else 32768))
    return result


def validate_plan(plan):
    required = {"operator", "ordered", "estimated_rows", "estimated_local_pages",
                "estimated_subtree_pages", "estimated_local_cpu", "estimated_subtree_cpu",
                "estimated_score", "children"}
    if required - plan.keys():
        raise ValueError("missing physical plan fields: " + str(required - plan.keys()))
    if plan["operator"] in ("BNLJ", "GHJ", "GlobalAggregate", "HashAggregate", "SortAggregate", "ExternalSort"):
        if plan.get("memory_bytes", 0) <= 0:
            raise ValueError("missing resolved operator budget")
    if plan["operator"] == "SortAggregate":
        if not isinstance(plan.get("internal_sort"), bool) or len(plan["children"]) != 1:
            raise ValueError("invalid SortAggregate input/property")
        if plan["children"][0]["operator"] == "ExternalSort":
            raise ValueError("unexpected synthetic aggregate sort child")
    for child in plan["children"]:
        validate_plan(child)


def validate(record):
    required = {"schema_version", "config", "success", "build_type", "compiler", "platform",
                "failure_stage", "failure_reason"}
    if required - record.keys():
        raise ValueError("missing record fields: " + str(required - record.keys()))
    if record["schema_version"] != 6:
        raise ValueError("unsupported schema")
    if not record["success"]:
        return
    required = {"elapsed_ms", "dataset", "result_rows", "diagnostic_digest", "ordered_digest",
                "buffer", "file_io", "scratch", "algorithm", "physical_plans", "explain",
                "reference_validation", "timing_boundary", "buffer_state", "finish_query_succeeded"}
    if required - record.keys():
        raise ValueError("missing successful fields: " + str(required - record.keys()))
    if record["build_type"] != "Release":
        raise ValueError("performance suite requires a Release executable")
    if record["elapsed_ms"] < 0 or record["diagnostic_digest"]["count"] != record["result_rows"]:
        raise ValueError("invalid time/count")
    for key in ("xor", "sum", "mixed_sum"):
        value = record["diagnostic_digest"][key]
        if len(value) != 16:
            raise ValueError("invalid diagnostic hash")
        int(value, 16)
    b, f, s, a = (record[k] for k in ("buffer", "file_io", "scratch", "algorithm"))
    if b["buffer_hits"] + b["buffer_misses"] != b["buffer_fetches"] or b["active_pins"]:
        raise ValueError("buffer counter/pin mismatch")
    if any(s[k] for k in ("extent_after_reset", "temporary_pins_after", "temporary_cached_pages_after")):
        raise ValueError("scratch cleanup failed")
    if s["allocated_temp_pages"] != f["temporary_allocations"]:
        raise ValueError("scratch/file allocation mismatch")
    c = record["config"]
    for plan in record["physical_plans"]:
        validate_plan(plan)
    if c["family"] not in ("clock", "btree_insert") and record["finish_query_succeeded"] is not True:
        raise ValueError("query did not finish")
    if c["reference"] and not record["reference_validation"].startswith(("exact canonical", "every fetched")):
        raise ValueError("smoke reference missing")
    if c["family"] == "sort":
        if a["initial_runs"] <= a["k"] and a["intermediate_merge_passes"]:
            raise ValueError("inconsistent merge passes")
        if a["initial_runs"] > 1 and not a["final_merge_streamed"]:
            raise ValueError("external final merge did not stream")
    if c["family"] == "join" and c["join"] == "ghj" and c["distribution"] == "all_equal":
        if a["fallback_pairs"] < 1 or record["result_rows"] != c["rows"] * c["right_rows"]:
            raise ValueError("all-equal fallback fixture failed")
    if c["family"] == "aggregate":
        if c["aggregate"] == "sort" and c["scan"] == "index":
            if not a["input_order_reused"] or a["used_internal_sort"] or a["sort"]["initial_runs"]:
                raise ValueError("index order was not reused")
        if c["aggregate"] == "sort" and c["scan"] == "seq" and not a["used_internal_sort"]:
            raise ValueError("unordered grouping did not sort")
        if a["spill_batches"] == 1:
            raise ValueError("spill final flush count mismatch")


def check_groups(records, selected):
    groups, ordered_groups = {}, {}
    for r in records:
        if not r["success"]:
            continue
        key = r["equivalence_group"]
        digest = r["diagnostic_digest"]
        if key in groups and groups[key] != digest:
            raise ValueError("logical result mismatch in " + key)
        groups[key] = digest
        c = r["config"]
        if c["family"] == "sort" or (c["family"] == "aggregate" and c["aggregate"] == "sort"):
            if key in ordered_groups and ordered_groups[key] != r["ordered_digest"]:
                raise ValueError("ordered result mismatch in " + key)
            ordered_groups[key] = r["ordered_digest"]
    if len(records) != len(selected):
        raise ValueError("unexpected record count")
    families = {r["config"]["family"] for r in records}
    if "sort" in families:
        sorts = [r for r in records if r["success"] and r["config"]["family"] == "sort"]
        if not any(r["algorithm"]["intermediate_merge_passes"] > 0 for r in sorts):
            raise ValueError("suite did not exercise multi-pass sort")
        capacities = {(r["algorithm"]["C_sort"], r["algorithm"]["k"]) for r in sorts
                      if r["config"]["experiment"] == "sort_vary_f"}
        if len(capacities) != 1:
            raise ValueError("F sweep changed M-derived sort capacities")
    if "aggregate" in families:
        hashes = {r["case_id"]: r for r in records if r["success"] and r["config"]["family"] == "aggregate"
                  and r["config"]["aggregate"] == "hash"}
        if hashes["aggregate_few_hash_seq"]["algorithm"]["spill_batches"] != 0:
            raise ValueError("few-groups fixture spilled")
        if hashes["aggregate_spill_hash_seq"]["algorithm"]["spill_batches"] < 3:
            raise ValueError("repeated-spill fixture did not spill repeatedly")
        if hashes["aggregate_uneven_avg_hash_seq"]["algorithm"]["spill_batches"] < 2:
            raise ValueError("uneven AVG fixture did not spill")


def summarize(records):
    grouped = {}
    for r in records:
        if r["success"]:
            grouped.setdefault(r["case_id"], []).append(r)
    summary = []
    for name, rows in grouped.items():
        times = [r["elapsed_ms"] for r in rows]
        r = rows[0]
        summary.append(dict(case_id=name, repetitions=len(rows), median_ms=statistics.median(times),
                            min_ms=min(times), max_ms=max(times), config=r["config"],
                            result_rows=r["result_rows"], diagnostic_digest=r["diagnostic_digest"],
                            representative_buffer=r["buffer"], representative_file_io=r["file_io"],
                            representative_scratch=r["scratch"], representative_algorithm=r["algorithm"]))
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", choices=("smoke", "full"), default="smoke")
    parser.add_argument("--build-dir", type=Path, default=Path("build-release"))
    parser.add_argument("--output", type=Path, default=Path("benchmark_results/smoke.jsonl"))
    parser.add_argument("--seed", type=int, default=130013)
    parser.add_argument("--repeats", type=int, help="default: smoke=1, full=3")
    parser.add_argument("--family", action="append", choices=("clock", "btree_point", "btree_range", "btree_insert", "sort", "join", "aggregate"))
    parser.add_argument("--timeout", type=float, default=300, help="seconds per process, includes setup/reference")
    parser.add_argument("--list-only", action="store_true")
    args = parser.parse_args()
    repeats = args.repeats if args.repeats is not None else (3 if args.suite == "full" else 1)
    if repeats < 1 or args.seed < 0 or args.timeout <= 0:
        parser.error("positive repeats/timeout and nonnegative seed required")
    selected = [c for c in cases(args.suite) if not args.family or c["family"] in args.family]
    if args.list_only:
        print(json.dumps(selected, indent=2))
        return 0
    executable = (args.build_dir / "valencedb_bench").resolve()
    if not executable.is_file():
        parser.error("build valencedb_bench in Release first: " + str(executable))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    records, expected = [], []
    stderr_path = args.output.with_suffix(".stderr.log")
    with args.output.open("w") as raw, stderr_path.open("w") as errors, tempfile.TemporaryDirectory(prefix="valencedb_suite_") as tmp:
        for trial in range(1, repeats + 1):
            for case in selected:
                options = dict(case["options"], family=case["family"], seed=args.seed, trial=trial)
                if args.suite == "smoke":
                    options["reference"] = True
                command = [str(executable)]
                for key, value in options.items():
                    flag = "--" + key.replace("_", "-")
                    if isinstance(value, bool):
                        if value:
                            command.append(flag)
                    else:
                        command.extend([flag, str(value)])
                expected.append(case)
                work = Path(tmp) / str(len(expected))
                failure = None
                try:
                    run = subprocess.run(command + ["--work-dir", str(work)], text=True, capture_output=True,
                                         timeout=args.timeout, check=False)
                    errors.write(f"{case['case_id']} trial={trial} exit={run.returncode}\n{run.stderr}")
                    lines = run.stdout.splitlines()
                    if len(lines) != 1:
                        raise ValueError("expected one JSON record: " + run.stdout[:500])
                    record = json.loads(lines[0])
                    validate(record)
                    if run.returncode != (0 if record["success"] else 1):
                        raise ValueError("exit status disagrees with record")
                except (subprocess.TimeoutExpired, ValueError, OSError, KeyError, TypeError) as error:
                    failure = str(error)
                    record = dict(schema_version=6, config=options, success=False,
                                  failure_stage="runner_validation", failure_reason=failure)
                record.update(suite=args.suite, case_id=case["case_id"], equivalence_group=case["equivalence_group"],
                              command=command, runner_platform=platform.platform(), runner_architecture=platform.machine())
                records.append(record)
                raw.write(json.dumps(record, sort_keys=True) + "\n")
                raw.flush()
                state = f"{record['elapsed_ms']:.3f} ms" if record["success"] else "FAILED: " + record["failure_reason"]
                print(f"[{len(records)}/{len(selected) * repeats}] {case['case_id']} trial={trial}: {state}", flush=True)
    failed = [r for r in records if not r["success"]]
    if not args.family:
        wanted = {"clock", "btree_point", "btree_range", "btree_insert", "sort", "join", "aggregate"}
        if {r["config"]["family"] for r in records} != wanted:
            print("SUITE VALIDATION FAILED: required family missing", file=sys.stderr)
            return 1
    if not failed:
        try:
            check_groups(records, expected)
            # Parse the saved artifact too; do not trust only in-memory objects.
            parsed = [json.loads(line) for line in args.output.read_text().splitlines()]
            for record in parsed:
                validate(record)
            if len(parsed) != len(expected):
                raise ValueError("saved result count mismatch")
        except (ValueError, KeyError) as error:
            print("SUITE VALIDATION FAILED:", error, file=sys.stderr)
            return 1
    summary = summarize(records)
    args.output.with_suffix(".summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print("\ncase | repeats | median ms | min..max ms | misses | db reads | temp pages")
    for row in summary:
        print(f"{row['case_id']} | {row['repetitions']} | {row['median_ms']:.3f} | "
              f"{row['min_ms']:.3f}..{row['max_ms']:.3f} | {row['representative_buffer']['buffer_misses']} | "
              f"{row['representative_file_io']['database_reads']} | {row['representative_scratch']['allocated_temp_pages']}")
    print(f"{len(records)} runs: {len(records) - len(failed)} succeeded, {len(failed)} failed; raw: {args.output}")
    for row in failed:
        print(f"FAILED {row['case_id']}: {row['failure_stage']}: {row['failure_reason']}", file=sys.stderr)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
