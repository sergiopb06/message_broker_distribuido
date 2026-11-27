#!/usr/bin/env python3
"""
Analysis of the message broker benchmarks (bench/run.sh).

usage: analyze.py <results1.csv> [<results2.csv> ...]
       analyze.py all            # every bench/results/bench_*.csv available

It takes the median of the repetitions of each point and reports, per
environment:

  brokers, partitions, consumers   (speedup by throughput)
    X(p)       throughput: produced msg/s, or consumed msg/s for consumers
    S(p)       X(p) / X(p0), p0 the smallest p measured (normally 1)
    E(p)       S(p) / (p / p0)
    e(p)       Karp-Flatt experimental serial fraction,
               e = (1/S - 1/q) / (1 - 1/q) with q = p / p0
    f          parallel fraction that best fits Amdahl, S(q) = 1 / ((1 - f) + f / q),
               least squares over the measured points; 1/(1 - f) bounds the speedup
  hot          throughput relative to 0 % of messages with the hot key
  replication  throughput and latency per R and acks, relative to R=1 acks=0

Latency (median and p99 of the produce round trip) is reported for every point.
A summary is written next to each input as <name>_summary.csv; bench/plot.py
draws the charts from the same data.
"""
import csv
import glob
import math
import os
import statistics
import sys
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SPEEDUP_SCENARIOS = ["brokers", "partitions", "consumers"]
ACKS_ORDER = {"0": 0, "1": 1, "ALL": 2}

TITLES = {
    "brokers": "N brokers, N partitions (R=1, acks=1)",
    "partitions": "3 brokers, P partitions (R=1, acks=1)",
    "consumers": "consumer group of C members",
    "hot": "share of messages with the same key (3 brokers, 3 partitions)",
    "replication": "replication cost (3 brokers, 1 partition)",
}


def read_csv(path):
    """Rows of one results file and the metadata of its comment header."""
    meta = {}
    lines = []
    with open(path, newline="") as fh:
        for line in fh:
            if line.startswith("#"):
                text = line[1:].strip()
                if ":" in text:
                    key, _, value = text.partition(":")
                    meta[key.strip()] = value.strip()
            else:
                lines.append(line)
    return list(csv.DictReader(lines)), meta


def param_key(scenario, param):
    if scenario == "replication":
        r, _, acks = param.partition("-")
        return (int(r[1:]), ACKS_ORDER.get(acks, 9))
    return float(param)


def summarize(rows):
    """(env, nodes, scenario) -> list of points (medians), ordered by param."""
    groups = defaultdict(list)
    for row in rows:
        groups[(row["env"], int(row["nodes"]), row["scenario"], row["param"])].append(row)

    points = defaultdict(list)
    for (env, nodes, scenario, param), runs in groups.items():
        def med(field):
            return statistics.median(float(r[field]) for r in runs)
        throughput_field = "consume_msg_s" if scenario == "consumers" else "msg_s"
        points[(env, nodes, scenario)].append({
            "param": param,
            "runs": len(runs),
            "replication": runs[0]["replication"],
            "acks": runs[0]["acks"],
            "throughput": med(throughput_field),
            "mb_s": med("mb_s"),
            "p50": med("latency_median_ms"),
            "p99": med("latency_p99_ms"),
        })
    for key in points:
        points[key].sort(key=lambda pt: param_key(key[2], pt["param"]))
        add_derived(key[2], points[key])
    return points


def amdahl_fit(qs, speedups):
    """Parallel fraction f in [0, 1] that minimizes the squared error of Amdahl."""
    best_f, best_err = 0.0, math.inf
    for i in range(10001):
        f = i / 10000
        err = sum((s - 1.0 / ((1 - f) + f / q)) ** 2 for q, s in zip(qs, speedups))
        if err < best_err:
            best_f, best_err = f, err
    return best_f


def add_derived(scenario, pts):
    if scenario in SPEEDUP_SCENARIOS:
        base = pts[0]
        p0 = float(base["param"])
        for pt in pts:
            q = float(pt["param"]) / p0
            s = pt["throughput"] / base["throughput"] if base["throughput"] > 0 else math.nan
            pt["q"] = q
            pt["speedup"] = s
            pt["efficiency"] = s / q
            pt["karp_flatt"] = ((1 / s - 1 / q) / (1 - 1 / q)) if q > 1 and s > 0 else math.nan
        f = amdahl_fit([pt["q"] for pt in pts], [pt["speedup"] for pt in pts])
        for pt in pts:
            pt["amdahl_f"] = f
            pt["amdahl_s"] = 1.0 / ((1 - f) + f / pt["q"])
    else:
        if scenario == "replication":
            base = next((pt for pt in pts if pt["param"] == "R1-0"), pts[0])
        else:
            base = pts[0]
        for pt in pts:
            pt["relative"] = pt["throughput"] / base["throughput"] if base["throughput"] > 0 else math.nan
            pt["latency_factor"] = pt["p50"] / base["p50"] if base["p50"] > 0 else math.nan


def print_points(env, nodes, scenario, pts):
    print(f"\n── {scenario} · {TITLES.get(scenario, '')} · {env} ({nodes} node(s))")
    if scenario in SPEEDUP_SCENARIOS:
        print(f"  {'p':>3} {'runs':>4} {'msg/s':>10} {'S(p)':>6} {'E(p)':>6} {'e(p)':>6} "
              f"{'Amdahl':>7} {'p50 ms':>8} {'p99 ms':>8}")
        for pt in pts:
            kf = "" if math.isnan(pt["karp_flatt"]) else f"{pt['karp_flatt']:.3f}"
            print(f"  {pt['param']:>3} {pt['runs']:>4} {pt['throughput']:>10.1f} {pt['speedup']:>6.2f} "
                  f"{pt['efficiency']:>6.2f} {kf:>6} {pt['amdahl_s']:>7.2f} {pt['p50']:>8.3f} {pt['p99']:>8.3f}")
        f = pts[0]["amdahl_f"]
        limit = "∞" if f >= 1 else f"{1 / (1 - f):.1f}"
        print(f"  Amdahl: parallel fraction f = {f:.3f}, serial 1-f = {1 - f:.3f}, speedup bound {limit}")
    else:
        label = "param" if scenario != "hot" else "hot %"
        print(f"  {label:>7} {'runs':>4} {'msg/s':>10} {'MB/s':>8} {'relative':>8} {'p50 ms':>8} "
              f"{'p99 ms':>8} {'p50 x':>6}")
        for pt in pts:
            print(f"  {pt['param']:>7} {pt['runs']:>4} {pt['throughput']:>10.1f} {pt['mb_s']:>8.3f} "
                  f"{pt['relative']:>8.2f} {pt['p50']:>8.3f} {pt['p99']:>8.3f} {pt['latency_factor']:>6.2f}")


def write_summary(points, path):
    fields = ["env", "nodes", "scenario", "param", "runs", "replication", "acks", "throughput", "mb_s",
              "p50", "p99", "speedup", "efficiency", "karp_flatt", "amdahl_f", "relative", "latency_factor"]
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for (env, nodes, scenario), pts in sorted(points.items()):
            for pt in pts:
                writer.writerow({"env": env, "nodes": nodes, "scenario": scenario, **pt})


def result_files(args):
    if not args or args == ["all"]:
        return sorted(p for p in glob.glob(os.path.join(ROOT, "bench", "results", "bench_*.csv"))
                      if not p.endswith("_summary.csv"))
    return args


def load_points(paths):
    rows = []
    for path in paths:
        file_rows, _ = read_csv(path)
        rows.extend(file_rows)
    return summarize(rows)


def main(argv):
    paths = result_files(argv[1:])
    if not paths:
        print("no results: run bench/run.sh first", file=sys.stderr)
        return 1
    for path in paths:
        rows, meta = read_csv(path)
        if not rows:
            continue
        write_summary(summarize(rows), path[:-4] + "_summary.csv")
        print(f"\n═══ {os.path.basename(path)} · {meta.get('env', '?').split('|')[0].strip()} · cpu {meta.get('cpu', '?')}")
    points = load_points(paths)
    for (env, nodes, scenario), pts in sorted(points.items()):
        print_points(env, nodes, scenario, pts)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
