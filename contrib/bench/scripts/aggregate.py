#!/usr/bin/env python3
"""Aggregate a benchmark sweep (results/) into Markdown tables.

Reads results/<profile>/<impl>/<request>-c<C>-r<rep>.json (ghz output) plus the
sidecar .res (proxy CPU), peak-rss.txt, footprint.txt, and metrics-final.txt
(server-side grpc_server_handling_seconds). Emits, per profile: GetBlock latency
across the concurrency curve, GetBlockRange throughput (blocks/s), and resources.

Stdlib only. Usage: aggregate.py [results_dir] [profile ...]
"""

import json
import os
import re
import statistics
import sys

RESULTS = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "results")
ONLY = set(sys.argv[2:])
IMPLS = ["rust", "go"]
FILE_RE = re.compile(r"^(?P<req>getblock|range\d+)-c(?P<c>\d+)-r(?P<rep>\d+)\.json$")


def pct(dist, p):
    """Latency (ns) at percentile p from ghz's latencyDistribution, or None."""
    for entry in dist or []:
        if entry.get("percentage") == p:
            return entry.get("latency")
    return None


def load_runs(impl_dir):
    """Return {(req, concurrency): [run dicts]} for one implementation."""
    runs = {}
    if not os.path.isdir(impl_dir):
        return runs
    for name in os.listdir(impl_dir):
        m = FILE_RE.match(name)
        if not m:
            continue
        with open(os.path.join(impl_dir, name)) as fh:
            report = json.load(fh)
        res_path = os.path.join(impl_dir, name[:-5] + ".res")
        cpu = None
        if os.path.exists(res_path):
            with open(res_path) as fh:
                cpu = json.load(fh).get("proxy_cpu_cores")
        req, c = m["req"], int(m["c"])
        runs.setdefault((req, c), []).append({
            "rps": report.get("rps", 0.0),
            "p50": pct(report.get("latencyDistribution"), 50),
            "p90": pct(report.get("latencyDistribution"), 90),
            "p99": pct(report.get("latencyDistribution"), 99),
            "count": report.get("count", 0),
            "ok": report.get("statusCodeDistribution", {}).get("OK", 0),
            "cpu": cpu,
        })
    return runs


def med(values):
    vals = [v for v in values if v is not None]
    return statistics.median(vals) if vals else None


def spread(values):
    vals = [v for v in values if v is not None]
    return statistics.pstdev(vals) if len(vals) > 1 else 0.0


def us(ns):
    return f"{ns / 1000:.0f}" if ns is not None else "—"


def read_int(path):
    try:
        with open(path) as fh:
            return int(fh.read().strip())
    except (OSError, ValueError):
        return None


def mib(nbytes):
    return f"{nbytes / 1048576:.1f}" if nbytes is not None else "—"


def concurrencies(per_impl):
    cs = set()
    for runs in per_impl.values():
        cs.update(c for (_req, c) in runs)
    return sorted(cs)


def requests(per_impl):
    reqs = set()
    for runs in per_impl.values():
        reqs.update(req for (req, _c) in runs)
    return reqs


def emit_profile(profile, out):
    profile_dir = os.path.join(RESULTS, profile)
    per_impl = {impl: load_runs(os.path.join(profile_dir, impl)) for impl in IMPLS}
    per_impl = {impl: runs for impl, runs in per_impl.items() if runs}
    if not per_impl:
        return
    impls = [impl for impl in IMPLS if impl in per_impl]
    cs = concurrencies(per_impl)
    reqs = requests(per_impl)

    out.append(f"## {profile}\n")

    if "getblock" in reqs:
        out.append("### GetBlock latency — median p50 / p99 across reps (µs)\n")
        head = "| concurrency | " + " | ".join(f"{i} p50 | {i} p99" for i in impls) + " |"
        out.append(head)
        out.append("|" + "---|" * (1 + 2 * len(impls)))
        for c in cs:
            cells = [str(c)]
            for impl in impls:
                runs = per_impl[impl].get(("getblock", c), [])
                cells += [us(med([r["p50"] for r in runs])), us(med([r["p99"] for r in runs]))]
            out.append("| " + " | ".join(cells) + " |")
        out.append("")

    range_reqs = sorted((r for r in reqs if r.startswith("range")),
                        key=lambda r: int(r[5:]))
    for req in range_reqs:
        width = int(req[5:])
        out.append(f"### GetBlockRange W={width} throughput — median blocks/s (±stdev)\n")
        out.append("| concurrency | " + " | ".join(impls) + " |")
        out.append("|" + "---|" * (1 + len(impls)))
        for c in cs:
            cells = [str(c)]
            for impl in impls:
                runs = per_impl[impl].get((req, c), [])
                rps = [r["rps"] for r in runs]
                if rps:
                    cells.append(f"{med(rps) * width:,.0f} ±{spread(rps) * width:,.0f}")
                else:
                    cells.append("—")
            out.append("| " + " | ".join(cells) + " |")
        out.append("")

    out.append("### Resources\n")
    out.append("| impl | peak RSS (MiB) | cache on disk (MiB) | max CPU (cores) |")
    out.append("|---|---|---|---|")
    footprint = {}
    fp_path = os.path.join(profile_dir, "footprint.txt")
    if os.path.exists(fp_path):
        with open(fp_path) as fh:
            for line in fh:
                if "=" in line:
                    k, v = line.strip().split("=", 1)
                    footprint[k] = v
    for impl in impls:
        rss = read_int(os.path.join(profile_dir, impl, "peak-rss.txt"))
        cache = footprint.get(f"{impl}_cache_bytes")
        cache = int(cache) if cache and cache.isdigit() else None
        cpus = [r["cpu"] for runs in per_impl[impl].values() for r in runs if r["cpu"] is not None]
        max_cpu = f"{max(cpus):.2f}" if cpus else "—"
        out.append(f"| {impl} | {mib(rss)} | {mib(cache)} | {max_cpu} |")
    out.append("")


def main():
    if not os.path.isdir(RESULTS):
        sys.exit(f"no results dir: {RESULTS}")
    profiles = sorted(p for p in os.listdir(RESULTS)
                      if os.path.isdir(os.path.join(RESULTS, p)) and (not ONLY or p in ONLY))
    out = ["<!-- generated by aggregate.py -->", ""]
    for profile in profiles:
        emit_profile(profile, out)
    print("\n".join(out))


if __name__ == "__main__":
    main()
