#!/usr/bin/env python3
"""Render benchmark result charts as SVG (stdlib only, no plotting deps).

Reads results/<profile>/<impl>/<request>-c<C>-r<rep>.json and writes, per profile,
a GetBlockRange throughput chart and a GetBlock p99-latency chart (Rust vs Go over
the concurrency curve) to contrib/bench/charts/. Colors and axis lines are chosen
to read on both light and dark GitHub themes.

Usage: plot.py [results_dir] [charts_dir]
"""

import json
import math
import os
import re
import statistics
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "..", "results")
CHARTS = sys.argv[2] if len(sys.argv) > 2 else os.path.join(HERE, "..", "charts")
FILE_RE = re.compile(r"^(?P<req>getblock|range\d+)-c(?P<c>\d+)-r\d+\.json$")

RUST = "#e43717"  # rust orange-red
GO = "#00add8"    # go cyan
AXIS = "#888888"  # mid gray: visible on light and dark
THROUGHPUT_W = 1000  # GetBlockRange window charted


def pct99(dist):
    for entry in dist or []:
        if entry.get("percentage") == 99:
            return entry.get("latency")
    return None


def collect(profile, impl):
    """{req: {concurrency: [runs]}} for one implementation."""
    runs = {}
    impl_dir = os.path.join(RESULTS, profile, impl)
    if not os.path.isdir(impl_dir):
        return runs
    for name in os.listdir(impl_dir):
        m = FILE_RE.match(name)
        if not m:
            continue
        with open(os.path.join(impl_dir, name)) as fh:
            report = json.load(fh)
        runs.setdefault(m["req"], {}).setdefault(int(m["c"]), []).append(report)
    return runs


def throughput_series(runs):
    """{concurrency: median blocks/s} for the charted GetBlockRange window."""
    req = f"range{THROUGHPUT_W}"
    out = {}
    for c, reports in runs.get(req, {}).items():
        out[c] = statistics.median(r.get("rps", 0) for r in reports) * THROUGHPUT_W
    return out


def p99_series(runs):
    """{concurrency: median GetBlock p99, in microseconds}."""
    out = {}
    for c, reports in runs.get("getblock", {}).items():
        vals = [pct99(r.get("latencyDistribution")) for r in reports]
        vals = [v for v in vals if v is not None]
        if vals:
            out[c] = statistics.median(vals) / 1000.0
    return out


def fmt_k(v):
    if v >= 1000:
        s = f"{v / 1000:.0f}k"
        return s
    return f"{v:.0f}"


def nice_step(target):
    """A 1/2/5×10^n step near `target`."""
    exp = math.floor(math.log10(target))
    base = target / (10 ** exp)
    mult = 1 if base < 1.5 else 2 if base < 3.5 else 5 if base < 7.5 else 10
    return mult * (10 ** exp)


def line_chart(title, ylabel, rust_pts, go_pts, ylog):
    """SVG string: two lines over a shared log2 concurrency x-axis."""
    width, height = 720, 380
    ml, mr, mt, mb = 66, 96, 40, 46
    pw, ph = width - ml - mr, height - mt - mb
    xs = sorted(set(rust_pts) | set(go_pts))
    xmax_log = math.log2(xs[-1]) if xs[-1] > 1 else 1
    yvals = [v for v in list(rust_pts.values()) + list(go_pts.values())]
    e = []
    e.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
        f'font-family="-apple-system,Segoe UI,Roboto,sans-serif" font-size="13">'
    )
    e.append(f'<text x="{width/2:.0f}" y="22" text-anchor="middle" font-size="15" fill="{AXIS}">{title}</text>')

    def xpos(c):
        return ml + (math.log2(c) / xmax_log) * pw

    # Y scale and gridlines.
    if ylog:
        lo = 10 ** math.floor(math.log10(min(yvals)))
        hi = 10 ** math.ceil(math.log10(max(yvals)))
        ticks, t = [], lo
        while t <= hi + 1e-9:
            ticks.append(t)
            t *= 10

        def ypos(v):
            return mt + ph - (math.log10(v) - math.log10(lo)) / (math.log10(hi) - math.log10(lo)) * ph
    else:
        step = nice_step(max(yvals) / 5)
        hi = math.ceil(max(yvals) / step) * step
        ticks = [i * step for i in range(int(hi / step) + 1)]

        def ypos(v):
            return mt + ph - (v / hi) * ph

    for t in ticks:
        y = ypos(t)
        e.append(f'<line x1="{ml}" y1="{y:.1f}" x2="{ml+pw}" y2="{y:.1f}" stroke="{AXIS}" stroke-opacity="0.2"/>')
        e.append(f'<text x="{ml-8}" y="{y+4:.1f}" text-anchor="end" fill="{AXIS}">{fmt_k(t)}</text>')

    # X ticks (concurrency).
    for c in xs:
        x = xpos(c)
        e.append(f'<text x="{x:.1f}" y="{mt+ph+18:.0f}" text-anchor="middle" fill="{AXIS}">{c}</text>')

    # Axes.
    e.append(f'<line x1="{ml}" y1="{mt}" x2="{ml}" y2="{mt+ph}" stroke="{AXIS}"/>')
    e.append(f'<line x1="{ml}" y1="{mt+ph}" x2="{ml+pw}" y2="{mt+ph}" stroke="{AXIS}"/>')
    # Axis labels.
    e.append(f'<text x="{ml+pw/2:.0f}" y="{height-8}" text-anchor="middle" fill="{AXIS}">concurrency (clients)</text>')
    e.append(f'<text transform="translate(16,{mt+ph/2:.0f}) rotate(-90)" text-anchor="middle" fill="{AXIS}">{ylabel}</text>')

    # Series.
    def draw(pts, color, label, ly):
        ordered = sorted(pts)
        poly = " ".join(f"{xpos(c):.1f},{ypos(pts[c]):.1f}" for c in ordered)
        e.append(f'<polyline points="{poly}" fill="none" stroke="{color}" stroke-width="2.5"/>')
        for c in ordered:
            e.append(f'<circle cx="{xpos(c):.1f}" cy="{ypos(pts[c]):.1f}" r="3.5" fill="{color}"/>')
        e.append(f'<line x1="{ml+pw+14}" y1="{ly}" x2="{ml+pw+30}" y2="{ly}" stroke="{color}" stroke-width="2.5"/>')
        e.append(f'<text x="{ml+pw+34}" y="{ly+4}" fill="{color}">{label}</text>')

    draw(rust_pts, RUST, "rust", mt + 10)
    draw(go_pts, GO, "go", mt + 30)
    e.append("</svg>")
    return "\n".join(e)


def main():
    os.makedirs(CHARTS, exist_ok=True)
    written = []
    for profile in sorted(p for p in os.listdir(RESULTS)
                          if os.path.isdir(os.path.join(RESULTS, p))):
        rust, go = collect(profile, "rust"), collect(profile, "go")
        if not rust or not go:
            continue
        charts = [
            (f"{profile}-throughput.svg",
             line_chart(f"{profile} — GetBlockRange throughput",
                        f"blocks/s (W={THROUGHPUT_W})",
                        throughput_series(rust), throughput_series(go), ylog=False)),
            (f"{profile}-latency.svg",
             line_chart(f"{profile} — GetBlock p99 latency",
                        "µs (log scale)",
                        p99_series(rust), p99_series(go), ylog=True)),
        ]
        for name, svg in charts:
            with open(os.path.join(CHARTS, name), "w") as fh:
                fh.write(svg + "\n")
            written.append(name)
    print("wrote:", ", ".join(written) if written else "(no results found)")


if __name__ == "__main__":
    main()
