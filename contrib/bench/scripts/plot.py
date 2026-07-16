#!/usr/bin/env python3
"""Render benchmark result charts as SVG (stdlib only, no plotting deps).

Reads results/<profile>/<impl>/<request>-c<C>-r<rep>.json and writes, per profile,
a GetBlockRange throughput chart and a GetBlock p99-latency chart (Rust vs Go over
the concurrency curve) to contrib/bench/charts/. Colors and axis lines are chosen
to read on both light and dark GitHub themes. Also writes a fifth chart,
charts/ingest-sync.svg, from a hardcoded ingest/full-sync data table (see below) —
independent of results/, since there is no raw per-request JSON for a multi-hour
ingest run.

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

RUST = "#e43717"     # rust orange-red — NEW Rust (this tree), fixed across every chart
GO = "#00add8"       # go cyan
OLD_RUST = "#eda100" # amber — OLD Rust (pre-windowed-ingestor baseline); reference-palette
                     # "yellow" categorical slot 3, kept distinct from RUST/GO under CVD
AXIS = "#888888"  # mid gray: visible on light and dark
THROUGHPUT_W = 1000  # GetBlockRange window charted

# ─── Ingest / full-sync chart: hardcoded measured data ──────────────────────
#
# Unlike the throughput/latency charts above, this data does not come from the
# git-ignored results/ directory (there is no raw per-request JSON for a
# multi-hour sync). The numbers below are transcribed verbatim from:
#   contrib/bench/results/mainnet-2026-07-summary.md  (Part B — ingest A/B/C)
#   contrib/bench/results/mainnet-2026-07-phase2.md   (B4 — full genesis-to-tip sync)
# Update this block by hand if those docs are ever revised, and keep the two in sync.

# Part B: 480s ingest windows at three mainnet start heights, blocks/s.
# R3 is tip-capped for NEW Rust and Go (both caught up to the live chain tip
# inside the window and idled the rest); OLD Rust is nowhere near tip-capped.
INGEST_RANGES = [
    {"label": "R1 modern pre-spam (start 1,500,000)", "old": 55.0, "new": 497.7, "go": 352.1, "capped": False},
    {"label": "R2 sandblasting (start 1,780,000)", "old": 5.6, "new": 37.2, "go": 6.6, "capped": False},
    {"label": "R3 recent (start 3,300,000)", "old": 55.6, "new": 232.0, "go": 232.0, "capped": True},
]

# B4: genesis-to-tip wall-clock, one implementation at a time, default settings.
FULL_SYNC = {
    "new_seconds": 4950,          # 1h 22m 30s, measured to completion (tip 3,411,957)
    "go_measured_seconds": 28806, # 8h 00m 06s, stopped at the 8h cap (59.9% of chain)
    "go_extrap_low": 34000,       # ≈9.5h
    "go_extrap_high": 36000,      # ≈10h
}


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


def fmt_hms(seconds):
    h, rem = divmod(int(seconds), 3600)
    m = rem // 60
    return f"{h}h {m:02d}m" if h else f"{m}m"


def ingest_sync_svg():
    """SVG string: ingest blocks/s by range (small multiples — the three ranges
    span a ~90x magnitude difference, so a shared axis would flatten R2/R3 to
    slivers) plus the genesis-to-tip full-sync wall-clock comparison, which
    shares one axis since it is a single measure (seconds) for both impls."""
    width = 720
    ml, mr = 118, 96
    bar_area = width - ml - mr
    row_h, row_gap, bar_h = 14, 8, 14
    facet_gap = 16
    e = []
    y = 0

    def row_label(text, yy):
        e.append(f'<text x="{ml-8}" y="{yy+row_h-3:.1f}" text-anchor="end" fill="{AXIS}" font-size="12">{text}</text>')

    def value_label(text, x, yy):
        e.append(f'<text x="{x+6:.1f}" y="{yy+row_h-3:.1f}" fill="{AXIS}" font-size="12">{text}</text>')

    # ── Header + legend ──
    e.append(f'<text x="{width/2:.0f}" y="22" text-anchor="middle" font-size="15" fill="{AXIS}">'
              f'ingest throughput — NEW Rust vs OLD Rust vs Go (mainnet, 2026-07)</text>')
    y = 40
    for i, (label, color) in enumerate([("NEW Rust", RUST), ("OLD Rust", OLD_RUST), ("Go", GO)]):
        lx = ml + i * 150
        e.append(f'<rect x="{lx}" y="{y-10}" width="14" height="14" rx="3" fill="{color}"/>')
        e.append(f'<text x="{lx+20}" y="{y+1}" fill="{AXIS}" font-size="12">{label}</text>')
    y += 22

    # ── Facet 1: blocks/s per range, one independent linear scale per facet ──
    any_capped = False
    for rng in INGEST_RANGES:
        e.append(f'<text x="{ml}" y="{y+10}" fill="{AXIS}" font-size="13">{rng["label"]}</text>')
        y += 20
        max_val = max(rng["new"], rng["old"], rng["go"]) * 1.12
        for key, label, color in [("new", "NEW", RUST), ("old", "OLD", OLD_RUST), ("go", "Go", GO)]:
            val = rng[key]
            bw = (val / max_val) * bar_area if max_val > 0 else 0
            e.append(f'<rect x="{ml}" y="{y}" width="{bw:.1f}" height="{bar_h}" rx="4" fill="{color}"/>')
            row_label(label, y)
            star = "*" if rng["capped"] and key in ("new", "go") else ""
            value_label(f"{val:.1f} b/s{star}", ml + bw, y)
            y += row_h + row_gap
        if rng["capped"]:
            any_capped = True
        y += facet_gap

    if any_capped:
        e.append(f'<text x="{ml}" y="{y+8}" fill="{AXIS}" font-size="11">'
                  f'* tip-capped: ingestor caught up to the live chain tip mid-window and idled</text>')
        y += 15
        e.append(f'<text x="{ml}" y="{y+8}" fill="{AXIS}" font-size="11">'
                  f'(effective catch-up ≈281-297 blocks/s — see results docs)</text>')
        y += 24

    # ── Divider ──
    y += 6
    e.append(f'<line x1="{ml}" y1="{y}" x2="{width-mr+40}" y2="{y}" stroke="{AXIS}" stroke-opacity="0.25"/>')
    y += 24

    # ── Facet 2: full genesis-to-tip sync wall-clock, one shared axis (hours) ──
    e.append(f'<text x="{ml}" y="{y+10}" fill="{AXIS}" font-size="13">full sync: genesis → tip (wall-clock)</text>')
    y += 22
    x_max = FULL_SYNC["go_extrap_high"] * 1.06
    hour = 3600

    def xw(seconds):
        return (seconds / x_max) * bar_area

    # hour gridlines
    hh = 0
    while hh * hour <= x_max:
        gx = ml + xw(hh * hour)
        e.append(f'<line x1="{gx:.1f}" y1="{y}" x2="{gx:.1f}" y2="{y+2*(row_h+row_gap)}" '
                  f'stroke="{AXIS}" stroke-opacity="0.15"/>')
        e.append(f'<text x="{gx:.1f}" y="{y+2*(row_h+row_gap)+14}" text-anchor="middle" fill="{AXIS}" font-size="11">{hh}h</text>')
        hh += 2

    # NEW Rust bar. Short tip labels only (text stays a text token, never the
    # series color — identity comes from the colored mark, not colored text);
    # the fuller measured/extrapolated story is in the caption line below.
    bw = xw(FULL_SYNC["new_seconds"])
    e.append(f'<rect x="{ml}" y="{y}" width="{bw:.1f}" height="{bar_h}" rx="4" fill="{RUST}"/>')
    row_label("NEW Rust", y)
    value_label(fmt_hms(FULL_SYNC["new_seconds"]), ml + bw, y)
    y += row_h + row_gap

    # Go bar: solid = measured to the 8h cutoff; dashed outline = extrapolated remainder
    measured_w = xw(FULL_SYNC["go_measured_seconds"])
    extrap_w = xw(FULL_SYNC["go_extrap_high"]) - measured_w
    e.append(f'<rect x="{ml}" y="{y}" width="{measured_w:.1f}" height="{bar_h}" rx="4" fill="{GO}"/>')
    e.append(f'<rect x="{ml+measured_w:.1f}" y="{y}" width="{extrap_w:.1f}" height="{bar_h}" rx="4" '
              f'fill="none" stroke="{GO}" stroke-width="1.5" stroke-dasharray="3,3"/>')
    row_label("Go", y)
    value_label("≈9.5–10h", ml + measured_w + extrap_w, y)
    y += row_h + row_gap + 18

    e.append(f'<text x="{ml}" y="{y}" fill="{AXIS}" font-size="11">'
              f'NEW Rust: measured. Go: solid = measured to the 8h cap (59.9% of chain);</text>')
    y += 15
    e.append(f'<text x="{ml}" y="{y}" fill="{AXIS}" font-size="11">'
              f'dashed = extrapolated total from measured post-spam rates (see results docs).</text>')
    y += 16

    height = y + 16
    header = (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
              f'font-family="-apple-system,Segoe UI,Roboto,sans-serif" font-size="13">')
    return header + "\n" + "\n".join(e) + "\n</svg>"


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

    # Ingest/full-sync chart: hardcoded data (see the block above), so it is
    # written unconditionally, independent of the results/ directory.
    with open(os.path.join(CHARTS, "ingest-sync.svg"), "w") as fh:
        fh.write(ingest_sync_svg() + "\n")
    written.append("ingest-sync.svg")

    print("wrote:", ", ".join(written) if written else "(no results found)")


if __name__ == "__main__":
    main()
