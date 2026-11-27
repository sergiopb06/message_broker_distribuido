#!/usr/bin/env python3
"""
Charts of the message broker benchmarks, as SVG files with no dependencies,
in bench/results/plots/. Every environment found is drawn in the same chart.

usage: python3 bench/plot.py [results.csv ...]     (default: bench/results/bench_*.csv)

  <scenario>_speedup.svg      S(p) per environment, the ideal S(p) = p and Amdahl's fit
  <scenario>_efficiency.svg   E(p) per environment        (brokers, partitions, consumers)
  <scenario>_latency.svg      median and p99 latency of the produce round trip
  hot_throughput.svg          msg/s against the share of messages with the hot key
  replication_throughput.svg  msg/s against acks, one line per R and environment
  replication_latency.svg     p99 against acks, one line per R and environment
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import analyze  # noqa: E402

PALETTE = ["#2563eb", "#ea580c", "#10b981", "#9333ea", "#dc2626", "#0891b2"]
IDEAL = "#9ca3af"

X_TITLES = {
    "brokers": "Brokers (and partitions)",
    "partitions": "Partitions (3 brokers)",
    "consumers": "Members of the group",
}


def generate_svg_line_chart(title, x_label, y_label, series_dict, x_vals, y_max=None, is_efficiency=False):
    """SVG line chart; series_dict maps a name to {"color", "data": {x: y}, "dashed"}."""
    width, height = 700, 420
    margin_l, margin_r, margin_t, margin_b = 80, 140, 60, 60
    plot_w = width - margin_l - margin_r
    plot_h = height - margin_t - margin_b

    all_y = [v for s in series_dict.values() for v in s["data"].values()]
    if not all_y:
        all_y = [1.0]

    if y_max is None:
        y_max = max(all_y) * 1.15
        if is_efficiency:
            y_max = max(1.1, y_max)
    y_min = 0.0

    def get_x(val):
        idx = x_vals.index(val)
        return margin_l + (idx / max(1, len(x_vals) - 1)) * plot_w

    def get_y(val):
        return margin_t + plot_h - ((val - y_min) / max(1e-6, y_max - y_min)) * plot_h

    svg = []
    svg.append(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
               f'style="background-color: #ffffff; font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica, Arial, sans-serif;">')

    # Background and grid
    svg.append(f'  <rect width="{width}" height="{height}" fill="#ffffff" rx="8"/>')
    svg.append(f'  <text x="{width / 2}" y="32" text-anchor="middle" font-size="16" font-weight="bold" fill="#1f2937">{title}</text>')

    # Horizontal grid
    n_ticks = 5
    for i in range(n_ticks + 1):
        y_val = y_min + (y_max - y_min) * (i / n_ticks)
        y_pos = get_y(y_val)
        svg.append(f'  <line x1="{margin_l}" y1="{y_pos:.1f}" x2="{margin_l + plot_w}" y2="{y_pos:.1f}" stroke="#e5e7eb" stroke-width="1"/>')
        val_str = f"{y_val:.2f}" if y_max < 5 else f"{y_val:.1f}"
        svg.append(f'  <text x="{margin_l - 12}" y="{y_pos + 4:.1f}" text-anchor="end" font-size="11" fill="#6b7280">{val_str}</text>')

    # Vertical grid and X-axis labels
    for p in x_vals:
        x_pos = get_x(p)
        svg.append(f'  <line x1="{x_pos:.1f}" y1="{margin_t}" x2="{x_pos:.1f}" y2="{margin_t + plot_h}" stroke="#f3f4f6" stroke-width="1"/>')
        svg.append(f'  <text x="{x_pos:.1f}" y="{margin_t + plot_h + 20}" text-anchor="middle" font-size="12" font-weight="500" fill="#374151">{p}</text>')

    # Axis titles
    svg.append(f'  <text x="{margin_l + plot_w / 2}" y="{height - 15}" text-anchor="middle" font-size="12" fill="#4b5563">{x_label}</text>')
    svg.append(f'  <text x="25" y="{margin_t + plot_h / 2}" text-anchor="middle" transform="rotate(-90 25 {margin_t + plot_h / 2})" font-size="12" fill="#4b5563">{y_label}</text>')

    # Main axes
    svg.append(f'  <line x1="{margin_l}" y1="{margin_t}" x2="{margin_l}" y2="{margin_t + plot_h}" stroke="#9ca3af" stroke-width="1.5"/>')
    svg.append(f'  <line x1="{margin_l}" y1="{margin_t + plot_h}" x2="{margin_l + plot_w}" y2="{margin_t + plot_h}" stroke="#9ca3af" stroke-width="1.5"/>')

    # Ideal line for speedup or efficiency, when applicable
    if is_efficiency:
        y_ideal = get_y(1.0)
        svg.append(f'  <line x1="{margin_l}" y1="{y_ideal:.1f}" x2="{margin_l + plot_w}" y2="{y_ideal:.1f}" stroke="#9ca3af" stroke-width="1.5" stroke-dasharray="4,4"/>')

    # Draw each series
    legend_y = margin_t + 10
    for name, s in series_dict.items():
        color = s["color"]
        data = s["data"]
        points = []
        for p in x_vals:
            if p in data:
                points.append((get_x(p), get_y(data[p])))

        if len(points) >= 2:
            path_d = f"M {points[0][0]:.1f} {points[0][1]:.1f} " + " ".join([f"L {pt[0]:.1f} {pt[1]:.1f}" for pt in points[1:]])
            dash = ' stroke-dasharray="5,5"' if s.get("dashed") else ''
            svg.append(f'  <path d="{path_d}" fill="none" stroke="{color}" stroke-width="2.5"{dash}/>')

        for pt in points:
            svg.append(f'  <circle cx="{pt[0]:.1f}" cy="{pt[1]:.1f}" r="4.5" fill="{color}" stroke="#ffffff" stroke-width="1.5"/>')

        # Legend
        svg.append(f'  <line x1="{margin_l + plot_w + 15}" y1="{legend_y}" x2="{margin_l + plot_w + 35}" y2="{legend_y}" stroke="{color}" stroke-width="2.5"/>')
        svg.append(f'  <circle cx="{margin_l + plot_w + 25}" cy="{legend_y}" r="3.5" fill="{color}"/>')
        svg.append(f'  <text x="{margin_l + plot_w + 42}" y="{legend_y + 4}" font-size="11" fill="#374151">{name}</text>')
        legend_y += 24

    svg.append('</svg>')
    return "\n".join(svg)


def by_env(points, scenario):
    """environment label -> points of that scenario"""
    out = {}
    for (env, nodes, scen), pts in sorted(points.items()):
        if scen == scenario:
            out[f"{env} ({nodes}n)"] = pts
    return out


def write(plots_dir, name, svg):
    path = os.path.join(plots_dir, name)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"    {path}")


def number(text):
    value = float(text)
    return int(value) if value.is_integer() else value


def speedup_charts(points, plots_dir):
    for scenario in analyze.SPEEDUP_SCENARIOS:
        envs = by_env(points, scenario)
        if not envs:
            continue
        x_vals = sorted({number(pt["param"]) for pts in envs.values() for pt in pts})
        speedup, efficiency, latency = {}, {}, {}
        for i, (label, pts) in enumerate(envs.items()):
            color = PALETTE[i % len(PALETTE)]
            speedup[label] = {"color": color, "data": {number(pt["param"]): pt["speedup"] for pt in pts}}
            speedup[f"{label} Amdahl f={pts[0]['amdahl_f']:.2f}"] = {
                "color": color, "dashed": True,
                "data": {number(pt["param"]): pt["amdahl_s"] for pt in pts}}
            efficiency[label] = {"color": color, "data": {number(pt["param"]): pt["efficiency"] for pt in pts}}
            latency[f"{label} p50"] = {"color": color, "data": {number(pt["param"]): pt["p50"] for pt in pts}}
            latency[f"{label} p99"] = {"color": color, "dashed": True,
                                       "data": {number(pt["param"]): pt["p99"] for pt in pts}}
        p0 = x_vals[0]
        speedup["Ideal S(p) = p"] = {"color": IDEAL, "dashed": True, "data": {p: p / p0 for p in x_vals}}
        x_title = X_TITLES[scenario]
        write(plots_dir, f"{scenario}_speedup.svg", generate_svg_line_chart(
            f"Speedup S(p) - {scenario}", x_title, "S(p) by throughput", speedup, x_vals))
        write(plots_dir, f"{scenario}_efficiency.svg", generate_svg_line_chart(
            f"Efficiency E(p) - {scenario}", x_title, "E(p)", efficiency, x_vals, y_max=1.1, is_efficiency=True))
        if scenario != "consumers":
            write(plots_dir, f"{scenario}_latency.svg", generate_svg_line_chart(
                f"Produce latency - {scenario}", x_title, "ms (median of runs)", latency, x_vals))


def hot_chart(points, plots_dir):
    envs = by_env(points, "hot")
    if not envs:
        return
    x_vals = sorted({number(pt["param"]) for pts in envs.values() for pt in pts})
    series = {label: {"color": PALETTE[i % len(PALETTE)],
                      "data": {number(pt["param"]): pt["throughput"] for pt in pts}}
              for i, (label, pts) in enumerate(envs.items())}
    write(plots_dir, "hot_throughput.svg", generate_svg_line_chart(
        "Hot key - throughput", "% of messages with the same key", "msg/s", series, x_vals))


def replication_charts(points, plots_dir):
    envs = by_env(points, "replication")
    if not envs:
        return
    x_vals = ["0", "1", "ALL"]
    throughput, latency = {}, {}
    i = 0
    for label, pts in envs.items():
        for r in sorted({pt["replication"] for pt in pts}):
            color = PALETTE[i % len(PALETTE)]
            i += 1
            line = {pt["acks"]: pt for pt in pts if pt["replication"] == r}
            throughput[f"{label} R={r}"] = {"color": color, "data": {a: p["throughput"] for a, p in line.items()}}
            latency[f"{label} R={r}"] = {"color": color, "data": {a: p["p99"] for a, p in line.items()}}
    write(plots_dir, "replication_throughput.svg", generate_svg_line_chart(
        "Replication cost - throughput", "acks", "msg/s", throughput, x_vals))
    write(plots_dir, "replication_latency.svg", generate_svg_line_chart(
        "Replication cost - p99 latency", "acks", "ms", latency, x_vals))


def main(argv):
    paths = analyze.result_files(argv[1:])
    if not paths:
        print("no results: run bench/run.sh first", file=sys.stderr)
        return 1
    points = analyze.load_points(paths)
    plots_dir = os.path.join(analyze.ROOT, "bench", "results", "plots")
    os.makedirs(plots_dir, exist_ok=True)
    print(f"charts from {len(paths)} result file(s):")
    speedup_charts(points, plots_dir)
    hot_chart(points, plots_dir)
    replication_charts(points, plots_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
