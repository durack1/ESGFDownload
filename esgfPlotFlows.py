#!/usr/bin/env python3
"""
esgfPlotFlows.py

Generate a world flow-map of ESGF download performance from esgfDownloadLog.py
logs. Arcs run from each ESGF node (data source) to the colleague's machine
(data sink), coloured by sustained download speed (avg_rate_MBps).

Two renderers, both from one shared data layer:
  * Static  (matplotlib + cartopy) -> publication-quality PNG/PDF/SVG. Each arc
    is annotated with the number of unique download attempts it aggregates.
  * Interactive (plotly) -> self-contained HTML. Hover an arc for the full
    breakdown: origin host/city/ISP, node, per-download speeds, and the mean.

All coordinates and speeds are read straight from the logs (the logger embeds
origin_lat/lon and dest_lat/lon), so no external lookups or hardcoded node
tables are needed.

Usage:
    python esgfPlotFlows.py LOG_OR_DIR [LOG_OR_DIR ...] [options]

    LOG_OR_DIR  one or more *_esgf_results.csv files, or directories to scan.

Options:
    --out-prefix PREFIX   output basename (default: esgf_flowmap)
    --static-only         only render the PNG
    --interactive-only    only render the HTML
    --format {png,pdf,svg}  static image format (default: png)
    --min-speed FLOAT     clamp colour scale minimum (MB/s)
    --max-speed FLOAT     clamp colour scale maximum (MB/s)
    --title TEXT          figure title

PJD 22 Jul 2026 - started.

"""

import argparse
import csv
import glob
import math
import os
import sys
from collections import defaultdict


# --------------------------------------------------------------------------- #
# DATA LAYER
# --------------------------------------------------------------------------- #

def _to_float(v):
    try:
        if v is None or v == "":
            return None
        return float(v)
    except (ValueError, TypeError):
        return None


def find_log_files(paths):
    """Expand a mix of files and directories into a sorted list of CSV logs."""
    files = []
    for p in paths:
        if os.path.isdir(p):
            files.extend(glob.glob(os.path.join(p, "*_esgf_results.csv")))
            files.extend(glob.glob(os.path.join(p, "**", "*_esgf_results.csv"),
                                   recursive=True))
        elif os.path.isfile(p):
            files.append(p)
        else:
            print(f"  warning: {p!r} is not a file or directory, skipping",
                  file=sys.stderr)
    # de-dup while preserving order
    seen = set()
    out = []
    for f in files:
        rp = os.path.realpath(f)
        if rp not in seen:
            seen.add(rp)
            out.append(f)
    return out


def load_downloads(files):
    """Read all logs into a flat list of per-download records with the fields
    the map needs. Rows lacking coordinates or a speed are skipped (counted)."""
    records = []
    skipped = 0
    for path in files:
        try:
            with open(path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    olat = _to_float(row.get("origin_lat"))
                    olon = _to_float(row.get("origin_lon"))
                    dlat = _to_float(row.get("dest_lat"))
                    dlon = _to_float(row.get("dest_lon"))
                    rate = _to_float(row.get("avg_rate_MBps"))
                    if None in (olat, olon, dlat, dlon) or rate is None:
                        skipped += 1
                        continue
                    records.append({
                        "origin_host": row.get("origin_hostname") or "?",
                        "origin_city": row.get("origin_city") or "",
                        "origin_country": row.get("origin_country") or "",
                        "origin_isp": row.get("origin_isp") or "",
                        "origin_lat": olat, "origin_lon": olon,
                        "dest_org": row.get("dest_org") or "",
                        "dest_city": row.get("dest_city") or "",
                        "dest_country": row.get("dest_country") or "",
                        "dest_host": row.get("host") or "",
                        "dest_lat": dlat, "dest_lon": dlon,
                        "rate": rate,
                        "label": row.get("label") or "",
                        "os": row.get("os_description") or "",
                        "status": row.get("status") or "",
                        "source_file": os.path.basename(path),
                    })
        except Exception as e:  # noqa: BLE001
            print(f"  warning: could not read {path!r}: {e}", file=sys.stderr)
    return records, skipped


def _node_name(rec):
    """Human label for an ESGF node: prefer org, else city, else hostname."""
    return rec["dest_org"] or rec["dest_city"] or rec["dest_host"] or "node"


def _origin_name(rec):
    return rec["origin_city"] or rec["origin_host"] or "origin"


def aggregate_flows(records):
    """Group downloads by (origin, node) endpoint pair. Each flow carries the
    mean speed (for arc colour), the count of unique attempts, and the list of
    individual downloads (for hover detail)."""
    groups = defaultdict(list)
    for r in records:
        key = (round(r["origin_lat"], 3), round(r["origin_lon"], 3),
               round(r["dest_lat"], 3), round(r["dest_lon"], 3))
        groups[key].append(r)
    flows = []
    for key, rows in groups.items():
        rates = [r["rate"] for r in rows]
        flows.append({
            "origin_lat": rows[0]["origin_lat"],
            "origin_lon": rows[0]["origin_lon"],
            "dest_lat": rows[0]["dest_lat"],
            "dest_lon": rows[0]["dest_lon"],
            "origin_name": _origin_name(rows[0]),
            "origin_host": rows[0]["origin_host"],
            "origin_isp": rows[0]["origin_isp"],
            "origin_country": rows[0]["origin_country"],
            "node_name": _node_name(rows[0]),
            "dest_host": rows[0]["dest_host"],
            "dest_country": rows[0]["dest_country"],
            "mean_rate": sum(rates) / len(rates),
            "min_rate": min(rates),
            "max_rate": max(rates),
            "count": len(rows),
            "downloads": rows,
        })
    return flows


def great_circle_points(lat1, lon1, lat2, lon2, n=100):
    """Interpolate points along the great-circle path (slerp on the sphere).
    Returns (lats, lons). Used for gently curved arcs on both renderers."""
    p1 = math.radians(lat1), math.radians(lon1)
    p2 = math.radians(lat2), math.radians(lon2)
    d = 2 * math.asin(math.sqrt(
        math.sin((p2[0] - p1[0]) / 2) ** 2 +
        math.cos(p1[0]) * math.cos(p2[0]) * math.sin((p2[1] - p1[1]) / 2) ** 2))
    if d == 0:
        return [lat1, lat2], [lon1, lon2]
    lats, lons = [], []
    for i in range(n + 1):
        f = i / n
        a = math.sin((1 - f) * d) / math.sin(d)
        b = math.sin(f * d) / math.sin(d)
        x = a * math.cos(p1[0]) * math.cos(p1[1]) + \
            b * math.cos(p2[0]) * math.cos(p2[1])
        y = a * math.cos(p1[0]) * math.sin(p1[1]) + \
            b * math.cos(p2[0]) * math.sin(p2[1])
        z = a * math.sin(p1[0]) + b * math.sin(p2[0])
        lats.append(math.degrees(math.atan2(z, math.sqrt(x * x + y * y))))
        lons.append(math.degrees(math.atan2(y, x)))
    return lats, lons


def speed_bounds(flows, cli_min, cli_max):
    rates = [f["mean_rate"] for f in flows] or [0, 1]
    lo = cli_min if cli_min is not None else min(rates)
    hi = cli_max if cli_max is not None else max(rates)
    if hi <= lo:
        hi = lo + 1
    return lo, hi


# --------------------------------------------------------------------------- #
# STATIC RENDERER (matplotlib + cartopy)
# --------------------------------------------------------------------------- #

def render_static(flows, out_path, vmin, vmax, title, fmt):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.cm as cm
    import matplotlib.colors as mcolors
    from matplotlib.lines import Line2D
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature

    fig = plt.figure(figsize=(16, 9), dpi=200)
    ax = plt.axes(projection=ccrs.Robinson())
    ax.set_global()

    # Muted basemap so the coloured arcs carry the signal. Natural Earth data
    # is fetched on first use; cartopy defers that fetch until draw time, so we
    # probe availability up front and fall back to a plain graticule if the
    # data can't be downloaded (e.g. an offline machine). On a normal machine
    # this fetch happens once and is then cached.
    basemap_ok = True
    try:
        # Force acquisition now so a download failure is caught here, not at
        # savefig time.
        list(cfeature.COASTLINE.geometries())
    except Exception as e:  # noqa: BLE001
        basemap_ok = False
        print(f"  note: basemap data unavailable ({type(e).__name__}); "
              "rendering arcs on a plain graticule. On a machine with internet "
              "cartopy fetches coastlines automatically on first run.",
              file=sys.stderr)

    if basemap_ok:
        ax.add_feature(cfeature.LAND, facecolor="#EAE6DE", edgecolor="none",
                       zorder=0)
        ax.add_feature(cfeature.OCEAN, facecolor="#F4F1EA", zorder=0)
        ax.add_feature(cfeature.COASTLINE, linewidth=0.4,
                       edgecolor="#B8B2A6", zorder=1)
        ax.add_feature(cfeature.BORDERS, linewidth=0.25,
                       edgecolor="#CFC9BD", zorder=1)
    else:
        ax.patch.set_facecolor("#F4F1EA")
        ax.gridlines(draw_labels=False, linewidth=0.3, color="#CFC9BD",
                     alpha=0.6)

    cmap = matplotlib.colormaps["viridis"]
    norm = mcolors.Normalize(vmin=vmin, vmax=vmax)

    geo = ccrs.Geodetic()

    # Draw slowest first so faster (brighter) arcs sit on top.
    for fl in sorted(flows, key=lambda f: f["mean_rate"]):
        color = cmap(norm(fl["mean_rate"]))
        # Node -> origin (data flows to the user), cartopy Geodetic curves it.
        ax.plot([fl["dest_lon"], fl["origin_lon"]],
                [fl["dest_lat"], fl["origin_lat"]],
                color=color, linewidth=1.8, alpha=0.9, transform=geo,
                zorder=3, solid_capstyle="round")

    # Endpoint markers: nodes (squares) and origins (circles).
    seen_nodes, seen_origins = {}, {}
    for fl in flows:
        seen_nodes[(fl["dest_lat"], fl["dest_lon"])] = fl["node_name"]
        seen_origins[(fl["origin_lat"], fl["origin_lon"])] = fl["origin_name"]

    for (la, lo), name in seen_nodes.items():
        ax.scatter([lo], [la], marker="s", s=42, c="#2A2A2A",
                   edgecolors="white", linewidths=0.6, transform=geo, zorder=5)
        ax.text(lo, la + 2.5, name, fontsize=6.5, ha="center", va="bottom",
                transform=geo, zorder=6, color="#2A2A2A",
                fontweight="bold")
    for (la, lo), name in seen_origins.items():
        ax.scatter([lo], [la], marker="o", s=30, c="white",
                   edgecolors="#2A2A2A", linewidths=1.0, transform=geo,
                   zorder=5)

    # Count annotation at each arc midpoint (unique attempts aggregated).
    for fl in flows:
        if fl["count"] > 1:
            arc_lats, arc_lons = great_circle_points(
                fl["dest_lat"], fl["dest_lon"],
                fl["origin_lat"], fl["origin_lon"], n=20)
            mid = len(arc_lats) // 2
            ax.text(arc_lons[mid], arc_lats[mid], f"n={fl['count']}",
                    fontsize=5.5, ha="center", va="center", transform=geo,
                    zorder=6, color="#404040",
                    bbox=dict(boxstyle="round,pad=0.15", fc="white",
                              ec="none", alpha=0.7))

    # Colourbar.
    sm = cm.ScalarMappable(norm=norm, cmap=cmap)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, orientation="horizontal", fraction=0.04,
                        pad=0.03, shrink=0.5)
    cbar.set_label("Sustained download speed (MB/s)", fontsize=9)
    cbar.ax.tick_params(labelsize=8)

    # Legend for marker meaning + direction.
    handles = [
        Line2D([0], [0], marker="s", color="none", markerfacecolor="#2A2A2A",
               markeredgecolor="white", markersize=7, label="ESGF node (source)"),
        Line2D([0], [0], marker="o", color="none", markerfacecolor="white",
               markeredgecolor="#2A2A2A", markersize=7, label="Colleague (sink)"),
    ]
    ax.legend(handles=handles, loc="lower left", fontsize=7.5,
              frameon=True, facecolor="white", edgecolor="#CFC9BD")

    n_dl = sum(f["count"] for f in flows)
    ttl = title or "ESGF download performance: node \u2192 client"
    ax.set_title(f"{ttl}\n{len(flows)} routes, {n_dl} downloads",
                 fontsize=13, fontweight="bold", pad=12)

    fig.savefig(out_path, format=fmt, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_path


# --------------------------------------------------------------------------- #
# INTERACTIVE RENDERER (plotly)
# --------------------------------------------------------------------------- #

def render_interactive(flows, out_path, vmin, vmax, title):
    import plotly.graph_objects as go

    fig = go.Figure()

    # Sample the viridis colours via plotly's built-in scale.
    import plotly.colors as pcolors

    def color_for(rate):
        frac = 0.0 if vmax == vmin else (rate - vmin) / (vmax - vmin)
        frac = max(0.0, min(1.0, frac))
        return pcolors.sample_colorscale("Viridis", [frac])[0]

    # Arc traces (one per flow) so each can carry its own hover text.
    for fl in sorted(flows, key=lambda f: f["mean_rate"]):
        lats, lons = great_circle_points(
            fl["dest_lat"], fl["dest_lon"],
            fl["origin_lat"], fl["origin_lon"], n=80)
        per_dl = "<br>".join(
            f"&nbsp;&nbsp;{d['label'] or 'download'}: {d['rate']:.2f} MB/s"
            for d in sorted(fl["downloads"], key=lambda d: -d["rate"])
        )
        hover = (
            f"<b>{fl['node_name']} \u2192 {fl['origin_name']}</b><br>"
            f"source: {fl['node_name']} ({fl['dest_country']})<br>"
            f"&nbsp;&nbsp;{fl['dest_host']}<br>"
            f"sink: {fl['origin_host']} \u2014 {fl['origin_name']}, "
            f"{fl['origin_country']}<br>"
            f"&nbsp;&nbsp;ISP: {fl['origin_isp']}<br>"
            f"<b>mean: {fl['mean_rate']:.2f} MB/s</b> "
            f"(min {fl['min_rate']:.2f}, max {fl['max_rate']:.2f})<br>"
            f"downloads (n={fl['count']}):<br>{per_dl}"
            "<extra></extra>"
        )
        fig.add_trace(go.Scattergeo(
            lat=lats, lon=lons, mode="lines",
            line=dict(width=2.2, color=color_for(fl["mean_rate"])),
            opacity=0.9, hoverinfo="text", text=hover,
            showlegend=False,
        ))

    # Node markers (squares).
    node_pts = {}
    origin_pts = {}
    for fl in flows:
        node_pts[(fl["dest_lat"], fl["dest_lon"])] = fl
        origin_pts[(fl["origin_lat"], fl["origin_lon"])] = fl
    fig.add_trace(go.Scattergeo(
        lat=[k[0] for k in node_pts], lon=[k[1] for k in node_pts],
        mode="markers+text",
        marker=dict(symbol="square", size=9, color="#2A2A2A",
                    line=dict(width=1, color="white")),
        text=[f["node_name"] for f in node_pts.values()],
        textposition="top center", textfont=dict(size=9, color="#2A2A2A"),
        hovertext=[f"{f['node_name']} ({f['dest_country']})<br>{f['dest_host']}"
                   for f in node_pts.values()],
        hoverinfo="text", name="ESGF node (source)",
    ))
    fig.add_trace(go.Scattergeo(
        lat=[k[0] for k in origin_pts], lon=[k[1] for k in origin_pts],
        mode="markers",
        marker=dict(symbol="circle", size=8, color="white",
                    line=dict(width=1.4, color="#2A2A2A")),
        hovertext=[f"{f['origin_host']} \u2014 {f['origin_name']}, "
                   f"{f['origin_country']}" for f in origin_pts.values()],
        hoverinfo="text", name="Colleague (sink)",
    ))

    # A hidden marker trace to carry the colourbar for the speed scale.
    fig.add_trace(go.Scattergeo(
        lat=[None], lon=[None], mode="markers",
        marker=dict(size=0.1, color=[vmin, vmax], colorscale="Viridis",
                    cmin=vmin, cmax=vmax, showscale=True,
                    colorbar=dict(title="MB/s", thickness=14, len=0.5,
                                  x=0.02, y=0.25)),
        showlegend=False, hoverinfo="skip",
    ))

    n_dl = sum(f["count"] for f in flows)
    ttl = title or "ESGF download performance: node \u2192 client"
    fig.update_layout(
        title=dict(text=f"{ttl}  \u2014  {len(flows)} routes, {n_dl} downloads",
                   font=dict(size=16)),
        geo=dict(projection_type="natural earth",
                 showland=True, landcolor="#EAE6DE",
                 showocean=True, oceancolor="#F4F1EA",
                 showcountries=True, countrycolor="#CFC9BD",
                 coastlinecolor="#B8B2A6", coastlinewidth=0.5),
        legend=dict(x=0.02, y=0.98, bgcolor="rgba(255,255,255,0.8)"),
        margin=dict(l=0, r=0, t=50, b=0),
        paper_bgcolor="white",
    )

    fig.write_html(out_path, include_plotlyjs="cdn")
    return out_path


# --------------------------------------------------------------------------- #
# MAIN
# --------------------------------------------------------------------------- #

def parse_args(argv):
    p = argparse.ArgumentParser(
        description="Plot ESGF download flow-map from esgfDownloadLog logs.")
    p.add_argument("paths", nargs="+",
                   help="log CSV files and/or directories to scan")
    p.add_argument("--out-prefix", default="esgf_flowmap")
    p.add_argument("--static-only", action="store_true")
    p.add_argument("--interactive-only", action="store_true")
    p.add_argument("--format", choices=["png", "pdf", "svg"], default="png")
    p.add_argument("--min-speed", type=float, default=None)
    p.add_argument("--max-speed", type=float, default=None)
    p.add_argument("--title", default=None)
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv if argv is not None else sys.argv[1:])

    files = find_log_files(args.paths)
    if not files:
        print("No *_esgf_results.csv logs found.", file=sys.stderr)
        return 2
    print(f"Reading {len(files)} log file(s) ...")

    records, skipped = load_downloads(files)
    if not records:
        print("No plottable downloads (need origin_lat/lon, dest_lat/lon and "
              "avg_rate_MBps). Are these logs from the geo-enabled logger?",
              file=sys.stderr)
        return 2
    if skipped:
        print(f"  note: skipped {skipped} row(s) lacking coordinates or speed")

    flows = aggregate_flows(records)
    vmin, vmax = speed_bounds(flows, args.min_speed, args.max_speed)
    print(f"  {len(records)} downloads across {len(flows)} unique routes; "
          f"speed range {vmin:.1f}\u2013{vmax:.1f} MB/s")

    outputs = []
    if not args.interactive_only:
        static_path = f"{args.out_prefix}.{args.format}"
        render_static(flows, static_path, vmin, vmax, args.title, args.format)
        outputs.append(static_path)
        print(f"  wrote {static_path}")
    if not args.static_only:
        html_path = f"{args.out_prefix}.html"
        render_interactive(flows, html_path, vmin, vmax, args.title)
        outputs.append(html_path)
        print(f"  wrote {html_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
