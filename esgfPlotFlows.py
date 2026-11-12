#!/usr/bin/env python3
"""
esgfPlotFlows.py

Generate a world flow-map of ESGF download performance from esgfDownloadLog.py
logs. Arcs run from each ESGF node (data source) to the client machine (data
sink), coloured by mean sustained download speed (avg_rate_MBps).

Two renderers, both from one shared data layer:
  * Static  (matplotlib + cartopy) -> publication-quality PNG/PDF/SVG: the map
    plus a client-site x node matrix giving the mean speed and the number of
    download attempts for every route.
  * Interactive (plotly) -> one HTML page with the same map and matrix. Hover
    an arc or a matrix cell for the full breakdown: node, client host and
    ISP, and every download's speed or failure. The page loads plotly.js and
    its basemap from plotly's CDN, so viewing it needs internet access.

Everything is read from the logs. How the logs are interpreted:
  * A node is identified by its hostname (the "host" column). Its name is the
    label prefix used in the log ("CEDA_1GB" -> "CEDA") and its position is
    the most frequently logged dest_lat/dest_lon for that host, so the same
    node is never drawn twice when geolocation wobbles between runs.
  * Client machines within --cluster-km of each other are one client site.
  * Logs written before the geo columns existed are still used: coordinates
    are taken from the run's JSON sidecar when it has them, node positions are
    back-filled from any other log of the same host, and a run with no
    geolocation at all inherits the site of another run from the same
    hostname + local IP.
  * Failed downloads count as attempts but never towards the mean speed.
  * A short list of known corrections (node identity and position, where a
    run was really made, downloads wrongly logged as failed) is hard-coded
    near the top of this file: NODE_OVERRIDES, RUN_OVERRIDES and
    SIZE_MATCH_IS_SUCCESS_HOSTS.

Usage:
    python esgfPlotFlows.py LOG_OR_DIR [LOG_OR_DIR ...] [options]

    LOG_OR_DIR  one or more *_esgf_results*.csv files, or directories to scan.

Options:
    --out-prefix PREFIX     output basename (default: esgf_flowmap)
    --static-only           only render the static image
    --interactive-only      only render the HTML
    --format {png,pdf,svg}  static image format (default: png)
    --scale {log,linear}    colour scale (default: log)
    --min-speed FLOAT       clamp colour scale minimum (MB/s)
    --max-speed FLOAT       clamp colour scale maximum (MB/s)
    --cluster-km FLOAT      merge client machines closer than this (default 100)
    --no-table              static image: map only, no route matrix
    --title TEXT            figure title
    --basemap-dir DIR       local Natural Earth shapefiles (offline machines)

PJD 22 Jul 2026 - started.
"""

import argparse
import csv
import glob
import json
import math
import os
import sys
from collections import Counter, defaultdict

OK_STATUSES = {"verified", "complete_unverified", "checksum_mismatch"}

# --------------------------------------------------------------------------- #
# KNOWN CORRECTIONS TO THE LOGS
# Things the logs record wrongly or cannot know. Applied when the logs are
# read; the log files themselves are never modified.
# --------------------------------------------------------------------------- #

ANL_HOST = "g-52ba3.fd635.8443.data.globus.org"

# Node identity and position by hostname. Overrides both the label used in
# the log and the geolocated position of the node's IP address.
NODE_OVERRIDES = {
    # Argonne National Laboratory, 9700 S Cass Ave, Lemont, IL 60439, USA.
    # The earliest logs mislabelled this endpoint "NERSC-GLOBUS", and its IP
    # geolocates to Washington DC or Naperville depending on the run.
    ANL_HOST: {"name": "ANL", "city": "Lemont", "country": "United States",
               "lat": 41.7183, "lon": -87.9789},
}

# Nodes that do not report a file size, so the logger cannot confirm that an
# interrupted-then-resumed download finished and records it as "failed". For
# these hosts a "failed" row counts as completed when its byte count equals
# that of a verified download of the same size tier from the same host.
SIZE_MATCH_IS_SUCCESS_HOSTS = {ANL_HOST}

# Where a run was really made, keyed by the run stamp that starts the log
# file name. Only the fields given are replaced.
RUN_OVERRIDES = {
    # DWER Windows laptop on home internet; the run has no geo data. The ISP
    # is given as the other home-network logs from that week record it.
    "20260702T155255": {"origin_city": "Perth", "origin_country": "Australia",
                        "origin_isp": "SUPERLOOP AUSTRALIA PTY LTD",
                        "origin_lat": -31.9559, "origin_lon": 115.8606},
    # DWER laptop on the DWER wireless network, Prime House, Joondalup.
    "20260707T154140": {"origin_city": "Joondalup",
                        "origin_country": "Australia",
                        "origin_lat": -31.7450, "origin_lon": 115.7670},
    # Run from a fixed-line service in Pretoria; its IP geolocated to Cape
    # Town.
    "20261108T055957": {"origin_city": "Pretoria",
                        "origin_country": "South Africa",
                        "origin_lat": -25.7449, "origin_lon": 28.1878},
}

LAND_FC = "#E9E6E0"
OCEAN_FC = "#F7F5F0"
COAST_EC = "#B5AFA3"
BORDER_EC = "#D3CEC4"
INK = "#262626"
FAIL_GREY = "#8C8C8C"


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


def _mode(values):
    """Most common non-empty value (ties -> first seen)."""
    vals = [v for v in values if v not in (None, "")]
    return Counter(vals).most_common(1)[0][0] if vals else ""


def haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) *
         math.sin(math.radians(lon2 - lon1) / 2) ** 2)
    return 6371.0 * 2 * math.asin(math.sqrt(a))


def find_log_files(paths):
    """Expand a mix of files and directories into a sorted list of CSV logs
    (including *-partial.csv logs from interrupted runs)."""
    files = []
    for p in paths:
        if os.path.isdir(p):
            files.extend(glob.glob(os.path.join(p, "**", "*_esgf_results*.csv"),
                                   recursive=True))
        elif os.path.isfile(p):
            files.append(p)
        else:
            print(f"  warning: {p!r} is not a file or directory, skipping",
                  file=sys.stderr)
    seen, out = set(), []
    for f in sorted(files):
        rp = os.path.realpath(f)
        if rp not in seen:
            seen.add(rp)
            out.append(f)
    return out


def _load_sidecar(csv_path):
    """Read the run's JSON log (same name, .json) if it exists."""
    path = os.path.splitext(csv_path)[0] + ".json"
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:  # noqa: BLE001 - sidecar is optional
        return {}


def load_downloads(files):
    """Read every log row into a flat record. Coordinates come from the CSV
    when present, otherwise from the run's JSON sidecar; rows still lacking
    them are resolved (or dropped) later by resolve_records()."""
    records = []
    for path in files:
        side = _load_sidecar(path)
        rd = side.get("run_diagnostics") or {}
        ogeo = (rd.get("origin") or {}).get("geo") or {}
        hdiag = side.get("host_diagnostics") or {}
        run_host = (rd.get("origin") or {}).get("hostname") or rd.get("hostname")
        try:
            with open(path, newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
        except Exception as e:  # noqa: BLE001
            print(f"  warning: could not read {path!r}: {e}", file=sys.stderr)
            continue
        for row in rows:
            host = row.get("host") or ""
            dgeo = (hdiag.get(host) or {}).get("dest_geo") or {}
            label = row.get("label") or ""
            status = row.get("status") or ""
            records.append({
                "origin_host": row.get("origin_hostname") or run_host or "?",
                "origin_city": row.get("origin_city") or ogeo.get("city") or "",
                "origin_country": (row.get("origin_country")
                                   or ogeo.get("country") or ""),
                "origin_isp": row.get("origin_isp") or ogeo.get("isp") or "",
                "origin_lat": (_to_float(row.get("origin_lat"))
                               if _to_float(row.get("origin_lat")) is not None
                               else _to_float(ogeo.get("latitude"))),
                "origin_lon": (_to_float(row.get("origin_lon"))
                               if _to_float(row.get("origin_lon")) is not None
                               else _to_float(ogeo.get("longitude"))),
                "local_ip": rd.get("local_ip"),
                "dest_host": host,
                "dest_city": row.get("dest_city") or dgeo.get("city") or "",
                "dest_country": (row.get("dest_country")
                                 or dgeo.get("country") or ""),
                "dest_lat": (_to_float(row.get("dest_lat"))
                             if _to_float(row.get("dest_lat")) is not None
                             else _to_float(dgeo.get("latitude"))),
                "dest_lon": (_to_float(row.get("dest_lon"))
                             if _to_float(row.get("dest_lon")) is not None
                             else _to_float(dgeo.get("longitude"))),
                "label": label,
                "node_label": label.rsplit("_", 1)[0] if "_" in label else label,
                "rate": _to_float(row.get("avg_rate_MBps")),
                "bytes": _to_float(row.get("downloaded_bytes")),
                "total_bytes": _to_float(row.get("total_bytes")),
                "size_tier": label.rsplit("_", 1)[-1] if "_" in label else "",
                "status": status,
                "ok": status in OK_STATUSES,
                "date": (row.get("started") or "")[:10],
                "source_file": os.path.basename(path),
            })
            for stamp, fix in RUN_OVERRIDES.items():
                if os.path.basename(path).startswith(stamp):
                    records[-1].update(fix)
    return records


def apply_size_match(records):
    """Count "failed" rows as completed where the byte count proves the whole
    file arrived (see SIZE_MATCH_IS_SUCCESS_HOSTS). Returns how many changed."""
    full = defaultdict(set)
    for r in records:
        if r["dest_host"] in SIZE_MATCH_IS_SUCCESS_HOSTS and r["ok"]:
            for b in (r["bytes"], r["total_bytes"]):
                if b:
                    full[(r["dest_host"], r["size_tier"])].add(b)
    changed = 0
    for r in records:
        if (r["dest_host"] in SIZE_MATCH_IS_SUCCESS_HOSTS and not r["ok"]
                and r["bytes"]
                and r["bytes"] in full[(r["dest_host"], r["size_tier"])]):
            r["ok"] = True
            r["status"] = "complete_size_match"
            changed += 1
    return changed


def resolve_records(records, cluster_km):
    """Turn raw rows into plottable downloads.

    Returns (downloads, nodes, sites, notes). Every download gets a "node" and
    a "site" key; nodes/sites are dicts keyed by that id holding the name and
    one canonical position each."""
    notes = []
    n_fixed = apply_size_match(records)
    if n_fixed:
        notes.append(f"{n_fixed} download(s) logged as failed counted as "
                     "completed: full file size received")

    # ---- nodes: one per hostname ----------------------------------------- #
    by_host = defaultdict(list)
    for r in records:
        if r["dest_host"]:
            by_host[r["dest_host"]].append(r)
    nodes = {}
    for host, rows in by_host.items():
        if host in NODE_OVERRIDES:
            nodes[host] = dict(NODE_OVERRIDES[host], host=host)
            continue
        located = [r for r in rows if r["dest_lat"] is not None
                   and r["dest_lon"] is not None]
        if not located:
            continue
        # Group the logged positions (geolocation can differ between runs)
        # and keep the group seen most often.
        groups = []
        for r in located:
            for g in groups:
                if haversine_km(r["dest_lat"], r["dest_lon"],
                                g[0]["dest_lat"], g[0]["dest_lon"]) <= 50:
                    g.append(r)
                    break
            else:
                groups.append([r])
        groups.sort(key=len, reverse=True)
        members = groups[0]
        name = _mode(r["node_label"] for r in rows) or host
        if name.upper().endswith("-GLOBUS"):
            name = name[:-len("-GLOBUS")]
        nodes[host] = {
            "name": name,
            "host": host,
            "lat": sum(r["dest_lat"] for r in members) / len(members),
            "lon": sum(r["dest_lon"] for r in members) / len(members),
            "city": _mode(r["dest_city"] for r in members),
            "country": _mode(r["dest_country"] for r in members),
        }
        if len(groups) > 1:
            where = ", ".join(f"{_mode(r['dest_city'] for r in g) or '?'} "
                              f"x{len(g)}" for g in groups)
            notes.append(
                f"node {name} ({host}) geolocates to different places between "
                f"runs ({where}); plotted at {nodes[host]['city']}")

    # ---- origins: inherit a site for runs with no geolocation ------------- #
    known = {}
    for r in records:
        if r["origin_lat"] is not None and r["origin_lon"] is not None:
            known.setdefault((r["origin_host"], r["local_ip"]), r)
    inferred_files = set()
    for r in records:
        if r["origin_lat"] is None or r["origin_lon"] is None:
            src = known.get((r["origin_host"], r["local_ip"]))
            if src and r["local_ip"]:
                for k in ("origin_lat", "origin_lon", "origin_city",
                          "origin_country", "origin_isp"):
                    r[k] = r[k] if r[k] not in (None, "") else src[k]
                inferred_files.add(r["source_file"])
    if inferred_files:
        notes.append(
            f"{len(inferred_files)} run(s) had no geolocation; client site "
            "inherited from another run with the same hostname + local IP")

    # ---- drop what still cannot be placed --------------------------------- #
    downloads, dropped = [], Counter()
    for r in records:
        if r["dest_host"] not in nodes:
            dropped[(r["source_file"], "no node position")] += 1
        elif r["origin_lat"] is None or r["origin_lon"] is None:
            dropped[(r["source_file"], "no client position")] += 1
        else:
            downloads.append(r)
    for (fname, why), n in sorted(dropped.items()):
        notes.append(f"skipped {n} row(s) from {fname}: {why}")

    # ---- client sites: cluster nearby machines ---------------------------- #
    spots = defaultdict(list)
    for r in downloads:
        spots[(round(r["origin_lat"], 2), round(r["origin_lon"], 2))].append(r)
    sites = {}
    for (la, lo), rows in sorted(spots.items(), key=lambda kv: -len(kv[1])):
        home = None
        for sid, s in sites.items():
            if haversine_km(la, lo, s["lat"], s["lon"]) <= cluster_km:
                home = sid
                break
        if home is None:
            home = f"site{len(sites)}"
            sites[home] = {
                "name": _mode(r["origin_city"] for r in rows) or
                _mode(r["origin_host"] for r in rows),
                "country": _mode(r["origin_country"] for r in rows),
                "lat": la, "lon": lo, "cities": set(),
            }
        sites[home]["cities"].update(r["origin_city"] for r in rows
                                     if r["origin_city"])
        for r in rows:
            r["site"] = home
    for s in sites.values():
        extra = sorted(s["cities"] - {s["name"]})
        if extra:
            notes.append(f"client site {s['name']} also includes "
                         f"{', '.join(extra)} (within {cluster_km:g} km)")
    for r in downloads:
        r["node"] = r["dest_host"]
    return downloads, nodes, sites, notes


def aggregate_flows(downloads, nodes, sites):
    """One flow per (client site, node). Mean speed is over completed
    downloads only; failures are counted separately."""
    groups = defaultdict(list)
    for r in downloads:
        groups[(r["site"], r["node"])].append(r)
    flows = []
    for (sid, nid), rows in groups.items():
        rates = [r["rate"] for r in rows if r["ok"] and r["rate"] is not None]
        flows.append({
            "site": sid, "node": nid,
            "site_name": sites[sid]["name"], "node_name": nodes[nid]["name"],
            "origin_lat": sites[sid]["lat"], "origin_lon": sites[sid]["lon"],
            "dest_lat": nodes[nid]["lat"], "dest_lon": nodes[nid]["lon"],
            "mean_rate": sum(rates) / len(rates) if rates else None,
            "min_rate": min(rates) if rates else None,
            "max_rate": max(rates) if rates else None,
            "n_ok": len(rates),
            "n_fail": len(rows) - len(rates),
            "count": len(rows),
            "downloads": rows,
        })
    return flows


def great_circle_points(lat1, lon1, lat2, lon2, n=100):
    """Interpolate points along the great-circle path (slerp on the sphere)."""
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


def speed_bounds(flows, cli_min, cli_max, scale):
    rates = [f["mean_rate"] for f in flows if f["mean_rate"] is not None]
    rates = rates or [1.0, 10.0]
    lo = cli_min if cli_min is not None else min(rates)
    hi = cli_max if cli_max is not None else max(rates)
    if scale == "log":
        lo = max(lo, 1e-3)
        steps = [m * 10 ** e for e in range(-4, 7) for m in (1, 2, 5)]
        if cli_min is None:
            lo = max(t for t in steps if t <= lo * 1.0001)
        if cli_max is None:
            hi = min(t for t in steps if t >= hi * 0.9999)
    if hi <= lo:
        hi = lo * 10 if scale == "log" else lo + 1
    return lo, hi


def speed_fraction(rate, vmin, vmax, scale):
    """Position of a speed on the colour scale, 0..1."""
    if scale == "log":
        f = (math.log10(max(rate, 1e-9)) - math.log10(vmin)) / \
            (math.log10(vmax) - math.log10(vmin))
    else:
        f = (rate - vmin) / (vmax - vmin)
    return max(0.0, min(1.0, f))


def log_ticks(vmin, vmax):
    """1-2-5 tick values spanning a log range."""
    ticks = []
    for e in range(int(math.floor(math.log10(vmin))) - 1,
                   int(math.ceil(math.log10(vmax))) + 1):
        for m in (1, 2, 5):
            t = m * 10 ** e
            if vmin * 0.999 <= t <= vmax * 1.001:
                ticks.append(t)
    return ticks


def fmt_speed(v):
    if v is None:
        return "-"
    if v >= 99.5:
        return f"{v:.0f}"
    if v >= 0.995:
        return f"{v:.1f}"
    return f"{v:.2f}"


def summary_line(downloads, nodes, sites):
    n_ok = sum(1 for r in downloads if r["ok"])
    n_fail = len(downloads) - n_ok
    dates = sorted(r["date"] for r in downloads if r["date"])
    span = ""
    if dates:
        span = dates[0] if dates[0] == dates[-1] else f"{dates[0]} to {dates[-1]}"
    def plural(n, word):
        return f"{n} {word}" + ("" if n == 1 else "s")

    parts = [plural(len(sites), "client site"), plural(len(nodes), "node"),
             plural(n_ok, "completed download")]
    if n_fail:
        parts.append(f"{n_fail} failed")
    if span:
        parts.append(span)
    return "  \u00b7  ".join(parts)


# --------------------------------------------------------------------------- #
# STATIC RENDERER (matplotlib + cartopy)
# --------------------------------------------------------------------------- #

def _draw_basemap_cartopy(ax, cfeature):
    """Tier 1: cartopy's built-in Natural Earth features (auto-download+cache).
    The fetch is deferred to draw time, so force it now to catch an offline
    failure here rather than at savefig."""
    try:
        list(cfeature.COASTLINE.geometries())
    except Exception:  # noqa: BLE001
        return False
    ax.add_feature(cfeature.LAND, facecolor=LAND_FC, edgecolor="none", zorder=0)
    ax.add_feature(cfeature.COASTLINE, linewidth=0.4, edgecolor=COAST_EC,
                   zorder=1)
    ax.add_feature(cfeature.BORDERS, linewidth=0.25, edgecolor=BORDER_EC,
                   zorder=1)
    return True


def _draw_basemap_local(ax, ccrs, basemap_dir):
    """Tier 2: Natural Earth shapefiles from a local directory (offline)."""
    if not basemap_dir or not os.path.isdir(basemap_dir):
        return False
    try:
        from cartopy.io.shapereader import Reader
        from cartopy.feature import ShapelyFeature
    except Exception:  # noqa: BLE001
        return False

    def _find(*patterns):
        for pat in patterns:
            hits = glob.glob(os.path.join(basemap_dir, pat))
            if hits:
                return hits[0]
        return None

    land = _find("*land.shp")
    coast = _find("*coastline.shp")
    borders = _find("*boundary*.shp", "*border*.shp")
    drew = False
    pc = ccrs.PlateCarree()
    try:
        if land:
            ax.add_feature(ShapelyFeature(Reader(land).geometries(), pc,
                                          facecolor=LAND_FC, edgecolor="none"),
                           zorder=0)
            drew = True
        if coast:
            ax.add_feature(ShapelyFeature(Reader(coast).geometries(), pc,
                                          facecolor="none", edgecolor=COAST_EC,
                                          linewidth=0.4), zorder=1)
            drew = True
        if borders:
            ax.add_feature(ShapelyFeature(Reader(borders).geometries(), pc,
                                          facecolor="none", edgecolor=BORDER_EC,
                                          linewidth=0.25), zorder=1)
    except Exception:  # noqa: BLE001
        return drew
    return drew


def _place_labels(fig, ax, items, marker_px):
    """Place text labels around their anchor points without overlapping each
    other or any marker. items: dicts with x, y (display px), text and style.
    Greedy search over rings of candidate positions; a label pushed beyond the
    first ring gets a thin leader line back to its point."""
    import matplotlib.patheffects as pe

    renderer = fig.canvas.get_renderer()
    px = fig.dpi / 72.0
    axbox = ax.get_window_extent(renderer)
    placed = []
    inv = ax.transData.inverted()
    angles = [90, 270, 0, 180, 45, 135, 315, 225, 68, 112, 22, 158,
              338, 202, 292, 248]
    radii = [8, 15, 24, 34, 46, 60]

    def overlap(a, b, pad=0.0):
        w = min(a[2], b[2]) - max(a[0], b[0]) + pad
        h = min(a[3], b[3]) - max(a[1], b[1]) + pad
        return w * h if (w > 0 and h > 0) else 0.0

    def own_dist(box, x, y):
        cx, cy = (box[0] + box[2]) / 2, (box[1] + box[3]) / 2
        return math.hypot(cx - x, cy - y)

    for it in items:
        # Direction pointing away from nearby markers, if there are any.
        near = [(mx, my) for (mx, my) in marker_px
                if (mx, my) != (it["x"], it["y"])
                and math.hypot(mx - it["x"], my - it["y"]) < 70 * px]
        away = None
        if near:
            gx = sum(m[0] for m in near) / len(near)
            gy = sum(m[1] for m in near) / len(near)
            if math.hypot(it["x"] - gx, it["y"] - gy) > 1e-6:
                away = math.atan2(it["y"] - gy, it["x"] - gx)
        probe = ax.text(0, 0, it["text"], fontsize=it["size"],
                        fontweight=it["weight"], fontstyle=it["style"])
        ext = probe.get_window_extent(renderer)
        tw, th = ext.width, ext.height
        probe.remove()

        best = None
        for ri, rad in enumerate(radii):
            for ang in angles:
                c, s = math.cos(math.radians(ang)), math.sin(math.radians(ang))
                ax_, ay_ = it["x"] + rad * px * c, it["y"] + rad * px * s
                ha = "left" if c > 0.3 else "right" if c < -0.3 else "center"
                va = "bottom" if s > 0.3 else "top" if s < -0.3 else "center"
                x0 = ax_ if ha == "left" else ax_ - tw if ha == "right" \
                    else ax_ - tw / 2
                y0 = ay_ if va == "bottom" else ay_ - th if va == "top" \
                    else ay_ - th / 2
                box = (x0, y0, x0 + tw, y0 + th)
                cost = sum(overlap(box, p, pad=3 * px) for p in placed)
                mine = own_dist(box, it["x"], it["y"])
                for (mx, my) in marker_px:
                    if (mx, my) == (it["x"], it["y"]):
                        continue
                    r = 6 * px
                    cost += 4 * overlap(box, (mx - r, my - r, mx + r, my + r))
                    if own_dist(box, mx, my) < 1.5 * mine:
                        cost += 400.0  # would read as the other point's label
                if away is not None:
                    cost += 0.4 * (1 - math.cos(math.radians(ang) - away))
                if (box[0] < axbox.x0 or box[2] > axbox.x1 or
                        box[1] < axbox.y0 or box[3] > axbox.y1):
                    cost += 1e6
                cost += ri * 0.01  # prefer staying close when all else equal
                if best is None or cost < best[0]:
                    best = (cost, ax_, ay_, ha, va, box, ri)
            if best is not None and best[0] < 1.0:
                break

        _, tx, ty, ha, va, box, ri = best
        placed.append(box)
        halo = [pe.withStroke(linewidth=2.2, foreground="white", alpha=0.9)]
        xy = inv.transform((it["x"], it["y"]))
        xytext = inv.transform((tx, ty))
        ax.annotate(
            it["text"], xy=xy, xytext=xytext, xycoords="data",
            textcoords="data", ha=ha, va=va, fontsize=it["size"],
            fontweight=it["weight"], fontstyle=it["style"], color=INK,
            zorder=8, path_effects=halo,
            arrowprops=(dict(arrowstyle="-", color="#555555", linewidth=0.5,
                             shrinkA=1, shrinkB=3) if ri >= 1 else None))


def _draw_matrix(ax, flows, nodes, sites, cmap, norm):
    """Client-site x node table: mean speed and attempt counts per route."""
    from matplotlib.patches import Rectangle

    site_ids = sorted(sites, key=lambda s: sites[s]["lon"])
    node_ids = sorted(nodes, key=lambda n: nodes[n]["lon"])
    by_key = {(f["site"], f["node"]): f for f in flows}

    ax.set_xlim(0, len(node_ids))
    ax.set_ylim(len(site_ids), 0)
    for i, sid in enumerate(site_ids):
        for j, nid in enumerate(node_ids):
            fl = by_key.get((sid, nid))
            if fl is None:
                ax.add_patch(Rectangle((j, i), 1, 1, facecolor="#FAFAF8",
                                       edgecolor="white", linewidth=1.5))
                ax.text(j + 0.5, i + 0.5, "\u2013", ha="center", va="center",
                        fontsize=8, color="#BBBBBB")
                continue
            if fl["mean_rate"] is None:
                ax.add_patch(Rectangle((j, i), 1, 1, facecolor="#EFEFEF",
                                       edgecolor="white", linewidth=1.5,
                                       hatch="////"))
                ax.text(j + 0.5, i + 0.40, "failed", ha="center", va="center",
                        fontsize=8.5, color="#555555", fontweight="bold")
                ax.text(j + 0.5, i + 0.74, f"0 of {fl['count']}", ha="center",
                        va="center", fontsize=6.5, color="#555555")
                continue
            rgba = cmap(norm(fl["mean_rate"]))
            lum = 0.2126 * rgba[0] + 0.7152 * rgba[1] + 0.0722 * rgba[2]
            tc = "white" if lum < 0.5 else INK
            ax.add_patch(Rectangle((j, i), 1, 1, facecolor=rgba,
                                   edgecolor="white", linewidth=1.5))
            ax.text(j + 0.5, i + 0.40, fmt_speed(fl["mean_rate"]), ha="center",
                    va="center", fontsize=10, color=tc, fontweight="bold")
            sub = (f"n={fl['count']}" if not fl["n_fail"]
                   else f"{fl['n_ok']} of {fl['count']} ok")
            ax.text(j + 0.5, i + 0.74, sub, ha="center", va="center",
                    fontsize=6.5, color=tc)

    ax.set_xticks([j + 0.5 for j in range(len(node_ids))])
    ax.set_xticklabels([nodes[n]["name"] for n in node_ids], fontsize=8.5,
                       fontweight="bold")
    ax.xaxis.tick_top()
    ax.set_yticks([i + 0.5 for i in range(len(site_ids))])
    ax.set_yticklabels(
        [f"{sites[s]['name']}, {sites[s]['country']}" if sites[s]["country"]
         else sites[s]["name"] for s in site_ids], fontsize=8.5)
    ax.tick_params(length=0, pad=5)
    for sp in ax.spines.values():
        sp.set_visible(False)
    ax.set_xlabel("ESGF node (source), west to east", fontsize=8, labelpad=8,
                  color="#555555")
    ax.xaxis.set_label_position("top")
    if len(site_ids) >= 3:  # the label needs that much height
        ax.set_ylabel("Client site (sink)", fontsize=8, color="#555555")


def render_static(flows, nodes, sites, subtitle, out_path, vmin, vmax, scale,
                  title, fmt, basemap_dir=None, table=True):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.cm as cm
    import matplotlib.colors as mcolors
    import matplotlib.patheffects as pe
    import matplotlib.ticker as mticker
    from matplotlib.lines import Line2D
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature

    # Colour scale: viridis with the palest yellow trimmed off so the fastest
    # routes stay legible on a light map.
    cmap = mcolors.LinearSegmentedColormap.from_list(
        "viridis_trim", matplotlib.colormaps["viridis"](
            [i / 255 * 0.93 for i in range(256)]))
    norm = (mcolors.LogNorm(vmin=vmin, vmax=vmax) if scale == "log"
            else mcolors.Normalize(vmin=vmin, vmax=vmax))

    proj = ccrs.Robinson()
    lat_s, lat_n = -58, 84  # trims Antarctica, which carries no data
    pc0 = ccrs.PlateCarree()
    x_half = proj.transform_point(180, 0, pc0)[0]
    y_s = proj.transform_point(0, lat_s, pc0)[1]
    y_n = proj.transform_point(0, lat_n, pc0)[1]

    n_rows = len(sites)
    fig_w = 13.0
    map_w = 0.96 * fig_w
    map_h = map_w * (y_n - y_s) / (2 * x_half)
    rows_h = 0.52 * n_rows
    body_h = max(rows_h, 2.2)  # keeps the colourbar readable for few rows
    table_h = (1.0 + body_h) if table else 0.75
    fig_h = 0.8 + map_h + table_h
    fig = plt.figure(figsize=(fig_w, fig_h), dpi=200)

    map_bottom = table_h / fig_h
    ax = fig.add_axes([0.02, map_bottom, 0.96, map_h / fig_h],
                      projection=proj)
    ax.patch.set_facecolor(OCEAN_FC)

    if not (_draw_basemap_cartopy(ax, cfeature)
            or _draw_basemap_local(ax, ccrs, basemap_dir)):
        print("  note: no basemap data (no internet and no local shapefiles); "
              "drawing a plain graticule. Provide Natural Earth shapefiles "
              "via --basemap-dir, or run once online so cartopy caches them.",
              file=sys.stderr)
        ax.gridlines(draw_labels=False, linewidth=0.3, color=BORDER_EC)

    geo = ccrs.Geodetic()
    casing = [pe.Stroke(linewidth=2.6, foreground=OCEAN_FC, alpha=0.85),
              pe.Normal()]

    # Routes where nothing completed sit underneath, dashed grey.
    for fl in [f for f in flows if f["mean_rate"] is None]:
        ax.plot([fl["dest_lon"], fl["origin_lon"]],
                [fl["dest_lat"], fl["origin_lat"]], color=FAIL_GREY,
                linewidth=0.9, linestyle=(0, (4, 3)), alpha=0.9,
                transform=geo, zorder=2)
    # Then slowest to fastest, each arc cased so crossings stay readable.
    for fl in sorted([f for f in flows if f["mean_rate"] is not None],
                     key=lambda f: f["mean_rate"]):
        ax.plot([fl["dest_lon"], fl["origin_lon"]],
                [fl["dest_lat"], fl["origin_lat"]],
                color=cmap(norm(fl["mean_rate"])), linewidth=1.5,
                transform=geo, zorder=3, solid_capstyle="round",
                path_effects=casing)

    for n in nodes.values():
        ax.scatter([n["lon"]], [n["lat"]], marker="s", s=50, c=INK,
                   edgecolors="white", linewidths=0.7, transform=geo, zorder=5)
    for s in sites.values():
        # Smaller than the node square, so both show where a client sits on
        # top of a node (e.g. a login node at the data centre itself).
        ax.scatter([s["lon"]], [s["lat"]], marker="o", s=24, c="white",
                   edgecolors=INK, linewidths=1.1, transform=geo, zorder=6)

    # Fix the view only after plotting (autoscale would otherwise shrink it).
    ax.set_xlim(-x_half, x_half)
    ax.set_ylim(y_s, y_n)
    for sp in ax.spines.values():
        sp.set_edgecolor("#999999")
        sp.set_linewidth(0.6)

    # Labels, placed once the layout is final.
    fig.canvas.draw()
    pc = ccrs.PlateCarree()

    def to_px(lon, lat):
        x, y = ax.projection.transform_point(lon, lat, pc)
        return tuple(ax.transData.transform((x, y)))

    items, marker_px = [], []
    for n in sorted(nodes.values(), key=lambda n: -n["lat"]):
        x, y = to_px(n["lon"], n["lat"])
        marker_px.append((x, y))
        items.append(dict(x=x, y=y, text=n["name"], size=7.5, weight="bold",
                          style="normal"))
    for s in sorted(sites.values(), key=lambda s: -s["lat"]):
        x, y = to_px(s["lon"], s["lat"])
        marker_px.append((x, y))
        items.append(dict(x=x, y=y, text=s["name"], size=7.5, weight="normal",
                          style="italic"))
    _place_labels(fig, ax, items, marker_px)

    handles = [
        Line2D([0], [0], marker="s", color="none", markerfacecolor=INK,
               markeredgecolor="white", markersize=7.5,
               label="ESGF node (source)"),
        Line2D([0], [0], marker="o", color="none", markerfacecolor="white",
               markeredgecolor=INK, markersize=5.5, label="Client site (sink)"),
    ]
    if any(f["mean_rate"] is None for f in flows):
        handles.append(Line2D([0], [0], color=FAIL_GREY, linewidth=0.9,
                              linestyle=(0, (4, 3)),
                              label="All attempts failed"))
    ax.legend(handles=handles, loc="lower left", fontsize=7.5, frameon=True,
              facecolor="white", edgecolor="#CCCCCC", framealpha=0.95)

    fig.text(0.5, 1 - 0.30 / fig_h,
             title or "ESGF download performance: node \u2192 client",
             ha="center", va="center", fontsize=14, fontweight="bold",
             color=INK)
    fig.text(0.5, 1 - 0.58 / fig_h, subtitle, ha="center", va="center",
             fontsize=9, color="#555555")

    sm = cm.ScalarMappable(norm=norm, cmap=cmap)
    sm.set_array([])
    cbar_label = "Mean sustained download speed (MB/s)"
    if table:
        tw = min(0.71, 0.11 * len(nodes))
        tax = fig.add_axes([0.17, (0.25 + body_h - rows_h) / fig_h, tw,
                            rows_h / fig_h])
        _draw_matrix(tax, flows, nodes, sites, cmap, norm)
        cax = fig.add_axes([0.17 + tw + 0.025, 0.25 / fig_h, 0.012,
                            body_h / fig_h])
        cbar = fig.colorbar(sm, cax=cax)
    else:
        cax = fig.add_axes([0.3, 0.42 / fig_h, 0.4, 0.15 / fig_h])
        cbar = fig.colorbar(sm, cax=cax, orientation="horizontal")
    if scale == "log":
        cbar.set_ticks(log_ticks(vmin, vmax))
        cbar.ax.minorticks_off()
    cbar.formatter = mticker.FuncFormatter(lambda v, _: f"{v:g}")
    cbar.update_ticks()
    cbar.set_label(cbar_label, fontsize=8)
    cbar.ax.tick_params(labelsize=7.5)
    cbar.outline.set_linewidth(0.5)

    fig.savefig(out_path, format=fmt, facecolor="white")
    plt.close(fig)
    return out_path


# --------------------------------------------------------------------------- #
# INTERACTIVE RENDERER (plotly)
# --------------------------------------------------------------------------- #

def _route_hover(fl, nodes, sites, max_lines=12):
    """HTML hover text for one route: endpoints, mean, and each download."""
    node, site = nodes[fl["node"]], sites[fl["site"]]
    rows = sorted(fl["downloads"], key=lambda d: (d["date"], d["label"]))
    lines = []
    for d in rows[:max_lines]:
        size = d["label"].rsplit("_", 1)[-1] if "_" in d["label"] else ""
        speed = (f"{d['rate']:.2f} MB/s" if d["rate"] is not None
                 else "no data")
        flag = "" if d["ok"] else " <b>failed</b>"
        who = " \u00b7 ".join(x for x in (d["origin_host"], d["origin_city"],
                                            d["origin_isp"]) if x)
        lines.append(f"{d['date']}  {size}  {speed}{flag}  "
                     f"<span style='color:#777'>{who}</span>")
    if len(rows) > max_lines:
        lines.append(f"<i>\u2026 and {len(rows) - max_lines} more</i>")
    if fl["mean_rate"] is not None:
        head = f"<b>Mean {fmt_speed(fl['mean_rate'])} MB/s</b>"
        if fl["n_ok"] > 1:
            head += (f"  (range {fmt_speed(fl['min_rate'])} to "
                     f"{fmt_speed(fl['max_rate'])})")
    else:
        head = "<b>All attempts failed</b>"
    tally = f"{fl['n_ok']} completed"
    if fl["n_fail"]:
        tally += f", {fl['n_fail']} failed"
    return (f"<b>{node['name']} \u2192 {site['name']}</b><br>"
            f"{node['host']} ({node['city']}, {node['country']})<br>"
            f"to {site['name']}, {site['country']}<br><br>"
            f"{head}<br>{tally}:<br>" + "<br>".join(lines))


def _label_anchors(points, px_per_deg=3.1):
    """Place labels for the interactive map so they avoid each other and the
    markers at the default zoom. plotly cannot draw leader lines or offset a
    label freely, so a label that will not fit beside its marker is anchored
    at a nearby geographic position and joined to the marker by a thin line.

    points: dicts with lon, lat, text, kind ('node' or 'site').
    Returns one dict per point: lon, lat (text anchor), textposition, and
    leader (True when the anchor is away from the marker)."""
    def shrink(lat):  # natural-earth narrows away from the equator
        return 1 - 0.28 * (lat / 90.0) ** 2

    def xy(lon, lat):
        return lon * px_per_deg * shrink(lat), lat * px_per_deg

    def overlap(a, b, pad=0.0):
        w = min(a[2], b[2]) - max(a[0], b[0]) + pad
        h = min(a[3], b[3]) - max(a[1], b[1]) + pad
        return w * h if (w > 0 and h > 0) else 0.0

    marks = [xy(p["lon"], p["lat"]) for p in points]
    order = sorted(range(len(points)), key=lambda i: -points[i]["lat"])
    rings = [(0, list(range(0, 360, 45)))] + \
            [(r, [a * 22.5 for a in range(16)]) for r in (16, 26, 38, 52, 68)]
    placed, out = [], {}
    for i in order:
        x, y = marks[i]
        bold = points[i]["kind"] == "node"
        tw = (7.2 if bold else 6.4) * len(points[i]["text"]) + 2
        th = 14.0
        near = [m for j, m in enumerate(marks) if j != i
                and math.hypot(m[0] - x, m[1] - y) < 70]
        away = None
        if near:
            gx = sum(m[0] for m in near) / len(near)
            gy = sum(m[1] for m in near) / len(near)
            if math.hypot(x - gx, y - gy) > 1e-6:
                away = math.atan2(y - gy, x - gx)
        first = 90 if bold else 270  # nodes prefer above, sites below
        best = None
        for ri, (rad, angles) in enumerate(rings):
            for ang in angles:
                c, sn = math.cos(math.radians(ang)), math.sin(math.radians(ang))
                hx = 1 if c > 0.3 else -1 if c < -0.3 else 0
                vy = 1 if sn > 0.3 else -1 if sn < -0.3 else 0
                # Even a label beside its marker is anchored just clear of
                # the marker glyph: a text-only point has no marker offset.
                eff = rad if rad else 8
                ax_, ay_ = x + eff * c, y + eff * sn
                gap = 3
                cx = ax_ + hx * (gap + tw / 2)
                cy = ay_ + vy * (gap + th / 2)
                box = (cx - tw / 2, cy - th / 2, cx + tw / 2, cy + th / 2)
                cost = ri * 0.02 + sum(overlap(box, q, pad=3) for q in placed)
                cost += 0.004 * abs((ang - first + 180) % 360 - 180) / 45
                mine = math.hypot(cx - x, cy - y)
                for j, (mx, my) in enumerate(marks):
                    if j == i:
                        continue
                    cost += 4 * overlap(box, (mx - 8, my - 8, mx + 8, my + 8))
                    if rad == 0 and math.hypot(cx - mx, cy - my) < 1.5 * mine:
                        cost += 300.0  # would read as the other point's label
                if away is not None:
                    cost += 0.3 * (1 - math.cos(math.radians(ang) - away))
                if best is None or cost < best[0]:
                    best = (cost, rad, c, sn, hx, vy, box, eff)
            if best[0] < 1.0:
                break
        _, rad, c, sn, hx, vy, box, eff = best
        placed.append(box)
        lat = points[i]["lat"] + eff * sn / px_per_deg
        lon = points[i]["lon"] + eff * c / (px_per_deg * shrink(lat))
        vert = {1: "top", 0: "middle", -1: "bottom"}[vy]
        horiz = {1: "right", 0: "center", -1: "left"}[hx]
        out[i] = dict(lon=lon, lat=lat, leader=rad > 0,
                      textposition=("middle right" if (vy == 0 and hx == 0)
                                    else f"{vert} {horiz}"))
    return [out[i] for i in range(len(points))]


def render_interactive(flows, nodes, sites, subtitle, out_path, vmin, vmax,
                       scale, title):
    import plotly.graph_objects as go
    import plotly.colors as pcolors
    from plotly.subplots import make_subplots

    font = "Inter, 'Helvetica Neue', Helvetica, Arial, sans-serif"
    viridis_trim = [[i / 20, pcolors.sample_colorscale(
        "Viridis", [i / 20 * 0.93])[0]] for i in range(21)]

    def color_for(rate):
        frac = speed_fraction(rate, vmin, vmax, scale) * 0.93
        return pcolors.sample_colorscale("Viridis", [frac])[0]

    def zval(rate):
        return math.log10(rate) if scale == "log" else rate

    site_ids = sorted(sites, key=lambda k: sites[k]["lon"])
    node_ids = sorted(nodes, key=lambda k: nodes[k]["lon"])
    row_px = 58
    map_px, table_px = 560, row_px * len(site_ids)
    height = 110 + map_px + 90 + table_px + 30
    fig = make_subplots(
        rows=2, cols=1, specs=[[{"type": "geo"}], [{"type": "xy"}]],
        row_heights=[map_px, table_px], vertical_spacing=90 / height)

    # ---- map: arcs -------------------------------------------------------- #
    ordered = ([f for f in flows if f["mean_rate"] is None] +
               sorted([f for f in flows if f["mean_rate"] is not None],
                      key=lambda f: f["mean_rate"]))
    failed_legend_done = False
    for fl in ordered:
        lats, lons = great_circle_points(
            fl["dest_lat"], fl["dest_lon"],
            fl["origin_lat"], fl["origin_lon"], n=80)
        hover = _route_hover(fl, nodes, sites)
        if fl["mean_rate"] is None:
            fig.add_trace(go.Scattergeo(
                lat=lats, lon=lons, mode="lines",
                line=dict(width=1.3, color=FAIL_GREY, dash="dash"),
                hoverinfo="text", text=hover, name="All attempts failed",
                legendgroup="failed", showlegend=not failed_legend_done),
                row=1, col=1)
            failed_legend_done = True
        else:
            fig.add_trace(go.Scattergeo(
                lat=lats, lon=lons, mode="lines",
                line=dict(width=2.2, color=color_for(fl["mean_rate"])),
                opacity=0.92, hoverinfo="text", text=hover,
                showlegend=False), row=1, col=1)

    # ---- map: markers and labels ------------------------------------------ #
    nl, sl = [nodes[k] for k in node_ids], [sites[k] for k in site_ids]
    pts = ([dict(lon=n["lon"], lat=n["lat"], text=n["name"], kind="node")
            for n in nl] +
           [dict(lon=s["lon"], lat=s["lat"], text=s["name"], kind="site")
            for s in sl])
    anchors = _label_anchors(pts)
    lead_lat, lead_lon = [], []
    for pt, an in zip(pts, anchors):
        if an["leader"]:
            lead_lat += [pt["lat"], an["lat"], None]
            lead_lon += [pt["lon"], an["lon"], None]
    if lead_lat:
        fig.add_trace(go.Scattergeo(
            lat=lead_lat, lon=lead_lon, mode="lines",
            line=dict(width=0.8, color="#555555"), hoverinfo="skip",
            showlegend=False), row=1, col=1)
    fig.add_trace(go.Scattergeo(
        lat=[n["lat"] for n in nl], lon=[n["lon"] for n in nl],
        mode="markers",
        marker=dict(symbol="square", size=10, color=INK,
                    line=dict(width=1, color="white")),
        hovertext=[f"<b>{n['name']}</b>  ESGF node<br>{n['host']}<br>"
                   f"{n['city']}, {n['country']}" for n in nl],
        hoverinfo="text", name="ESGF node (source)"), row=1, col=1)
    fig.add_trace(go.Scattergeo(
        lat=[s["lat"] for s in sl], lon=[s["lon"] for s in sl],
        mode="markers",
        marker=dict(symbol="circle", size=7, color="white",
                    line=dict(width=1.5, color=INK)),
        hovertext=[f"<b>{s['name']}</b>, {s['country']}  client site"
                   for s in sl],
        hoverinfo="text", name="Client site (sink)"), row=1, col=1)
    fig.add_trace(go.Scattergeo(
        lat=[an["lat"] for an in anchors], lon=[an["lon"] for an in anchors],
        mode="text",
        text=[f"<b>{pt['text']}</b>" if pt["kind"] == "node"
              else f"<i>{pt['text']}</i>" for pt in pts],
        textposition=[an["textposition"] for an in anchors],
        textfont=dict(size=11, color=INK, family=font),
        hoverinfo="skip", showlegend=False), row=1, col=1)

    # ---- route table ------------------------------------------------------ #
    by_key = {(f["site"], f["node"]): f for f in flows}
    xs = [nodes[k]["name"] for k in node_ids]
    ys = [f"{sites[k]['name']}, {sites[k]['country']}" if sites[k]["country"]
          else sites[k]["name"] for k in site_ids]
    z_ok, t_ok, h_ok, z_bad, t_bad, h_bad = [], [], [], [], [], []
    for sid in site_ids:
        zr, tr, hr, zb, tb, hb = [], [], [], [], [], []
        for nid in node_ids:
            fl = by_key.get((sid, nid))
            ok = fl is not None and fl["mean_rate"] is not None
            bad = fl is not None and fl["mean_rate"] is None
            zr.append(zval(fl["mean_rate"]) if ok else None)
            tr.append((f"<b>{fmt_speed(fl['mean_rate'])}</b><br>" +
                       (f"n={fl['count']}" if not fl["n_fail"] else
                        f"{fl['n_ok']} of {fl['count']} ok")) if ok else "")
            hr.append(_route_hover(fl, nodes, sites) if ok else "")
            zb.append(1 if bad else None)
            tb.append(f"<b>failed</b><br>0 of {fl['count']}" if bad else "")
            hb.append(_route_hover(fl, nodes, sites) if bad else "")
        z_ok.append(zr); t_ok.append(tr); h_ok.append(hr)
        z_bad.append(zb); t_bad.append(tb); h_bad.append(hb)

    if scale == "log":
        ticks = log_ticks(vmin, vmax)
        cb_ticks = dict(tickvals=[math.log10(t) for t in ticks],
                        ticktext=[f"{t:g}" for t in ticks])
    else:
        cb_ticks = {}
    table_dom = fig.layout.yaxis.domain
    fig.add_trace(go.Heatmap(
        z=z_ok, x=xs, y=ys, text=t_ok, texttemplate="%{text}",
        textfont=dict(size=12, family=font), customdata=h_ok,
        hovertemplate="%{customdata}<extra></extra>", hoverongaps=False,
        colorscale=viridis_trim, zmin=zval(vmin), zmax=zval(vmax),
        xgap=3, ygap=3,
        # A short title on top: a long vertical one makes plotly reserve a
        # very wide right margin, which shrinks the map.
        colorbar=dict(title=dict(text="MB/s", side="top",
                                 font=dict(size=12)),
                      thickness=14, outlinewidth=0, x=1.005, xanchor="left",
                      y=(table_dom[0] + table_dom[1]) / 2,
                      len=table_dom[1] - table_dom[0], **cb_ticks)),
        row=2, col=1)
    fig.add_trace(go.Heatmap(
        z=z_bad, x=xs, y=ys, text=t_bad, texttemplate="%{text}",
        textfont=dict(size=11, family=font, color="#555555"),
        customdata=h_bad, hovertemplate="%{customdata}<extra></extra>",
        hoverongaps=False, colorscale=[[0, "#E3E3E3"], [1, "#E3E3E3"]],
        showscale=False, xgap=3, ygap=3), row=2, col=1)

    # ---- layout ----------------------------------------------------------- #
    ttl = title or "ESGF download performance: node \u2192 client"
    fig.update_geos(
        projection_type="natural earth", projection_rotation_lon=0,
        lonaxis_range=[-180, 180], lataxis_range=[-58, 84],
        showland=True, landcolor=LAND_FC, showocean=True, oceancolor=OCEAN_FC,
        showlakes=False, showcountries=True, countrycolor=BORDER_EC,
        countrywidth=0.5, coastlinecolor=COAST_EC, coastlinewidth=0.6,
        showframe=True, framecolor="#BBBBBB", framewidth=0.8,
        bgcolor="white")
    fig.update_xaxes(side="top", showgrid=False, zeroline=False,
                     tickfont=dict(size=12, color=INK, family=font),
                     ticks="", fixedrange=True, row=2, col=1)
    fig.update_yaxes(autorange="reversed", showgrid=False, zeroline=False,
                     tickfont=dict(size=12, color=INK, family=font),
                     ticks="", ticksuffix="  ", fixedrange=True, row=2, col=1)
    fig.add_annotation(
        text="Route table: mean sustained speed in MB/s and number of "
             "download attempts. Hover a cell or an arc for every download.",
        xref="paper", yref="paper", x=0, xanchor="left",
        y=table_dom[1] + 52 / height, yanchor="bottom", showarrow=False,
        font=dict(size=12, color="#666666", family=font))
    fig.update_layout(
        height=height, autosize=True,
        title=dict(text=f"<b>{ttl}</b><br><span style='font-size:13px;"
                        f"color:#666666'>{subtitle}</span>",
                   font=dict(size=20, color=INK, family=font),
                   x=0.5, xanchor="center", y=1 - 34 / height, yanchor="top"),
        font=dict(family=font, color=INK),
        legend=dict(orientation="h", x=0.5, xanchor="center",
                    y=1.0, yanchor="bottom",
                    font=dict(size=12), bgcolor="rgba(255,255,255,0)"),
        hoverlabel=dict(bgcolor="white", bordercolor="#BBBBBB", align="left",
                        font=dict(size=12, color=INK, family=font)),
        margin=dict(l=20, r=90, t=110, b=30),
        paper_bgcolor="white", plot_bgcolor="#FAFAF8")
    fig.write_html(
        out_path, include_plotlyjs="cdn",
        config=dict(displaylogo=False, scrollZoom=True, responsive=True,
                    modeBarButtonsToRemove=["select2d", "lasso2d"],
                    toImageButtonOptions=dict(filename="esgf_flowmap",
                                              scale=2)))
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
    p.add_argument("--scale", choices=["log", "linear"], default="log",
                   help="colour scale for speed (default: log)")
    p.add_argument("--min-speed", type=float, default=None)
    p.add_argument("--max-speed", type=float, default=None)
    p.add_argument("--cluster-km", type=float, default=100.0,
                   help="merge client machines closer than this into one "
                   "site (default: 100)")
    p.add_argument("--no-table", action="store_true",
                   help="static image: draw the map only, no route matrix")
    p.add_argument("--title", default=None)
    p.add_argument("--basemap-dir", default=None,
                   help="directory of Natural Earth shapefiles (ne_110m land, "
                   "coastline, boundary lines) for machines where cartopy "
                   "cannot download them")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv if argv is not None else sys.argv[1:])

    files = find_log_files(args.paths)
    if not files:
        print("No *_esgf_results*.csv logs found.", file=sys.stderr)
        return 2
    print(f"Reading {len(files)} log file(s) ...")

    records = load_downloads(files)
    downloads, nodes, sites, notes = resolve_records(records, args.cluster_km)
    for note in notes:
        print(f"  note: {note}")
    if not downloads:
        print("No plottable downloads: the logs need client and node "
              "coordinates (origin_lat/lon, dest_lat/lon).", file=sys.stderr)
        return 2

    flows = aggregate_flows(downloads, nodes, sites)
    vmin, vmax = speed_bounds(flows, args.min_speed, args.max_speed,
                              args.scale)
    subtitle = summary_line(downloads, nodes, sites)
    print(f"  {len(downloads)} downloads on {len(flows)} routes; "
          f"route means {fmt_speed(vmin)} to {fmt_speed(vmax)} MB/s")

    if not args.interactive_only:
        static_path = f"{args.out_prefix}.{args.format}"
        render_static(flows, nodes, sites, subtitle, static_path, vmin, vmax,
                      args.scale, args.title, args.format,
                      basemap_dir=args.basemap_dir, table=not args.no_table)
        print(f"  wrote {static_path}")
    if not args.static_only:
        html_path = f"{args.out_prefix}.html"
        render_interactive(flows, nodes, sites, subtitle, html_path, vmin,
                           vmax, args.scale, args.title)
        print(f"  wrote {html_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
