#!/usr/bin/env python3
"""
esgf_download_test.py

Test CMIP6 file downloads across multiple ESGF data nodes, logging per-phase
connection timing, throughput, stability and integrity (SHA256).

Design notes
------------
* Standard library only (urllib/http.client/socket/ssl/hashlib). No pip installs.
* Cross-platform (Linux/WSL2, macOS, Windows).
* Direct HTTPS, anonymous only. If a target redirects to a login page, the
  attempt is logged as "auth_required" and skipped.
* Resilient: downloads resume via HTTP Range requests after any interruption,
  with exponential backoff, retrying until the file completes or the per-file
  attempt budget is exhausted. Resume count is logged.
* Integrity: streaming SHA256 computed during download and compared to the
  expected checksum (from ESGF metadata) when provided. File is deleted after
  verification (download -> verify -> discard -> next).
* Output: one JSON file (full detail) and one CSV (flat summary) per run.

PJD 2 Jul 2026 - flipped {stamp} in log files - get these to sequentially order
                 correctly in a directory listing.
PJD 2 Jul 2026 - reorganised timestamping.
PJD 3 Jul 2026 - cleanup ANL vs NERSC globus endpoints.
PJD 4 Jul 2026 - added NCI downloads.

Edit the TARGETS list below with the files/nodes you want to test.
"""

import argparse
import csv
import getpass
import hashlib
import http.client
import json
import os
import platform
import re
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.request
from datetime import datetime, timezone
from urllib.parse import urlsplit

# --------------------------------------------------------------------------- #
# CONFIGURATION
# --------------------------------------------------------------------------- #

# Each target: a label (node + size hint) and the direct HTTPS URL.
# Optionally add "sha256" (from the ESGF metadata) to enable integrity checks.
# Fill these in with the actual replica URLs you resolve from the ESGF search
# API for your ~1 / ~2 / ~4 GB files on each node.
TARGETS = [
    # CEDA
    {
        "label": "CEDA_1GB",
        "url": "https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p2f1/Omon/thetao/gn/v20190429/thetao_Omon_CanESM5_historical_r1i1p2f1_gn_185001-186012.nc",
        "sha256": "fa7c3a0a6cbe4aec8805fda48948972a0bc42aec4fdd065ff3c5b61763522ea6",
    },
    {
        "label": "CEDA_2GB",
        "url": "https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/CMIP/MOHC/UKESM1-0-LL/historical/r1i1p1f2/Omon/thetao/gn/v20190627/thetao_Omon_UKESM1-0-LL_historical_r1i1p1f2_gn_200001-201412.nc",
        "sha256": "8ad3a8d96cddf553fe9ab4b7494ca1049c9d5c070c79adc542af13f925644b41",
    },
    {
        "label": "CEDA_3GB",
        "url": "https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/CMIP/CNRM-CERFACS/CNRM-CM6-1/historical/r9i1p1f2/Omon/thetao/gn/v20190125/thetao_Omon_CNRM-CM6-1_historical_r9i1p1f2_gn_187501-189912.nc",
        "sha256": "e825584ab437e3e1c754d8e171378fb724486e5dfbbcbe53fc3b45fea645dec7",
    },
    # {
    #    "label": "CEDA_9GB",
    #    "url": "https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/CMIP/MIROC/MIROC-ES2L/piControl/r1i1p1f2/Omon/thetao/gn/v20190823/thetao_Omon_MIROC-ES2L_piControl_r1i1p1f2_gn_225001-234912.nc",
    #    "sha256": "22836d086f0f441220d0608b2106497f98936a9e74539516a478ef26b07e93ab",
    # },
    # DKRZ
    {
        "label": "DKRZ_1GB",
        "url": "https://esgf3.dkrz.de/thredds/fileServer/cmip6/CMIP/CCCma/CanESM5/historical/r1i1p2f1/Omon/thetao/gn/v20190429/thetao_Omon_CanESM5_historical_r1i1p2f1_gn_185001-186012.nc",
        "sha256": "fa7c3a0a6cbe4aec8805fda48948972a0bc42aec4fdd065ff3c5b61763522ea6",
    },
    {
        "label": "DKRZ_2GB",
        "url": "https://esgf3.dkrz.de/thredds/fileServer/cmip6/CMIP/MOHC/UKESM1-0-LL/historical/r1i1p1f2/Omon/thetao/gn/v20190627/thetao_Omon_UKESM1-0-LL_historical_r1i1p1f2_gn_200001-201412.nc",
        "sha256": "8ad3a8d96cddf553fe9ab4b7494ca1049c9d5c070c79adc542af13f925644b41",
    },
    {
        "label": "DKRZ_3GB",
        "url": "https://esgf3.dkrz.de/thredds/fileServer/cmip6/CMIP/CNRM-CERFACS/CNRM-CM6-1/historical/r9i1p1f2/Omon/thetao/gn/v20190125/thetao_Omon_CNRM-CM6-1_historical_r9i1p1f2_gn_187501-189912.nc",
        "sha256": "e825584ab437e3e1c754d8e171378fb724486e5dfbbcbe53fc3b45fea645dec7",
    },
    # {
    #    "label": "DKRZ_11GB",
    #    "url": "https://esgf3.dkrz.de/thredds/fileServer/cmip6/CMIP/MIROC/MIROC-ES2L/historical/r7i1p1f2/Omon/thetao/gr1/v20200731/thetao_Omon_MIROC-ES2L_historical_r7i1p1f2_gr1_185001-201412.nc",
    #    "sha256": "7b7b5be4d98fec9fc3db458ab9cceb54adcf26d4895f5151b18eb0461f39ca1a",
    # },
    # NCI - gadi
    {
        "label": "NCI_1GB",
        "url": "https://esgf.nci.org.au/thredds/fileServer/esgcet/replica/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p2f1/Omon/thetao/gn/v20190429/thetao_Omon_CanESM5_historical_r1i1p2f1_gn_185001-186012.nc",
        "sha256": "fa7c3a0a6cbe4aec8805fda48948972a0bc42aec4fdd065ff3c5b61763522ea6",
    },
    {
        "label": "NCI_3GB",
        "url": "https://esgf.nci.org.au/thredds/fileServer/esgcet/replica/CMIP6/CMIP/CNRM-CERFACS/CNRM-CM6-1/historical/r9i1p1f2/Omon/thetao/gn/v20190125/thetao_Omon_CNRM-CM6-1_historical_r9i1p1f2_gn_187501-189912.nc",
        "sha256": "e825584ab437e3e1c754d8e171378fb724486e5dfbbcbe53fc3b45fea645dec7",
    },
    {
        "label": "NCI_11GB",
        "url": "https://esgf.nci.org.au/thredds/fileServer/esgcet/replica/CMIP6/CMIP/MIROC/MIROC-ES2L/historical/r1i1p1f2/Omon/thetao/gr1/v20200731/thetao_Omon_MIROC-ES2L_historical_r1i1p1f2_gr1_185001-201412.nc",
        "sha256": "36a71995bb3fa7a09903561917848fd8a404454af9755d1f8c9458f659d6312b",
    },
    # ORNL - artemis
    {
        "label": "ORNL_1GB",
        "url": "https://esgf-node.ornl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p2f1/Omon/thetao/gn/v20190429/thetao_Omon_CanESM5_historical_r1i1p2f1_gn_185001-186012.nc",
        "sha256": "fa7c3a0a6cbe4aec8805fda48948972a0bc42aec4fdd065ff3c5b61763522ea6",
    },
    {
        "label": "ORNL_2GB",
        "url": "https://esgf-node.ornl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/MOHC/UKESM1-0-LL/historical/r1i1p1f2/Omon/thetao/gn/v20190627/thetao_Omon_UKESM1-0-LL_historical_r1i1p1f2_gn_200001-201412.nc",
        "sha256": "8ad3a8d96cddf553fe9ab4b7494ca1049c9d5c070c79adc542af13f925644b41",
    },
    {
        "label": "ORNL_3GB",
        "url": "https://esgf-node.ornl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CNRM-CERFACS/CNRM-CM6-1/historical/r9i1p1f2/Omon/thetao/gn/v20190125/thetao_Omon_CNRM-CM6-1_historical_r9i1p1f2_gn_187501-189912.nc",
        "sha256": "e825584ab437e3e1c754d8e171378fb724486e5dfbbcbe53fc3b45fea645dec7",
    },
    # Eagle - ANL
    {
        "label": "ANL-GLOBUS_1GB",
        "url": "https://g-52ba3.fd635.8443.data.globus.org/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p2f1/Omon/thetao/gn/v20190429/thetao_Omon_CanESM5_historical_r1i1p2f1_gn_185001-186012.nc",
        "sha256": "fa7c3a0a6cbe4aec8805fda48948972a0bc42aec4fdd065ff3c5b61763522ea6",
    },
    {
        "label": "ANL-GLOBUS_2GB",
        "url": "https://g-52ba3.fd635.8443.data.globus.org/css03_data/CMIP6/CMIP/MOHC/UKESM1-0-LL/historical/r1i1p1f2/Omon/thetao/gn/v20190627/thetao_Omon_UKESM1-0-LL_historical_r1i1p1f2_gn_200001-201412.nc",
        "sha256": "8ad3a8d96cddf553fe9ab4b7494ca1049c9d5c070c79adc542af13f925644b41",
    },
    {
        "label": "ANL-GLOBUS_3GB",
        "url": "https://g-52ba3.fd635.8443.data.globus.org/css03_data/CMIP6/CMIP/CNRM-CERFACS/CNRM-CM6-1/historical/r9i1p1f2/Omon/thetao/gn/v20190125/thetao_Omon_CNRM-CM6-1_historical_r9i1p1f2_gn_187501-189912.nc",
        "sha256": "e825584ab437e3e1c754d8e171378fb724486e5dfbbcbe53fc3b45fea645dec7",
    },
    # {
    #    "label": "ANL-GLOBUS_11GB",
    #    "url": "https://g-52ba3.fd635.8443.data.globus.org/css03_data/CMIP6/CMIP/MIROC/MIROC-ES2L/historical/r7i1p1f2/Omon/thetao/gr1/v20200731/thetao_Omon_MIROC-ES2L_historical_r7i1p1f2_gr1_185001-201412.nc",
    #    "sha256": "7b7b5be4d98fec9fc3db458ab9cceb54adcf26d4895f5151b18eb0461f39ca1a",
    # },
    # Perlmutter - NERSC
    {
        "label": "NERSC-GLOBUS_1GB",
        "url": "https://g-eba899.6b7bd8.0ec8.data.globus.org/css03_data/CMIP6/CMIP/CCCma/CanESM5/historical/r1i1p2f1/Omon/thetao/gn/v20190429/thetao_Omon_CanESM5_historical_r1i1p2f1_gn_185001-186012.nc",
        "sha256": "fa7c3a0a6cbe4aec8805fda48948972a0bc42aec4fdd065ff3c5b61763522ea6",
    },
    {
        "label": "NERSC-GLOBUS_2GB",
        "url": "https://g-eba899.6b7bd8.0ec8.data.globus.org/css03_data/CMIP6/CMIP/MOHC/UKESM1-0-LL/historical/r1i1p1f2/Omon/thetao/gn/v20190627/thetao_Omon_UKESM1-0-LL_historical_r1i1p1f2_gn_200001-201412.nc",
        "sha256": "8ad3a8d96cddf553fe9ab4b7494ca1049c9d5c070c79adc542af13f925644b41",
    },
    {
        "label": "NERSC-GLOBUS_3GB",
        "url": "https://g-eba899.6b7bd8.0ec8.data.globus.org/css03_data/CMIP6/CMIP/CNRM-CERFACS/CNRM-CM6-1/historical/r9i1p1f2/Omon/thetao/gn/v20190125/thetao_Omon_CNRM-CM6-1_historical_r9i1p1f2_gn_187501-189912.nc",
        "sha256": "e825584ab437e3e1c754d8e171378fb724486e5dfbbcbe53fc3b45fea645dec7",
    },
    # {
    #    "label": "NERSC-GLOBUS_11GB",
    #    "url": "https://g-eba899.6b7bd8.0ec8.data.globus.org/css03_data/CMIP6/CMIP/MIROC/MIROC-ES2L/historical/r7i1p1f2/Omon/thetao/gr1/v20200731/thetao_Omon_MIROC-ES2L_historical_r7i1p1f2_gr1_185001-201412.nc",
    #    "sha256": "7b7b5be4d98fec9fc3db458ab9cceb54adcf26d4895f5151b18eb0461f39ca1a",
    # },
]

CHUNK_SIZE = 1 << 20  # 1 MiB read size
THROUGHPUT_SAMPLE_SEC = 5.0  # log a throughput sample every N seconds
BAR_REFRESH_SEC = 0.25  # redraw the on-screen progress bar this often
_BAR_ENABLED = True  # toggled off by --no-bar
MAX_ATTEMPTS = 100  # per-file (re)connection attempts before giving up
CONNECT_TIMEOUT = 30.0  # socket connect timeout (s)
READ_TIMEOUT = 60.0  # per-read socket timeout (s) -> detects stalls
BACKOFF_BASE = 2.0  # exponential backoff base (s)
BACKOFF_CAP = 60.0  # max backoff between attempts (s)
MAX_REDIRECTS = 5
USER_AGENT = "esgf-download-test/1.0 (+stdlib)"

# Network diagnostics
TRACEROUTE_MAX_HOPS = 30
TRACEROUTE_TIMEOUT_S = 60  # overall cap on the traceroute subprocess
TRACEROUTE_PER_HOP_MS = 2000  # per-hop probe wait
# IP-info service for optional ISP/ASN/geo lookup (opt-in via --isp).
# Returns JSON; no key required for light use.
ISP_LOOKUP_URL = "https://ipinfo.io/{ip}/json"
ISP_LOOKUP_TIMEOUT_S = 10
# Origin geolocation for log provenance (on by default; disable with --no-geo).
# ip-api.com needs no key and returns country/city/ISP in one call.
GEO_LOOKUP_URL = "http://ip-api.com/json/?fields=status,country,countryCode,region,regionName,city,lat,lon,isp,org,as,query"
GEO_LOOKUP_TIMEOUT_S = 10

# Heuristic: a redirect Location matching any of these implies a login wall.
AUTH_HINTS = (
    "openid",
    "oauth",
    "login",
    "keycloak",
    "idp",
    "auth",
    "esg-orp",
    "j_spring_openid_security_check",
)


# --------------------------------------------------------------------------- #
# HELPERS
# --------------------------------------------------------------------------- #


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def human_rate(bytes_per_sec):
    mb = bytes_per_sec / (1 << 20)
    return round(mb, 3)


def human_bytes(n):
    if n is None:
        return "?"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f}{unit}" if unit != "B" else f"{int(n)}B"
        n /= 1024


def fmt_eta(seconds):
    if seconds is None or seconds < 0:
        return "--:--"
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def render_progress_bar(label, downloaded, total, rate_bps, width=28):
    """Render a single-line progress bar to stderr using carriage return.
    Falls back to a byte counter when total size is unknown."""
    if not sys.stderr.isatty() or not _BAR_ENABLED:
        return  # don't spew control chars into redirected logs/pipes
    speed = f"{human_rate(rate_bps):.2f}MB/s" if rate_bps else "  --  "
    if total:
        frac = min(1.0, downloaded / total)
        filled = int(width * frac)
        bar = "#" * filled + "-" * (width - filled)
        eta = fmt_eta((total - downloaded) / rate_bps) if rate_bps else "--:--"
        line = (
            f"  {label:<20.20} [{bar}] {frac*100:5.1f}%  "
            f"{human_bytes(downloaded)}/{human_bytes(total)}  "
            f"{speed}  ETA {eta}"
        )
    else:
        line = f"  {label:<20.20} {human_bytes(downloaded)} " f"(size unknown)  {speed}"
    # pad to clear any leftover chars from a longer previous line
    sys.stderr.write("\r" + line.ljust(96)[:96])
    sys.stderr.flush()


def finish_progress_bar():
    if sys.stderr.isatty() and _BAR_ENABLED:
        sys.stderr.write("\n")
        sys.stderr.flush()


def looks_like_auth(location):
    loc = (location or "").lower()
    return any(h in loc for h in AUTH_HINTS)


# --------------------------------------------------------------------------- #
# DIAGNOSTICS  (best-effort, degrade gracefully, minimal deps)
# --------------------------------------------------------------------------- #


def get_public_ip():
    """Best-effort public IP via a tiny HTTPS GET. Returns str or None."""
    try:
        req = urllib.request.Request(
            "https://api.ipify.org", headers={"User-Agent": USER_AGENT}
        )
        with urllib.request.urlopen(req, timeout=ISP_LOOKUP_TIMEOUT_S) as r:
            return r.read().decode().strip()
    except Exception:
        return None


def lookup_isp(ip):
    """
    Opt-in ISP/ASN/geo lookup for an IP (no offline equivalent exists).
    Returns a dict subset or {"error": ...}. Never raises.
    """
    if not ip:
        return {"error": "no ip"}
    try:
        url = ISP_LOOKUP_URL.format(ip=ip)
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=ISP_LOOKUP_TIMEOUT_S) as r:
            data = json.loads(r.read().decode())
        # Keep a tidy, stable subset.
        return {
            k: data.get(k)
            for k in ("ip", "org", "asn", "city", "region", "country", "loc")
            if k in data
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


def get_local_ip():
    """Determine the primary outbound local IP without sending data."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))  # no packets sent for UDP connect
            return s.getsockname()[0]
        finally:
            s.close()
    except Exception:
        return None


def reverse_dns(ip):
    try:
        return socket.gethostbyaddr(ip)[0]
    except Exception:
        return None


def traceroute(host):
    """
    Best-effort traceroute using the OS-native binary.

    Returns dict with raw output, parsed hop count, tool used, and status.
    Never raises; logs 'unavailable' if no suitable binary or it fails.
    """
    out = {"tool": None, "hop_count": None, "status": None, "raw": None}
    system = platform.system()

    if system == "Windows":
        exe = shutil.which("tracert")
        if not exe:
            out["status"] = "unavailable: tracert not found"
            return out
        cmd = [
            exe,
            "-d",
            "-h",
            str(TRACEROUTE_MAX_HOPS),
            "-w",
            str(TRACEROUTE_PER_HOP_MS),
            host,
        ]
    else:
        exe = shutil.which("traceroute")
        if not exe:
            out["status"] = (
                "unavailable: traceroute not installed "
                "(try `apt install traceroute` / `brew install traceroute`)"
            )
            return out
        # -n numeric, -m max hops, -w per-hop wait (s), -q 1 probe
        cmd = [
            exe,
            "-n",
            "-m",
            str(TRACEROUTE_MAX_HOPS),
            "-w",
            str(max(1, TRACEROUTE_PER_HOP_MS // 1000)),
            "-q",
            "1",
            host,
        ]

    out["tool"] = os.path.basename(exe)
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=TRACEROUTE_TIMEOUT_S
        )
        raw = (proc.stdout or "") + (proc.stderr or "")
        out["raw"] = raw.strip()
        out["hop_count"] = _parse_hop_count(raw, system)
        out["status"] = "ok" if out["hop_count"] is not None else "ran_unparsed"
    except subprocess.TimeoutExpired:
        out["status"] = f"unavailable: timed out after {TRACEROUTE_TIMEOUT_S}s"
    except Exception as e:
        out["status"] = f"unavailable: {type(e).__name__}: {e}"
    return out


def _parse_hop_count(raw, system):
    """Count numbered hop lines in traceroute/tracert output."""
    hops = 0
    for line in raw.splitlines():
        s = line.strip()
        m = re.match(r"^(\d+)\b", s)
        if m:
            hops = max(hops, int(m.group(1)))
    return hops or None


def geolocate_origin():
    """
    Best-effort geolocation of this machine's public IP, for log provenance.
    Uses ip-api.com (no key). Returns a tidy dict or {"error": ...}.
    Never raises. This is what tells you a log came from Dakar vs Colombo.
    """
    try:
        req = urllib.request.Request(
            GEO_LOOKUP_URL, headers={"User-Agent": USER_AGENT}
        )
        with urllib.request.urlopen(req, timeout=GEO_LOOKUP_TIMEOUT_S) as r:
            data = json.loads(r.read().decode())
        if data.get("status") != "success":
            return {"error": data.get("message", "lookup failed")}
        return {
            "public_ip": data.get("query"),
            "country": data.get("country"),
            "country_code": data.get("countryCode"),
            "region": data.get("regionName"),
            "city": data.get("city"),
            "latitude": data.get("lat"),
            "longitude": data.get("lon"),
            "isp": data.get("isp"),
            "org": data.get("org"),
            "asn": data.get("as"),
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


def get_all_local_ips():
    """All local IPs bound to this host (best-effort), for multi-homed boxes."""
    ips = set()
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None):
            ip = info[4][0]
            if not ip.startswith("127.") and ip != "::1":
                ips.add(ip)
    except Exception:
        pass
    primary = get_local_ip()
    if primary:
        ips.add(primary)
    return sorted(ips)


def collect_origin_identity(do_geo):
    """
    Self-identifying provenance for the log: who/where/what machine produced it.
    Captured once per run, on by default so colleagues' logs are always
    attributable without needing to pass any flags.
    """
    fqdn = None
    try:
        fqdn = socket.getfqdn()
    except Exception:
        pass
    identity = {
        "hostname": socket.gethostname(),
        "fqdn": fqdn,
        "username": _safe_username(),
        "local_ips": get_all_local_ips(),
        "timezone": _local_timezone(),
        "geo": geolocate_origin() if do_geo else {"status": "skipped (--no-geo)"},
    }
    return identity


def _safe_username():
    try:
        return getpass.getuser()
    except Exception:
        return os.environ.get("USER") or os.environ.get("USERNAME")


def _local_timezone():
    try:
        offset = time.strftime("%z")
        name = time.strftime("%Z")
        return f"{name} (UTC{offset[:3]}:{offset[3:]})" if offset else name
    except Exception:
        return None


def collect_run_diagnostics(do_isp, do_geo=True):
    """Host/platform/network context captured once per run."""
    local_ip = get_local_ip()
    # If geo is on we get the public IP from the geo lookup for free; only fall
    # back to the dedicated public-IP probe when geo is off but --isp is on.
    public_ip = get_public_ip() if (do_isp and not do_geo) else None
    diag = {
        "origin": collect_origin_identity(do_geo),
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "python_version": platform.python_version(),
        "python_build": " ".join(platform.python_build()),
        "local_ip": local_ip,
        "public_ip": public_ip,
        "isp_lookup_enabled": do_isp,
        "local_isp": lookup_isp(public_ip) if do_isp else None,
    }
    return diag


def collect_host_diagnostics(host, do_isp, do_trace):
    """Per-target-host network context (resolution, rDNS, traceroute, ISP)."""
    ips = []
    try:
        infos = socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)
        ips = sorted({i[4][0] for i in infos})
    except Exception:
        pass
    primary = ips[0] if ips else None
    return {
        "host": host,
        "resolved_ips": ips,
        "reverse_dns": reverse_dns(primary) if primary else None,
        "dest_isp": lookup_isp(primary) if (do_isp and primary) else None,
        "traceroute": (
            traceroute(host) if do_trace else {
                "status": "skipped (--no-traceroute)"}
        ),
    }


def open_connection(url, timeout):
    """
    Open an HTTPS connection with per-phase timing.

    Returns (conn, host, path, timings) where timings has dns/tcp/tls seconds.
    Raises on failure.
    """
    parts = urlsplit(url)
    if parts.scheme != "https":
        raise ValueError(
            f"Only https is supported, got scheme={parts.scheme!r}")
    host = parts.hostname
    port = parts.port or 443
    path = parts.path + (("?" + parts.query) if parts.query else "")

    timings = {}

    t0 = time.perf_counter()
    addrinfo = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    timings["dns_s"] = round(time.perf_counter() - t0, 4)

    family, socktype, proto, _, sockaddr = addrinfo[0]
    sock = socket.socket(family, socktype, proto)
    sock.settimeout(timeout)

    t1 = time.perf_counter()
    sock.connect(sockaddr)
    timings["tcp_connect_s"] = round(time.perf_counter() - t1, 4)

    ctx = ssl.create_default_context()
    t2 = time.perf_counter()
    ssock = ctx.wrap_socket(sock, server_hostname=host)
    timings["tls_handshake_s"] = round(time.perf_counter() - t2, 4)
    timings["tls_version"] = ssock.version()
    timings["peer_ip"] = sockaddr[0]

    conn = http.client.HTTPSConnection(host, port, timeout=timeout)
    conn.sock = ssock  # reuse the socket we already connected + timed
    return conn, host, path, timings


def request_with_redirects(url, headers, timeout, max_redirects=MAX_REDIRECTS):
    """
    Issue a GET, following redirects manually so we can detect auth walls.

    Returns (conn, response, final_url, timings, redirect_chain).
    response is an open HTTPResponse positioned at the body, OR
    raises AuthRequired / http.client errors.
    """
    redirect_chain = []
    current = url
    for _ in range(max_redirects + 1):
        conn, host, path, timings = open_connection(current, timeout)
        req_headers = dict(headers)
        req_headers.setdefault("Host", host)
        req_headers.setdefault("User-Agent", USER_AGENT)
        req_headers.setdefault("Accept", "*/*")

        t_send = time.perf_counter()
        conn.request("GET", path, headers=req_headers)
        resp = conn.getresponse()
        timings["ttfb_s"] = round(time.perf_counter() - t_send, 4)

        if resp.status in (301, 302, 303, 307, 308):
            location = resp.getheader("Location", "")
            redirect_chain.append(
                {"from": current, "status": resp.status, "location": location}
            )
            resp.read()
            conn.close()
            if looks_like_auth(location):
                raise AuthRequired(location)
            # resolve relative redirects
            if location.startswith("/"):
                p = urlsplit(current)
                location = f"{p.scheme}://{p.netloc}{location}"
            current = location
            continue

        return conn, resp, current, timings, redirect_chain

    raise RuntimeError("Too many redirects")


class AuthRequired(Exception):
    pass


# --------------------------------------------------------------------------- #
# CORE DOWNLOAD
# --------------------------------------------------------------------------- #


def download_target(target, tmp_dir, progress_path=None):
    """
    Download one target with resume + backoff. Returns a result dict.

    If progress_path is given, appends a JSON line there on every throughput
    sample and on each state change (connect, resume, error, done), so an
    interrupted in-flight download still leaves a durable trail on disk.
    """
    label = target["label"]
    url = target["url"]
    expected_sha = (target.get("sha256") or "").lower().strip()

    def log_progress(event, **extra):
        if not progress_path:
            return
        rec = {"ts": now_iso(), "label": label, "event": event}
        rec.update(extra)
        try:
            with open(progress_path, "a") as pf:
                pf.write(json.dumps(rec) + "\n")
                pf.flush()
                os.fsync(pf.fileno())
        except OSError:
            pass

    result = {
        "label": label,
        "url": url,
        "started": now_iso(),
        "status": "pending",
        "expected_sha256": expected_sha or None,
        "computed_sha256": None,
        "sha256_match": None,
        "total_bytes": None,
        "downloaded_bytes": 0,
        "accept_ranges": None,
        "resumes": 0,
        "attempts": 0,
        "wall_time_s": 0.0,
        "active_transfer_s": 0.0,
        "avg_rate_MBps": None,
        "first_connect_timings": None,
        "throughput_samples": [],  # (elapsed_s, instantaneous_MBps)
        "stalls": 0,  # read timeouts / resets encountered
        "errors": [],
        "redirect_chain": [],
        "finished": None,
    }

    tmp_path = os.path.join(tmp_dir, f"{label}.part")
    sha = hashlib.sha256()
    downloaded = 0
    # If a partial exists from a prior attempt in this run, hash it back in.
    if os.path.exists(tmp_path):
        with open(tmp_path, "rb") as f:
            for blk in iter(lambda: f.read(CHUNK_SIZE), b""):
                sha.update(blk)
                downloaded += len(blk)
    result["downloaded_bytes"] = downloaded

    wall_start = time.perf_counter()

    while result["attempts"] < MAX_ATTEMPTS:
        result["attempts"] += 1
        headers = {}
        if downloaded > 0:
            headers["Range"] = f"bytes={downloaded}-"

        try:
            conn, resp, final_url, timings, chain = request_with_redirects(
                url, headers, CONNECT_TIMEOUT
            )
            if chain:
                result["redirect_chain"] = chain
            if result["first_connect_timings"] is None:
                result["first_connect_timings"] = timings

            # Validate response status
            if downloaded > 0 and resp.status == 200:
                # Server ignored Range -> restart from scratch
                result["accept_ranges"] = False
                sha = hashlib.sha256()
                downloaded = 0
            elif downloaded > 0 and resp.status == 206:
                result["accept_ranges"] = True
            elif resp.status == 200:
                result["accept_ranges"] = (
                    resp.getheader("Accept-Ranges", "").lower() == "bytes"
                )
            elif resp.status == 416:
                # Range not satisfiable: likely already complete
                resp.read()
                conn.close()
                break
            else:
                body = resp.read(512)
                conn.close()
                raise RuntimeError(
                    f"HTTP {resp.status} {resp.reason}: {body[:200]!r}")

            # Determine total size
            clen = resp.getheader("Content-Length")
            crange = resp.getheader("Content-Range")  # bytes start-end/total
            if crange and "/" in crange:
                try:
                    result["total_bytes"] = int(crange.rsplit("/", 1)[1])
                except ValueError:
                    pass
            elif clen and result["total_bytes"] is None:
                result["total_bytes"] = downloaded + int(clen)

            # Stream the body
            transfer_start = time.perf_counter()
            sample_anchor = transfer_start
            sample_bytes = 0
            bar_anchor = transfer_start
            bar_bytes = 0
            bar_rate = 0.0
            log_progress(
                "transfer_start",
                downloaded_bytes=downloaded,
                total_bytes=result["total_bytes"],
                attempt=result["attempts"],
            )
            render_progress_bar(label, downloaded, result["total_bytes"], 0.0)
            f = open(tmp_path, "ab" if downloaded else "wb")
            try:
                while True:
                    try:
                        chunk = resp.read(CHUNK_SIZE)
                    except (socket.timeout, TimeoutError):
                        result["stalls"] += 1
                        raise
                    if not chunk:
                        break
                    f.write(chunk)
                    sha.update(chunk)
                    n = len(chunk)
                    downloaded += n
                    sample_bytes += n
                    bar_bytes += n

                    nowt = time.perf_counter()

                    # Refresh the on-screen bar ~4x/sec with a recent-rate estimate.
                    if nowt - bar_anchor >= BAR_REFRESH_SEC:
                        bar_rate = bar_bytes / (nowt - bar_anchor)
                        render_progress_bar(
                            label, downloaded, result["total_bytes"], bar_rate
                        )
                        bar_anchor = nowt
                        bar_bytes = 0

                    if nowt - sample_anchor >= THROUGHPUT_SAMPLE_SEC:
                        rate = sample_bytes / (nowt - sample_anchor)
                        result["throughput_samples"].append(
                            [round(nowt - wall_start, 2), human_rate(rate)]
                        )
                        result["downloaded_bytes"] = downloaded
                        pct = (
                            round(100.0 * downloaded /
                                  result["total_bytes"], 1)
                            if result["total_bytes"]
                            else None
                        )
                        log_progress(
                            "sample",
                            elapsed_s=round(nowt - wall_start, 2),
                            downloaded_bytes=downloaded,
                            total_bytes=result["total_bytes"],
                            percent=pct,
                            instant_MBps=human_rate(rate),
                            resumes=result["resumes"],
                            stalls=result["stalls"],
                        )
                        sample_anchor = nowt
                        sample_bytes = 0
            finally:
                f.close()
                # Final bar paint for this segment, then move to a new line.
                render_progress_bar(label, downloaded,
                                    result["total_bytes"], bar_rate)
                finish_progress_bar()
                result["active_transfer_s"] += time.perf_counter() - \
                    transfer_start
                conn.close()

            result["downloaded_bytes"] = downloaded

            # Completed?
            if result["total_bytes"] is None or downloaded >= result["total_bytes"]:
                break
            # Short read without error -> loop will resume via Range
            result["resumes"] += 1
            log_progress(
                "resume",
                downloaded_bytes=downloaded,
                total_bytes=result["total_bytes"],
                resumes=result["resumes"],
            )

        except AuthRequired as e:
            result["status"] = "auth_required"
            result["errors"].append(f"auth redirect -> {e}")
            result["finished"] = now_iso()
            result["wall_time_s"] = round(time.perf_counter() - wall_start, 2)
            log_progress("auth_required", downloaded_bytes=downloaded)
            _cleanup(tmp_path)
            return result

        except (
            socket.timeout,
            TimeoutError,
            ConnectionError,
            http.client.HTTPException,
            ssl.SSLError,
            OSError,
            RuntimeError,
        ) as e:
            result["resumes"] += 1 if downloaded > 0 else 0
            result["errors"].append(
                f"attempt {result['attempts']}: {type(e).__name__}: {e}"
            )
            backoff = min(BACKOFF_CAP, BACKOFF_BASE **
                          min(result["attempts"], 6))
            log_progress(
                "error",
                attempt=result["attempts"],
                error=f"{type(e).__name__}: {e}",
                downloaded_bytes=downloaded,
                backoff_s=round(backoff, 1),
            )
            time.sleep(backoff)
            continue

    # --- finalize ---
    result["wall_time_s"] = round(time.perf_counter() - wall_start, 2)
    result["downloaded_bytes"] = downloaded
    if result["active_transfer_s"] > 0:
        result["avg_rate_MBps"] = human_rate(
            downloaded / result["active_transfer_s"])

    complete = (
        result["total_bytes"] is not None and downloaded >= result["total_bytes"]
    ) or (result["total_bytes"] is None and downloaded > 0 and not result["errors"])

    if complete:
        result["computed_sha256"] = sha.hexdigest()
        if expected_sha:
            result["sha256_match"] = result["computed_sha256"] == expected_sha
            result["status"] = (
                "verified" if result["sha256_match"] else "checksum_mismatch"
            )
        else:
            result["status"] = "complete_unverified"
    elif result["status"] == "pending":
        result["status"] = "failed"

    _cleanup(tmp_path)  # discard the file regardless of outcome
    result["finished"] = now_iso()
    log_progress(
        "done",
        status=result["status"],
        downloaded_bytes=downloaded,
        total_bytes=result["total_bytes"],
        sha256_match=result["sha256_match"],
        wall_time_s=result["wall_time_s"],
        avg_rate_MBps=result["avg_rate_MBps"],
    )
    return result


def _cleanup(path):
    try:
        if os.path.exists(path):
            os.remove(path)
    except OSError:
        pass


# --------------------------------------------------------------------------- #
# OUTPUT
# --------------------------------------------------------------------------- #

CSV_FIELDS = [
    "origin_hostname",
    "origin_country",
    "origin_city",
    "origin_isp",
    "origin_public_ip",
    "label",
    "status",
    "url",
    "host",
    "total_bytes",
    "downloaded_bytes",
    "accept_ranges",
    "attempts",
    "resumes",
    "stalls",
    "wall_time_s",
    "active_transfer_s",
    "avg_rate_MBps",
    "sha256_match",
    "dns_s",
    "tcp_connect_s",
    "tls_handshake_s",
    "ttfb_s",
    "peer_ip",
    "tls_version",
    "reverse_dns",
    "hop_count",
    "traceroute_status",
    "dest_org",
    "dest_country",
    "started",
    "finished",
]


def flatten_for_csv(r, host_diag=None, origin=None):
    t = r.get("first_connect_timings") or {}
    hd = host_diag or {}
    tr = hd.get("traceroute") or {}
    isp = hd.get("dest_isp") or {}
    o = origin or {}
    geo = o.get("geo") or {}
    return {
        "origin_hostname": o.get("hostname"),
        "origin_country": geo.get("country"),
        "origin_city": geo.get("city"),
        "origin_isp": geo.get("isp"),
        "origin_public_ip": geo.get("public_ip"),
        "label": r["label"],
        "status": r["status"],
        "url": r["url"],
        "host": r.get("host"),
        "total_bytes": r["total_bytes"],
        "downloaded_bytes": r["downloaded_bytes"],
        "accept_ranges": r["accept_ranges"],
        "attempts": r["attempts"],
        "resumes": r["resumes"],
        "stalls": r["stalls"],
        "wall_time_s": r["wall_time_s"],
        "active_transfer_s": r["active_transfer_s"],
        "avg_rate_MBps": r["avg_rate_MBps"],
        "sha256_match": r["sha256_match"],
        "dns_s": t.get("dns_s"),
        "tcp_connect_s": t.get("tcp_connect_s"),
        "tls_handshake_s": t.get("tls_handshake_s"),
        "ttfb_s": t.get("ttfb_s"),
        "peer_ip": t.get("peer_ip"),
        "tls_version": t.get("tls_version"),
        "reverse_dns": hd.get("reverse_dns"),
        "hop_count": tr.get("hop_count"),
        "traceroute_status": tr.get("status"),
        "dest_org": isp.get("org"),
        "dest_country": isp.get("country"),
        "started": r["started"],
        "finished": r["finished"],
    }


def _atomic_write(path, write_fn):
    """Write via a temp file in the same dir, then os.replace -> never leaves a
    half-written output if the process dies mid-write."""
    d = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(dir=d, prefix=".tmp_", suffix=".part")
    try:
        with os.fdopen(fd, "w", newline="") as f:
            write_fn(f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


def flush_outputs(run_meta, json_path, csv_path):
    """Persist the full run state to JSON and CSV. Safe to call repeatedly;
    each call fully rewrites both files atomically so a crash can't corrupt
    already-completed results."""

    def _json(f):
        json.dump(run_meta, f, indent=2)

    _atomic_write(json_path, _json)

    def _csv(f):
        origin = (run_meta.get("run_diagnostics") or {}).get("origin")
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        for r in run_meta["results"]:
            hd = run_meta["host_diagnostics"].get(r.get("host"))
            w.writerow(flatten_for_csv(r, hd, origin))

    _atomic_write(csv_path, _csv)


def parse_args(argv):
    p = argparse.ArgumentParser(
        description="Test CMIP6 downloads across ESGF nodes with timing, "
        "stability and network diagnostics."
    )
    p.add_argument(
        "--isp",
        action="store_true",
        help="Enable ISP/ASN/geo lookups (one HTTPS GET per IP to "
        "ipinfo.io). Off by default to stay self-contained.",
    )
    p.add_argument(
        "--no-traceroute",
        action="store_true",
        help="Skip traceroute (otherwise run best-effort using the "
        "OS-native tracert/traceroute binary).",
    )
    p.add_argument(
        "--no-bar",
        action="store_true",
        help="Disable the live progress bar (auto-disabled anyway when "
        "stderr is not a terminal, e.g. when piping to a log file).",
    )
    p.add_argument(
        "--no-geo",
        action="store_true",
        help="Disable origin geolocation. By default the log records the "
        "machine's country/city/ISP + public IP (one lightweight HTTPS call) "
        "so shared logs are self-identifying; use this for privacy.",
    )
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv if argv is not None else sys.argv[1:])
    do_isp = args.isp
    do_trace = not args.no_traceroute
    do_geo = not args.no_geo
    global _BAR_ENABLED
    _BAR_ENABLED = not args.no_bar

    if not TARGETS:
        print(
            "No TARGETS configured. Edit the TARGETS list at the top of the "
            "script with your resolved ESGF file URLs, then re-run.",
            file=sys.stderr,
        )
        return 2

    stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    out_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(out_dir, f"{stamp}_esgf_results.json")
    csv_path = os.path.join(out_dir, f"{stamp}_esgf_results.csv")

    run_meta = {
        "run_started": now_iso(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "config": {
            "chunk_size": CHUNK_SIZE,
            "max_attempts": MAX_ATTEMPTS,
            "connect_timeout": CONNECT_TIMEOUT,
            "read_timeout": READ_TIMEOUT,
            "isp_lookup": do_isp,
            "traceroute": do_trace,
            "geolocate_origin": do_geo,
        },
        "run_diagnostics": None,
        "host_diagnostics": {},
        "results": [],
    }

    print("Collecting run/host diagnostics ...", flush=True)
    run_meta["run_diagnostics"] = collect_run_diagnostics(do_isp, do_geo)

    # Print a provenance banner so it's obvious which machine/location this
    # log belongs to (also written into the JSON/CSV).
    origin = run_meta["run_diagnostics"].get("origin", {})
    geo = origin.get("geo", {})
    if geo and "error" not in geo and "status" not in geo:
        print(
            f"  origin: {origin.get('hostname')} | "
            f"{geo.get('city')}, {geo.get('country')} | "
            f"{geo.get('isp')} | {geo.get('public_ip')}",
            flush=True,
        )
    else:
        print(f"  origin: {origin.get('hostname')} "
              f"(geo {'disabled' if not do_geo else 'unavailable'})",
              flush=True)

    # Per-host diagnostics, cached so each node is probed once even if it
    # serves several files.
    host_diag_cache = {}

    with tempfile.TemporaryDirectory(prefix="esgf_dl_") as tmp_dir:
        for target in TARGETS:
            print(f"\n=== {target['label']} ===", flush=True)
            host = urlsplit(target["url"]).hostname
            if host and host not in host_diag_cache:
                print(
                    f"  diagnostics for {host} "
                    f"(traceroute={'on' if do_trace else 'off'}) ...",
                    flush=True,
                )
                host_diag_cache[host] = collect_host_diagnostics(
                    host, do_isp, do_trace)
                run_meta["host_diagnostics"] = host_diag_cache

            print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            progress_path = os.path.join(
                out_dir, f"{stamp}_esgf_progress.jsonl")
            r = download_target(target, tmp_dir, progress_path=progress_path)
            r["host"] = host
            run_meta["results"].append(r)
            print(
                f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"  status={r['status']}  bytes={r['downloaded_bytes']}"
                f"  resumes={r['resumes']}  stalls={r['stalls']}"
                f"  avg={r['avg_rate_MBps']} MB/s  sha_match={r['sha256_match']}",
                flush=True,
            )

            # Persist the complete run state after every file, so a later
            # failure never loses results already gathered.
            run_meta["run_finished"] = now_iso()
            flush_outputs(run_meta, json_path, csv_path)
            print(
                f"  logged -> {os.path.basename(json_path)} / "
                f"{os.path.basename(csv_path)}",
                flush=True,
            )

    print(f"\nWrote:\n  {json_path}\n  {csv_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
