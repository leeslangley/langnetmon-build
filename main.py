#!/usr/bin/env python3
"""
NetMon Windows Agent
Monitors network health, displays a floating status window,
and reports metrics to the Mac Studio daemon.
"""

import http.client
import json
import logging
import os
import platform
import re
import socket
import subprocess
import sys
import threading
import time
import winreg
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Optional
import urllib.request
import urllib.error

# ── Single-instance guard ─────────────────────────────────────────────────
# Use a named Windows mutex to ensure only one instance runs at a time.
# Must be called before any other startup logic. The mutex handle is kept
# alive in _SINGLETON_MUTEX so GC doesn't release it.
_SINGLETON_MUTEX = None

def _enforce_single_instance() -> None:
    """
    Kill any other running NetMonAgent processes, then acquire the singleton
    mutex so future launches also clean up cleanly.
    """
    global _SINGLETON_MUTEX
    my_pid = os.getpid()
    my_name = Path(sys.executable).stem.lower()  # e.g. 'netmonagent-v1.7.6'

    # Kill all other NetMonAgent processes by name match (case-insensitive)
    try:
        result = subprocess.run(
            ["tasklist", "/FO", "CSV", "/NH"],
            capture_output=True, text=True, timeout=10,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        for line in result.stdout.splitlines():
            parts = [p.strip('"') for p in line.split(',')]
            if len(parts) < 2:
                continue
            proc_name = parts[0].lower()
            try:
                pid = int(parts[1])
            except ValueError:
                continue
            if pid == my_pid:
                continue
            if 'netmonagent' in proc_name:
                subprocess.run(
                    ["taskkill", "/F", "/PID", str(pid)],
                    capture_output=True,
                    creationflags=subprocess.CREATE_NO_WINDOW,
                )
    except Exception:
        pass

    # Acquire mutex so this instance is the definitive owner
    try:
        import ctypes
        mutex = ctypes.windll.kernel32.CreateMutexW(None, True, "NetMonAgent_SingleInstance_Mutex")
        _SINGLETON_MUTEX = mutex  # keep alive for process lifetime
    except Exception:
        pass  # non-Windows / ctypes unavailable — skip

import tkinter as tk
import pystray
from PIL import Image, ImageDraw

# ── Paths & config ─────────────────────────────────────────────────────────

# Use the exe's own directory (works in both PyInstaller frozen and dev mode)
_HERE = Path(sys.executable).parent if getattr(sys, "frozen", False) else Path(__file__).parent
CONFIG_PATH = _HERE / "netmon_config.json"
LOG_PATH = _HERE / "netmon_agent.log"

DEFAULT_CONFIG = {
    "mac_ip": "192.168.1.161",  # hardcoded — do not override
    "mac_port": 9876,
}

MAC_IP   = "192.168.1.161"  # always use this — ignore any saved config
MAC_PORT = 9876

# ── Sync config (Task 3 — LLANGLEY-CHILLB + LLANGLEY16 only) ──────────────
# Folder sync is gated by hostname. Defence-in-depth: config.wtf is hardcoded
# in the exclusion list AND checked explicitly before upload.
SYNC_ENABLED_HOSTS = ["LLANGLEY-CHILLB", "LLANGLEY16"]

SYNC_PATHS = {
    "old-cylance": r"C:\Users\leesl\Documents\Old Cylance",
    "wow-addons":  r"C:\Program Files (x86)\World of Warcraft\_retail_\Interface\Addons",
    "wow-wtf":     r"C:\Program Files (x86)\World of Warcraft\_retail_\WTF",
}

# HARD EXCLUSION — per-machine graphics settings. Never sync.
SYNC_EXCLUSIONS = ["config.wtf"]
SYNC_INTERVAL_SECONDS = 3600

def load_config() -> dict:
    """Always return the hardcoded defaults. Config file is ignored for mac_ip/mac_port
    to avoid stale configs on reinstall breaking connectivity. Sync config is
    merged from netmon_config.json if present (not critical if missing)."""
    cfg = {
        "mac_ip": MAC_IP,
        "mac_port": MAC_PORT,
        "sync_enabled_hosts": SYNC_ENABLED_HOSTS,
        "sync_paths": dict(SYNC_PATHS),
        "sync_exclusions": list(SYNC_EXCLUSIONS),
        "sync_interval_seconds": SYNC_INTERVAL_SECONDS,
    }
    try:
        if CONFIG_PATH.exists():
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                user = json.load(f)
            for k in ("sync_enabled_hosts", "sync_paths", "sync_exclusions", "sync_interval_seconds"):
                if k in user:
                    cfg[k] = user[k]
    except Exception:
        pass
    return cfg

# ── Logging ───────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(str(LOG_PATH), encoding="utf-8"),
    ],
)
log = logging.getLogger("netmon_agent")

# ── In-memory state ───────────────────────────────────────────────────────

GATEWAY_HOST = "192.168.1.254"

class _State:
    def __init__(self):
        self.lock = threading.Lock()
        self.ping_results: deque[dict] = deque(maxlen=5)
        self.gateway_ping_results: deque[dict] = deque(maxlen=5)
        self.http_latency_ms: Optional[float] = None
        self.http_success: bool = False
        # Per-URL HTTP probe results (url -> {latency_ms, success, ts})
        self.http_results: dict = {}
        # Dynamic probe URL list fetched from the Mac daemon
        self.http_probe_urls: list = ["https://www.google.com"]
        self.mac_reachable: bool = False
        self.mac_fail_since: Optional[datetime] = None  # when consecutive failures started
        self.last_update: datetime = datetime.now()
        self.last_command_poll: datetime = datetime.now()  # tracks command poll loop timing
        self._shell_active_count: int = 0  # concurrency guard for shell commands

state = _State()

# ── Internet-down state tracker ───────────────────────────────────────────
_internet_state: dict = {
    "down": False,
    "consecutive_failures": 0,
    "last_notified": 0.0,
    "down_since": 0.0,
    "http_partial_down": False,
    "http_partial_failures": 0,
}

# Module-level tray icon reference — set by NetMonWindow._run_tray so that
# report_loop (which runs in a background thread) can fire toast notifications.
_tray_icon: Optional[pystray.Icon] = None

# ── Sysinfo ───────────────────────────────────────────────────────────────

_START_TIME: float = time.time()   # module load time — used for uptime_hours
_sysinfo: dict = {}                # populated at startup, refreshed every 60s


def _collect_sysinfo() -> dict:
    """Collect WiFi / network / system info. Always returns a dict; never raises."""
    result: dict = {}
    try:
        result["hostname"] = socket.gethostname()
    except Exception:
        result["hostname"] = "unknown"
    try:
        result["ip_address"] = socket.gethostbyname(socket.gethostname())
    except Exception:
        result["ip_address"] = None
    try:
        result["os_version"] = platform.version()
    except Exception:
        result["os_version"] = None
    try:
        result["uptime_hours"] = (time.time() - _START_TIME) / 3600.0
    except Exception:
        result["uptime_hours"] = None

    # WiFi info via PowerShell WMI (avoids netsh which triggers Windows Location Services icon)
    try:
        proc = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
             "-Command",
             "$p = Get-NetConnectionProfile | Select-Object -First 1; "
             "$n = if($p){Get-NetAdapter -InterfaceIndex $p.InterfaceIndex -ErrorAction SilentlyContinue}; "
             "$r = @{}; "
             "if($p -and $n -and $n.MediaType -match '802.11'){"
             "$r['connection_type']='wifi'; "
             "$r['wifi_ssid']=$p.Name; "
             "$r['wifi_radio_type']=$n.MediaType; "
             "$r['adapter_name']=$n.Name; "
             "$r['wifi_channel']=$null; "
             "$r['wifi_bssid']=$null; "
             "$sig = netsh wlan show interfaces 2>$null | Select-String 'Signal'; "
             "if($sig){try{$r['wifi_signal_pct']=[int](($sig -replace '\D',''))}catch{$r['wifi_signal_pct']=$null}}else{$r['wifi_signal_pct']=$null}"
             "}else{"
             "$r['connection_type']='ethernet'; "
             "$r['wifi_ssid']=$null; "
             "$r['wifi_bssid']=$null; "
             "$r['wifi_radio_type']=$null; "
             "$r['wifi_channel']=$null; "
             "$r['wifi_signal_pct']=$null; "
             "$r['adapter_name']=if($n){$n.Name}else{$null}"
             "}; "
             "$r | ConvertTo-Json"],
            capture_output=True, text=True, timeout=10,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        if proc.stdout.strip():
            wifi_data = json.loads(proc.stdout.strip())
            if isinstance(wifi_data, dict):
                result.update(wifi_data)
        if not result.get("wifi_ssid") and result.get("connection_type") != "wifi":
            result["connection_type"] = "ethernet"
            for k in ["wifi_ssid","wifi_bssid","wifi_radio_type","wifi_channel","wifi_signal_pct","adapter_name"]:
                result.setdefault(k, None)
    except Exception:
        result["connection_type"]  = "unknown"
        for k in ["wifi_ssid","wifi_bssid","wifi_radio_type","wifi_channel","wifi_signal_pct","adapter_name"]:
            result[k] = None

    # DNS resolution latency
    try:
        import time as _time
        _t0 = _time.time()
        socket.getaddrinfo("google.com", 80)
        result["dns_latency_ms"] = round((_time.time() - _t0) * 1000, 1)
    except Exception:
        result["dns_latency_ms"] = None

    # Adapter link speed via WMIC (works for both WiFi and ethernet)
    try:
        proc2 = subprocess.run(
            ["wmic", "nic", "where", "NetEnabled=true", "get", "Name,Speed,MACAddress", "/format:csv"],
            capture_output=True, text=True, timeout=10,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        best_speed = None
        best_mac = None
        for line in proc2.stdout.splitlines():
            line = line.strip()
            if not line or line.startswith("Node"):
                continue
            parts = line.split(",")
            if len(parts) >= 4:
                mac_addr = parts[1].strip()
                name_val = parts[2].strip()
                speed_val = parts[3].strip()
                if speed_val and speed_val.isdigit() and int(speed_val) > 0:
                    spd_mbps = int(speed_val) // 1_000_000
                    if best_speed is None or spd_mbps > best_speed:
                        best_speed = spd_mbps
                        best_mac = mac_addr
        result["link_speed_mbps"] = best_speed
        result["mac_address"] = best_mac
    except Exception:
        result["link_speed_mbps"] = None
        result["mac_address"] = None

    # Gateway MAC from ARP table — detects DHCP flap or evil-twin changes
    try:
        proc3 = subprocess.run(
            ["arp", "-a", GATEWAY_HOST],
            capture_output=True, text=True, timeout=5,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        gw_mac = None
        for line in proc3.stdout.splitlines():
            if GATEWAY_HOST in line:
                parts = line.split()
                if len(parts) >= 2:
                    gw_mac = parts[1]
                    break
        result["gateway_mac"] = gw_mac
    except Exception:
        result["gateway_mac"] = None

    # CPU and RAM usage
    try:
        # CPU: PowerShell CIM (wmic is deprecated in newer Windows)
        proc4 = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
             "-Command", "(Get-CimInstance Win32_Processor).LoadPercentage"],
            capture_output=True, text=True, timeout=10,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        cpu_pct = None
        val = proc4.stdout.strip()
        if val.isdigit():
            cpu_pct = int(val)
        result["cpu_pct"] = cpu_pct
    except Exception:
        result["cpu_pct"] = None

    try:
        # RAM: PowerShell CIM (wmic is deprecated in newer Windows)
        proc5 = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
             "-Command",
             "$os = Get-CimInstance Win32_OperatingSystem; "
             "\"$($os.FreePhysicalMemory),$($os.TotalVisibleMemorySize)\""],
            capture_output=True, text=True, timeout=10,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        ram_free_mb = None
        ram_total_mb = None
        parts = proc5.stdout.strip().split(",")
        if len(parts) == 2:
            try:
                ram_free_mb = int(parts[0].strip()) // 1024
                ram_total_mb = int(parts[1].strip()) // 1024
            except (ValueError, IndexError):
                pass
        result["ram_free_mb"] = ram_free_mb
        result["ram_total_mb"] = ram_total_mb
        if ram_total_mb and ram_free_mb is not None:
            result["ram_used_pct"] = round((1 - ram_free_mb / ram_total_mb) * 100, 1)
        else:
            result["ram_used_pct"] = None
    except Exception:
        result["ram_free_mb"] = None
        result["ram_total_mb"] = None
        result["ram_used_pct"] = None

    return result


def _sysinfo_loop() -> None:
    """Refresh _sysinfo every 60 seconds."""
    global _sysinfo
    while True:
        try:
            _sysinfo = _collect_sysinfo()
            log.debug(
                f"Sysinfo refreshed: type={_sysinfo.get('connection_type')} "
                f"ssid={_sysinfo.get('wifi_ssid')} ip={_sysinfo.get('ip_address')}"
            )
        except Exception as e:
            log.error(f"Sysinfo loop error: {e}")
        time.sleep(60)


# ── Color thresholds ─────────────────────────────────────────────────────

C_GREEN  = "#2ecc71"
C_ORANGE = "#e67e22"
C_RED    = "#e74c3c"
C_GREY   = "#7f8c8d"

_COLOR_PRIORITY = {C_RED: 3, C_ORANGE: 2, C_GREEN: 1, C_GREY: 0}

def ping_color(results: list[dict]) -> str:
    if not results:
        return C_GREY
    failures = sum(1 for r in results if not r["success"])
    latencies = [r["latency_ms"] for r in results if r["success"] and r["latency_ms"] is not None]
    avg = sum(latencies) / len(latencies) if latencies else None
    if failures >= 3:
        return C_RED
    if failures >= 1 or (avg is not None and avg > 150):
        return C_ORANGE
    if avg is not None and avg > 80:
        return C_ORANGE
    return C_GREEN

def http_color(latency_ms: Optional[float], success: bool) -> str:
    if not success or latency_ms is None:
        return C_RED
    if latency_ms > 300:   # was 250 — relaxed by ~20%
        return C_RED
    if latency_ms > 180:   # was 150 — relaxed by ~20%
        return C_ORANGE
    return C_GREEN

def mac_color(reachable: bool) -> str:
    return C_GREEN if reachable else C_RED

def worst_color(*colors: str) -> str:
    return max(colors, key=lambda c: _COLOR_PRIORITY.get(c, 0))

# ── Ping (Windows) ───────────────────────────────────────────────────────

def _ping_once(host: str = "8.8.8.8") -> tuple[Optional[float], bool]:
    try:
        result = subprocess.run(
            ["ping", "-n", "1", "-w", "1000", host],
            capture_output=True, text=True, timeout=5,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        if result.returncode == 0:
            # Handles "time=12ms", "time<1ms", "time=1ms"
            m = re.search(r"time[=<](\d+)ms", result.stdout, re.IGNORECASE)
            if m:
                return float(m.group(1)), True
        return None, False
    except Exception as e:
        log.debug(f"Ping error: {e}")
        return None, False

def ping_loop() -> None:
    log.info("Ping loop started → 8.8.8.8 every 2s")
    while True:
        t0 = time.monotonic()
        try:
            latency, success = _ping_once()
            with state.lock:
                state.ping_results.append({"latency_ms": latency, "success": success})
                state.last_update = datetime.now()
            log.debug(f"Ping: {'OK' if success else 'FAIL'} {latency}ms")
        except Exception as e:
            log.error(f"Ping loop error: {e}")
        time.sleep(max(0.0, 2.0 - (time.monotonic() - t0)))


def gateway_ping_loop() -> None:
    log.info(f"Gateway ping loop started → {GATEWAY_HOST} every 2s")
    while True:
        t0 = time.monotonic()
        try:
            latency, success = _ping_once(GATEWAY_HOST)
            with state.lock:
                state.gateway_ping_results.append({"latency_ms": latency, "success": success})
            log.debug(f"GW ping: {'OK' if success else 'FAIL'} {latency}ms")
        except Exception as e:
            log.error(f"Gateway ping loop error: {e}")
        time.sleep(max(0.0, 2.0 - (time.monotonic() - t0)))

# ── HTTP check ────────────────────────────────────────────────────────────

def _http_check(url: str = "https://www.google.com") -> tuple[Optional[float], bool]:
    """
    HTTP check with explicit IPv4-only DNS to avoid Windows IPv6 fallback stall (~80s).
    Logs DNS time separately for diagnostics.
    """
    import ssl
    import http.client
    import socket
    from urllib.parse import urlparse

    try:
        parsed = urlparse(url)
        hostname = parsed.hostname
        port = parsed.port or (443 if parsed.scheme == "https" else 80)

        # Force IPv4 DNS — avoids 80s stall from Windows trying IPv6 first
        t_dns = time.monotonic()
        try:
            addr_infos = socket.getaddrinfo(hostname, port, socket.AF_INET, socket.SOCK_STREAM)
            ipv4 = addr_infos[0][4][0]
        except Exception as e:
            log.warning(f"HTTP DNS error for {hostname}: {e}")
            return None, False
        dns_ms = (time.monotonic() - t_dns) * 1000
        log.debug(f"HTTP DNS: {hostname} -> {ipv4} in {dns_ms:.1f}ms")

        t0 = time.monotonic()
        if parsed.scheme == "https":
            ctx = ssl.create_default_context()
            # Pass hostname (not ipv4) as the host so HTTPSConnection sets SNI correctly,
            # then override the connection address via _get_hostip trick using a custom
            # socket. Simpler: create conn with hostname for SNI, override connect to use ipv4.
            conn = http.client.HTTPSConnection(hostname, port, timeout=10, context=ctx)
            # Monkey-patch connect to force IPv4 — avoids Windows IPv6 stall (~80s).
            # HTTPSConnection is created with hostname so SNI is set correctly;
            # we just override the socket creation to use the pre-resolved IPv4 addr.
            def _ipv4_connect(bound_ipv4=ipv4, bound_port=port, bound_ctx=ctx, bound_host=hostname):
                sock = socket.create_connection((bound_ipv4, bound_port), timeout=10)
                conn.sock = bound_ctx.wrap_socket(sock, server_hostname=bound_host)
            conn.connect = _ipv4_connect
        else:
            conn = http.client.HTTPConnection(hostname, port, timeout=10)
            def _ipv4_connect(bound_ipv4=ipv4, bound_port=port):
                conn.sock = socket.create_connection((bound_ipv4, bound_port), timeout=10)
            conn.connect = _ipv4_connect

        conn.request("GET", parsed.path or "/", headers={"Host": hostname, "User-Agent": "netmon-agent/1.0"})
        resp = conn.getresponse()
        latency = (time.monotonic() - t0) * 1000
        ok = resp.status < 400
        resp.read()  # drain
        conn.close()
        log.debug(f"HTTP: {'OK' if ok else 'FAIL'} {latency:.1f}ms (DNS {dns_ms:.1f}ms) status={resp.status}")
        return latency, ok
    except Exception as e:
        log.warning(f"HTTP check error: {e}")
        return None, False

def _fetch_probe_urls(cfg: dict) -> Optional[list]:
    """Fetch the live probe URL list from the Mac daemon. Returns None on failure."""
    try:
        raw = _mac_http_get_ipv4(cfg["mac_ip"], int(cfg["mac_port"]), "/api/probes/config", timeout=5)
        payload = json.loads(raw.decode("utf-8", errors="replace"))
        urls = payload.get("probes") or []
        if isinstance(urls, list) and all(isinstance(u, str) for u in urls) and urls:
            return urls
        log.warning(f"Probe config returned unexpected payload: {payload}")
    except Exception as e:
        log.warning(f"Probe config fetch error: {e}")
    return None


def http_loop(cfg: dict) -> None:
    """Loop over the probes returned by the daemon. Refreshes hourly.
    Falls back to google.com if the config endpoint is unreachable."""
    log.info("HTTP check loop started — dynamic probes from daemon (IPv4-forced)")
    urls: list = ["https://www.google.com"]
    last_fetch = 0.0
    first_fetch_done = False
    while True:
        t0 = time.monotonic()
        try:
            # Fetch probe list on startup, then every hour
            if (not first_fetch_done) or (t0 - last_fetch > 3600):
                fetched = _fetch_probe_urls(cfg)
                if fetched:
                    urls = fetched
                    log.info(f"HTTP probe URLs refreshed: {urls}")
                elif not first_fetch_done:
                    log.warning("Probe config unreachable — falling back to https://www.google.com only")
                last_fetch = t0
                first_fetch_done = True

            results_this_round: dict = {}
            primary_latency: Optional[float] = None
            primary_ok: bool = False
            for i, url in enumerate(urls):
                latency, ok = _http_check(url)
                ts = datetime.utcnow().isoformat()
                results_this_round[url] = {"latency_ms": latency, "success": ok, "ts": ts}
                if i == 0:
                    primary_latency = latency
                    primary_ok = ok
                log.debug(f"HTTP {url}: {'OK' if ok else 'FAIL'} {latency}ms")

            with state.lock:
                state.http_results = results_this_round
                state.http_probe_urls = list(urls)
                # Preserve legacy single-probe fields for any callers still reading them
                state.http_latency_ms = primary_latency
                state.http_success = primary_ok
        except Exception as e:
            log.error(f"HTTP loop error: {e}")
        time.sleep(max(0.0, 5.0 - (time.monotonic() - t0)))

# ── Internet-down notification helper ────────────────────────────────────

def _notify_user(title: str, body: str) -> None:
    """Fire a pystray toast if the tray icon is running; fall back to log only.
    Anti-spam: silently drops calls made within 60 s of the last notification."""
    now = time.time()
    if now - _internet_state["last_notified"] < 60:
        return
    _internet_state["last_notified"] = now
    try:
        if _tray_icon:
            _tray_icon.notify(body, title=title)
    except Exception:
        pass


def _short_domain(url: str) -> str:
    """Strip protocol and 'www.' prefix from a probe URL, return bare host."""
    s = url.strip()
    if s.startswith("https://"):
        s = s[8:]
    elif s.startswith("http://"):
        s = s[7:]
    if s.startswith("www."):
        s = s[4:]
    s = s.split("/", 1)[0]
    return s


def _format_http_probe_status(results: dict) -> str:
    """Render probe results as 'name OK (Xms)' for success or 'name \u2717' for failure."""
    if not results:
        return "no probes"
    parts = []
    for url, r in results.items():
        name = _short_domain(url)
        if r.get("success"):
            lat = r.get("latency_ms")
            if lat is not None:
                parts.append(f"{name} OK ({int(round(lat))}ms)")
            else:
                parts.append(f"{name} OK")
        else:
            parts.append(f"{name} \u2717")
    return ", ".join(parts)


# ── Report to Mac ─────────────────────────────────────────────────────────

def report_loop(cfg: dict) -> None:
    import http.client as _hc
    mac_ip   = cfg["mac_ip"]
    mac_port = int(cfg["mac_port"])
    log.info(f"Report loop started → {mac_ip}:{mac_port} every 10s")
    while True:
        t0 = time.monotonic()
        try:
            with state.lock:
                results = list(state.ping_results)
                gw_results = list(state.gateway_ping_results)
                http_lat = state.http_latency_ms
                http_ok = state.http_success
                http_results_snapshot = dict(state.http_results)
                probe_urls_snapshot = list(state.http_probe_urls)

            lats = [r["latency_ms"] for r in results if r["success"] and r["latency_ms"] is not None]
            avg_ping = sum(lats) / len(lats) if lats else None
            ping_ok = any(r["success"] for r in results) if results else False

            gw_lats = [r["latency_ms"] for r in gw_results if r["success"] and r["latency_ms"] is not None]
            avg_gw_ping = sum(gw_lats) / len(gw_lats) if gw_lats else None
            gw_ping_ok = any(r["success"] for r in gw_results) if gw_results else False

            # ── Local internet-down detection ─────────────────────────────
            # Requires BOTH WAN ping (8.8.8.8) AND at least one HTTP probe
            # to fail before counting toward the outage threshold.
            _now = time.time()
            # Per-probe failure count this cycle. Falls back to the aggregate
            # http_ok flag when per-probe data isn't available.
            if http_results_snapshot:
                http_fail_count = sum(
                    1 for v in http_results_snapshot.values()
                    if not v.get("success", True)
                )
            else:
                http_fail_count = 0 if http_ok else 1
            # Full outage still trips on a single HTTP failure combined with
            # WAN ping down — that's a genuine connectivity loss signal.
            any_http_failed = http_fail_count >= 1 or not http_ok
            # Partial-HTTP (WAN-up) alerts require 2+ probes failing in the
            # same cycle — cuts noise from individual flaky endpoints.
            http_multi_failed = http_fail_count >= 2

            if not ping_ok and any_http_failed:
                _internet_state["consecutive_failures"] += 1
            else:
                if _internet_state["down"]:
                    # Internet just recovered
                    minutes = max(1, round((_now - _internet_state["down_since"]) / 60))
                    _notify_user(
                        "\u2713 Internet Restored",
                        f"WAN and HTTP connectivity recovered after "
                        f"{minutes} minute{'s' if minutes != 1 else ''}.",
                    )
                    log.info("Internet RESTORED after outage")
                _internet_state["consecutive_failures"] = 0
                _internet_state["down"] = False

            if _internet_state["consecutive_failures"] >= 3 and not _internet_state["down"]:
                _internet_state["down"] = True
                _internet_state["down_since"] = _now
                gw_status = "reachable" if gw_ping_ok else "unreachable"
                http_status = _format_http_probe_status(http_results_snapshot)
                _notify_user(
                    "\u26a0 Internet Down",
                    f"WAN ping failing. HTTP: {http_status}. "
                    f"Gateway {gw_status}.",
                )
                log.warning(f"Internet DOWN: WAN ping failing, HTTP: {http_status}")

            # ── HTTP-only partial-failure detection ──────────────────────
            # WAN ping is fine but 2+ HTTP probes are failing in the same
            # cycle. Fires only after 2 consecutive report cycles so we
            # don't alert on a single round of flaky probe responses.
            # Single-probe failures are logged per-probe but NOT alerted.
            # Skipped while WAN is also down — the full outage tracker
            # above owns that state.
            if ping_ok:
                if http_multi_failed:
                    _internet_state["http_partial_failures"] += 1
                else:
                    if _internet_state["http_partial_down"]:
                        _notify_user(
                            "\u2713 HTTP Probes Restored",
                            f"HTTP probe failures dropped below the "
                            f"2-domain alert threshold. "
                            f"{_format_http_probe_status(http_results_snapshot)}.",
                        )
                        log.info("HTTP probes RESTORED")
                    _internet_state["http_partial_failures"] = 0
                    _internet_state["http_partial_down"] = False

                if http_fail_count == 1:
                    # Single-domain failure — recorded but suppressed from
                    # alerts per the 2+ noise-reduction rule.
                    log.info(
                        f"HTTP single-probe failure (suppressed): "
                        f"{_format_http_probe_status(http_results_snapshot)}"
                    )

                if (
                    _internet_state["http_partial_failures"] >= 2
                    and not _internet_state["http_partial_down"]
                    and not _internet_state["down"]
                ):
                    _internet_state["http_partial_down"] = True
                    gw_status = "reachable" if gw_ping_ok else "unreachable"
                    http_status = _format_http_probe_status(http_results_snapshot)
                    _notify_user(
                        "\u26a0 HTTP Probe Alert",
                        f"{http_fail_count} HTTP probes failing. "
                        f"{http_status}. Gateway {gw_status}.",
                    )
                    log.warning(
                        f"HTTP partial failure ({http_fail_count} probes): "
                        f"{http_status}"
                    )
            # ── End internet-down detection ───────────────────────────────

            payload = {
                "hostname": os.environ.get("COMPUTERNAME", "windows-agent"),
                "agent_version": AGENT_VERSION,
                "ping_latency_ms": avg_ping,
                "ping_success": ping_ok,
                "gateway_ping_latency_ms": avg_gw_ping,
                "gateway_ping_success": gw_ping_ok,
                "http_latency_ms": http_lat,
                "http_success": http_ok,
                "http_results": http_results_snapshot,
                "http_probes": probe_urls_snapshot,
                "timestamp": datetime.utcnow().isoformat(),
                "sysinfo": dict(_sysinfo),
            }
            data = json.dumps(payload).encode("utf-8")
            # IPv4-forced POST — avoids Windows IPv6 stall
            ipv4 = socket.getaddrinfo(mac_ip, mac_port, socket.AF_INET)[0][4][0]
            conn = _hc.HTTPConnection(ipv4, mac_port, timeout=5)
            conn.request("POST", "/report", body=data,
                         headers={"Content-Type": "application/json", "User-Agent": "netmon-agent/1.0"})
            resp = conn.getresponse()
            if resp.status == 200:
                with state.lock:
                    state.mac_reachable = True
                    state.mac_fail_since = None  # reset on success
                log.debug(f"Report sent to Mac (HTTP {resp.status})")
            resp.read(); conn.close()
        except Exception as e:
            log.info(f"Report to Mac failed (unreachable?): {e}")
            with state.lock:
                if state.mac_fail_since is None:
                    state.mac_fail_since = datetime.now()
                # Only mark red after 60s of consecutive failures
                elapsed = (datetime.now() - state.mac_fail_since).total_seconds()
                if elapsed >= 60:
                    state.mac_reachable = False
        time.sleep(max(0.0, 10.0 - (time.monotonic() - t0)))

# ── Windows registry helpers ──────────────────────────────────────────────

_APP_NAME = "NetMonAgent"
_RUN_KEY = r"Software\Microsoft\Windows\CurrentVersion\Run"

def get_startup_enabled() -> bool:
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, _RUN_KEY) as k:
            winreg.QueryValueEx(k, _APP_NAME)
            return True
    except OSError:
        return False

def set_startup(enabled: bool) -> None:
    try:
        if enabled:
            if getattr(sys, "frozen", False):
                # Running as PyInstaller .exe
                exe = f'"{sys.executable}"'
            else:
                exe = f'"{sys.executable}" "{os.path.abspath(__file__)}"'
            with winreg.OpenKey(
                winreg.HKEY_CURRENT_USER, _RUN_KEY, 0, winreg.KEY_SET_VALUE
            ) as k:
                winreg.SetValueEx(k, _APP_NAME, 0, winreg.REG_SZ, exe)
            log.info(f"Startup registry entry added: {exe}")
        else:
            with winreg.OpenKey(
                winreg.HKEY_CURRENT_USER, _RUN_KEY, 0, winreg.KEY_SET_VALUE
            ) as k:
                winreg.DeleteValue(k, _APP_NAME)
            log.info("Startup registry entry removed")
    except Exception as e:
        log.error(f"Registry error: {e}")

# ── Tray icon image ───────────────────────────────────────────────────────

def _make_dot_image(hex_color: str, size: int = 64) -> Image.Image:
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    r = int(hex_color[1:3], 16)
    g = int(hex_color[3:5], 16)
    b = int(hex_color[5:7], 16)
    margin = 4
    draw.ellipse([margin, margin, size - margin, size - margin], fill=(r, g, b, 255))
    return img

# ── GUI ───────────────────────────────────────────────────────────────────

_BG = "#26215C"  # Langley Inc brand
_FG_DIM = "#a0a0c0"
_FG_LABEL = "#c0c0d8"
_INDICATOR_PX = 20
_FONT_SMALL = ("Segoe UI", 9)
_FONT_LABEL = ("Segoe UI", 9, "bold")

class NetMonWindow:
    def __init__(self, root: tk.Tk):
        self.root = root
        import socket as _s
        _hostname = _s.gethostname()
        self.root.title(f"LangNetmon v{AGENT_VERSION} — {_hostname} · Langley Inc")
        self.root.geometry("300x120")
        self.root.resizable(False, False)
        self.root.configure(bg=_BG)
        self.root.attributes("-topmost", True)

        self._always_on_top = tk.BooleanVar(value=True)
        self._startup_var = tk.BooleanVar(value=get_startup_enabled())

        self._build_indicators()
        self._build_time_label()
        self._build_checkboxes()

        self._tray_icon: Optional[pystray.Icon] = None
        self._prev_tray_color: str = C_GREEN  # track color changes for notifications
        threading.Thread(target=self._run_tray, daemon=True, name="tray").start()

        self.root.protocol("WM_DELETE_WINDOW", self._hide_window)
        self.root.bind("<Unmap>", self._on_minimize)
        self._schedule_update()

    # ── UI construction ───────────────────────────────────────────────────

    def _build_indicators(self):
        row = tk.Frame(self.root, bg=_BG)
        row.pack(side=tk.TOP, fill=tk.X, padx=8, pady=(8, 2))

        self._canvases: dict[str, tuple[tk.Canvas, int]] = {}
        for label in ("PING", "GW", "HTTP", "MAC"):
            col = tk.Frame(row, bg=_BG)
            col.pack(side=tk.LEFT, expand=True)
            size = _INDICATOR_PX + 4
            c = tk.Canvas(col, width=size, height=size, bg=_BG, highlightthickness=0)
            c.pack()
            oval_id = c.create_oval(
                2, 2, _INDICATOR_PX + 2, _INDICATOR_PX + 2,
                fill=C_GREY, outline="",
            )
            tk.Label(col, text=label, fg=_FG_LABEL, bg=_BG, font=_FONT_LABEL).pack()
            self._canvases[label] = (c, oval_id)

    def _build_time_label(self):
        self._ver_lbl = tk.Label(
            self.root, text=f"v{AGENT_VERSION}",
            fg=_FG_DIM, bg=_BG, font=_FONT_SMALL,
        )
        self._ver_lbl.pack(side=tk.TOP, pady=(0, 0))
        self._time_lbl = tk.Label(
            self.root, text="Updated: --:--:--",
            fg=_FG_DIM, bg=_BG, font=_FONT_SMALL,
        )
        self._time_lbl.pack(side=tk.TOP, pady=(0, 2))

    def _build_checkboxes(self):
        row = tk.Frame(self.root, bg=_BG)
        row.pack(side=tk.TOP, fill=tk.X, padx=4, pady=(0, 4))
        _cb_kw = dict(
            fg=_FG_LABEL, bg=_BG, activeforeground="#ffffff",
            activebackground=_BG, selectcolor=_BG, bd=0, font=_FONT_SMALL,
        )
        tk.Checkbutton(
            row, text="Always on top",
            variable=self._always_on_top,
            command=self._toggle_topmost,
            **_cb_kw,
        ).pack(side=tk.LEFT, padx=(2, 6))
        tk.Checkbutton(
            row, text="Start with Windows",
            variable=self._startup_var,
            command=self._toggle_startup,
            **_cb_kw,
        ).pack(side=tk.LEFT)

    # ── Callbacks ─────────────────────────────────────────────────────────

    def _toggle_topmost(self):
        self.root.attributes("-topmost", self._always_on_top.get())

    def _toggle_startup(self):
        set_startup(self._startup_var.get())

    def _show_window(self, icon=None, item=None):
        self.root.after(0, self.root.deiconify)
        self.root.after(0, self.root.lift)

    def _hide_window(self, icon=None, item=None):
        self.root.after(0, self.root.withdraw)

    def _on_minimize(self, event=None):
        """Intercept minimise — hide to tray instead of taskbar."""
        if event and event.widget == self.root:
            self.root.after(0, self.root.withdraw)

    def _quit(self, icon=None, item=None):
        if self._tray_icon:
            self._tray_icon.stop()
        self.root.after(0, self.root.destroy)

    # ── Tray ──────────────────────────────────────────────────────────────

    def _run_tray(self):
        global _tray_icon
        menu = pystray.Menu(
            pystray.MenuItem("Show",  self._show_window, default=True),
            pystray.MenuItem("Hide",  self._hide_window),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("Quit",  self._quit),
        )
        self._tray_icon = pystray.Icon(
            "NetMon",
            _make_dot_image(C_GREY),
            f"LangNetmon v{AGENT_VERSION}",
            menu=menu,
        )
        _tray_icon = self._tray_icon  # expose to module-level for background threads
        self._tray_icon.run()

    # ── Update loop ───────────────────────────────────────────────────────

    def _set_indicator(self, label: str, color: str):
        c, oval_id = self._canvases[label]
        c.itemconfig(oval_id, fill=color)

    def _schedule_update(self):
        self._do_update()
        self.root.after(1000, self._schedule_update)

    def _next_poll_in(self) -> int:
        """Seconds until the next command poll fires."""
        with state.lock:
            last = state.last_command_poll
        elapsed = (datetime.now() - last).total_seconds()
        interval = _POLL_DIAG if _diag_mode else _POLL_NORMAL
        remaining = max(0, int(interval - elapsed))
        return remaining

    def _do_update(self):
        try:
            with state.lock:
                pings = list(state.ping_results)
                gw_pings = list(state.gateway_ping_results)
                http_lat = state.http_latency_ms
                http_ok = state.http_success
                mac_ok = state.mac_reachable
                last_upd = state.last_update

            pc = ping_color(pings)
            gwc = ping_color(gw_pings)
            hc = http_color(http_lat, http_ok)
            mc = mac_color(mac_ok)

            self._set_indicator("PING", pc)
            self._set_indicator("GW", gwc)
            self._set_indicator("HTTP", hc)
            self._set_indicator("MAC", mc)
            countdown = self._next_poll_in()
            self._time_lbl.config(text=f"Next check in {countdown}s")

            if self._tray_icon is not None:
                tray_color = worst_color(pc, gwc, hc, mc)
                try:
                    self._tray_icon.icon = _make_dot_image(tray_color)
                except Exception:
                    pass

                # Count hard HTTP probe failures — alerts only fire when 2+
                # domains are failing simultaneously, to cut noise from a
                # single flaky endpoint.
                http_failed_domains = [
                    url for url, r in state.http_results.items()
                    if not r.get("success", True)
                ]
                http_alertable = len(http_failed_domains) >= 2

                # Fire a toast notification when status goes red and window is hidden
                window_hidden = not self.root.winfo_viewable()
                went_red = (tray_color == C_RED and self._prev_tray_color != C_RED)

                # If HTTP is the *only* red signal and fewer than 2 domains
                # are actually failing, suppress the toast. The tray color
                # still reflects the per-probe state, but we don't page on
                # a single-probe hiccup.
                only_http_red = (
                    hc == C_RED and pc != C_RED and gwc != C_RED and mc != C_RED
                )
                if went_red and only_http_red and not http_alertable:
                    went_red = False

                if went_red and window_hidden and self._tray_icon:
                    # Build a human-friendly message describing what went red
                    problems = []
                    if pc == C_RED:
                        lat = state.ping_latency_ms
                        problems.append(f"WAN ping {'timeout' if lat is None else f'{lat:.0f}ms'}")
                    if gwc == C_RED:
                        lat = state.gw_ping_latency_ms
                        problems.append(f"Gateway {'unreachable' if lat is None else f'{lat:.0f}ms'}")
                    if hc == C_RED and http_alertable:
                        failed = []
                        for url, r in state.http_results.items():
                            if not r.get("success", True):
                                domain = url.split("//")[-1].split("/")[0].replace("www.", "")
                                failed.append(domain)
                            elif r.get("latency_ms") and r["latency_ms"] > 500:
                                domain = url.split("//")[-1].split("/")[0].replace("www.", "")
                                failed.append(f"{domain} {r['latency_ms']:.0f}ms")
                        if failed:
                            problems.append(f"HTTP: {', '.join(failed)}")
                        else:
                            problems.append("HTTP checks failing")
                    if mc == C_RED:
                        problems.append("Mac Studio unreachable")
                    msg = " | ".join(problems) if problems else "Network issue detected"
                    try:
                        self._tray_icon.notify(msg, title="LangNetmon Alert")
                    except Exception:
                        pass
                self._prev_tray_color = tray_color
        except Exception as e:
            log.error(f"GUI update error: {e}")


# ── Version & auto-update ──────────────────────────────────────────────────

AGENT_VERSION = "2.5.0"


def _check_for_update(cfg: dict) -> None:
    """
    Hourly auto-update check against Mac daemon /version endpoint.
    If newer version found: downloads exe, launches bat to replace-and-restart.
    Only active when running as frozen PyInstaller exe.
    """
    if not getattr(sys, "frozen", False):
        log.info("Auto-update: skipped (not frozen exe)")
        return

    import tempfile
    mac_base = f"http://{cfg['mac_ip']}:{cfg['mac_port']}"
    version_url = f"{mac_base}/version"

    pass  # placeholder — update logic moved to _do_update_check()


def _ver_tuple(v):
    try:
        return tuple(int(x) for x in str(v).split("."))
    except Exception:
        return (0,)


def _mac_http_get_ipv4(mac_ip: str, mac_port: int, path: str, timeout: int = 10) -> bytes:
    """IPv4-forced HTTP GET to the Mac daemon — avoids Windows IPv6 stall."""
    import http.client as _hc
    ipv4 = socket.getaddrinfo(mac_ip, mac_port, socket.AF_INET)[0][4][0]
    conn = _hc.HTTPConnection(ipv4, mac_port, timeout=timeout)
    conn.request("GET", path, headers={"User-Agent": "netmon-agent/1.0"})
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return data


_UPDATE_DONE_THIS_SESSION: bool = False  # guard against update loops


def _do_update_check(cfg: dict) -> bool:
    """
    Check the Mac daemon for a newer agent version and self-update if found.
    Returns True if an update was triggered (process will restart).
    Only runs when frozen as a PyInstaller exe.
    """
    if not getattr(sys, "frozen", False):
        log.debug("Auto-update: skipped (not frozen exe)")
        return False

    global _UPDATE_DONE_THIS_SESSION
    if _UPDATE_DONE_THIS_SESSION:
        log.debug("Auto-update: already ran this session, skipping")
        return False

    import tempfile
    mac_ip   = cfg["mac_ip"]
    mac_port = int(cfg["mac_port"])

    try:
        raw = _mac_http_get_ipv4(mac_ip, mac_port, "/version", timeout=10)
        data = json.loads(raw)
        remote_version = data.get("version", "0")
        exe_url = data.get("url", "")

        if not exe_url or _ver_tuple(remote_version) <= _ver_tuple(AGENT_VERSION):
            log.debug(f"Auto-update: up to date (local={AGENT_VERSION} remote={remote_version})")
            return False

        log.info(f"Auto-update: new version {remote_version} available, downloading...")

        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".exe", dir=tempfile.gettempdir())
        # Download EXE — use IPv4-forced connection too
        ipv4 = socket.getaddrinfo(mac_ip, mac_port, socket.AF_INET)[0][4][0]
        import http.client as _hc
        conn = _hc.HTTPConnection(ipv4, mac_port, timeout=120)
        conn.request("GET", f"/agent/NetMonAgent.exe", headers={"User-Agent": "netmon-agent/1.0"})
        resp = conn.getresponse()
        exe_bytes = resp.read()
        conn.close()
        if len(exe_bytes) < 1_000_000:  # sanity check — valid exe must be >1MB
            log.error(f"Auto-update: download too small ({len(exe_bytes)} bytes) — aborting")
            tmp.close()
            os.unlink(tmp.name)
            return False
        tmp.write(exe_bytes)
        tmp.close()

        current_exe = sys.executable
        bat = tempfile.NamedTemporaryFile(
            delete=False, suffix=".bat", dir=tempfile.gettempdir(), mode="w"
        )
        bat.write(
            f"@echo off\r\n"
            f"timeout /t 3 /nobreak >nul 2>&1\r\n"
            f"copy /Y \"{tmp.name}\" \"{current_exe}\" >nul 2>&1\r\n"
            f"del \"{tmp.name}\" >nul 2>&1\r\n"
            f"start /b \"\" \"{current_exe}\"\r\n"
            f"(del \"%~f0\") >nul 2>&1\r\n"
        )
        bat.close()

        _si = subprocess.STARTUPINFO()
        _si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        _si.wShowWindow = subprocess.SW_HIDE
        subprocess.Popen(
            ["cmd", "/c", bat.name],
            startupinfo=_si,
            creationflags=subprocess.CREATE_NO_WINDOW | subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP,
            close_fds=True,
        )
        log.info(f"Auto-update: v{remote_version} launcher started, exiting")
        _UPDATE_DONE_THIS_SESSION = True
        os._exit(0)

    except Exception as e:
        log.debug(f"Auto-update check failed: {e}")
    return False


# ── Command poll (outbound-only diagnostics) ──────────────────────────────
# Agent polls Mac for pending commands every 30s. No inbound ports opened.
# Supported commands: dns, tcp, http, ping, tracert, ipconfig, state

_SHELL_WHITELIST = [
    "get-netadapter", "get-netipaddress", "get-netroute", "get-nettcpconnection",
    "get-dnsclient", "get-dnsclientcache", "clear-dnsclientcache",
    "get-netconnectionprofile", "get-wifisignal", "netsh wlan",
    "test-netconnection", "test-connection", "resolve-dnsname",
    "get-process", "get-service", "get-eventlog",
    "ipconfig", "arp", "route", "nslookup", "tracert", "ping",
    "get-childitem", "get-item", "get-content",
    "get-wmiobject win32_networkadapter", "get-wmiobject win32_computersystem",
    "get-computerinfo", "systeminfo",
    "get-volume", "get-disk", "get-physicaldisk",
    "get-counter", "get-date", "get-uptime",
    "whoami", "hostname", "ver",
]

# Blocked patterns — always rejected even if whitelist prefix matches
_SHELL_BLOCKED_PATTERNS = [
    r"\b(remove|delete|rm|del|format|erase)\b",
    r"\b(stop-service|restart-service|disable-service)\b",
    r"\b(net\s+stop|net\s+user|net\s+localgroup)\b",
    r"\b(reg\s+delete|reg\s+add)\b",
    r"\b(taskkill|kill)\b",
    r"\b(Invoke-WebRequest|Invoke-RestMethod|iwr|irm)\s+.*-OutFile",
    r"\b(Set-ExecutionPolicy)\b",
    r"\b(Start-Process)\b",
    r"\b(Out-File|Set-Content|Add-Content)[^|]*\b(C:\\|\\\\)\b",
    r"\b(New-Item)\s+.*-ItemType\s+(Directory|File)",
    r"\b(Invoke-Expression|iex)\b",
    r"\|\s*Out-File",
    r">>",
    r">",
]

_SHELL_MAX_CMD_LEN = 500
_SHELL_MAX_CONCURRENT = 2
_SHELL_TIMEOUT_SECONDS = 30
_SHELL_DRY_RUN_ENABLED = True


def _run_command(cmd: str, args: dict) -> dict:
    import subprocess, socket, ssl, http.client, time
    from urllib.parse import urlparse

    try:
        if cmd == "dns":
            host = args.get("host", "www.google.com")
            t0 = time.monotonic()
            ipv4 = socket.getaddrinfo(host, 80, socket.AF_INET)[0][4][0]
            dns_ms = round((time.monotonic() - t0) * 1000, 1)
            ipv6 = None
            try:
                ipv6 = socket.getaddrinfo(host, 80, socket.AF_INET6)[0][4][0]
            except Exception:
                pass
            return {"host": host, "ipv4": ipv4, "ipv6": ipv6, "dns_ms": dns_ms}

        elif cmd == "tcp":
            host = args.get("host", "www.google.com")
            port = int(args.get("port", 443))
            ip = socket.getaddrinfo(host, port, socket.AF_INET)[0][4][0]
            t0 = time.monotonic()
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(10)
            s.connect((ip, port))
            s.close()
            return {"host": host, "ip": ip, "port": port, "connect_ms": round((time.monotonic()-t0)*1000, 1)}

        elif cmd == "http":
            url = args.get("url", "https://www.google.com")
            parsed = urlparse(url)
            hostname = parsed.hostname
            port = parsed.port or (443 if parsed.scheme == "https" else 80)
            t_dns = time.monotonic()
            ipv4 = socket.getaddrinfo(hostname, port, socket.AF_INET)[0][4][0]
            dns_ms = round((time.monotonic() - t_dns) * 1000, 1)
            t0 = time.monotonic()
            if parsed.scheme == "https":
                ctx = ssl.create_default_context()
                conn = http.client.HTTPSConnection(ipv4, port, timeout=15, context=ctx)
                conn.set_tunnel(hostname)
            else:
                conn = http.client.HTTPConnection(ipv4, port, timeout=15)
            conn.request("GET", parsed.path or "/", headers={"Host": hostname, "User-Agent": "netmon-diag/1.0"})
            resp = conn.getresponse()
            total_ms = round((time.monotonic() - t0) * 1000, 1)
            resp.read(); conn.close()
            return {"url": url, "ip": ipv4, "dns_ms": dns_ms, "total_ms": total_ms, "status": resp.status}

        elif cmd == "ping":
            host = args.get("host", "8.8.8.8")
            count = min(int(args.get("count", 4)), 10)
            proc = subprocess.run(
                ["ping", "-n", str(count), host],
                capture_output=True, text=True, timeout=30,
                creationflags=subprocess.CREATE_NO_WINDOW
            )
            return {"host": host, "output": proc.stdout}

        elif cmd == "tracert":
            host = args.get("host", "8.8.8.8")
            proc = subprocess.run(
                ["tracert", "-d", "-w", "1000", "-h", "15", host],
                capture_output=True, text=True, timeout=60,
                creationflags=subprocess.CREATE_NO_WINDOW
            )
            return {"host": host, "output": proc.stdout}

        elif cmd == "ipconfig":
            proc = subprocess.run(
                ["ipconfig", "/all"],
                capture_output=True, text=True, timeout=10,
                creationflags=subprocess.CREATE_NO_WINDOW
            )
            return {"output": proc.stdout}

        elif cmd == "state":
            with state.lock:
                return {
                    "version": AGENT_VERSION,
                    "ping_ms": state.ping_latency_ms,
                    "gw_ping_ms": state.gw_ping_latency_ms,
                    "http_latency_ms": state.http_latency_ms,
                    "http_success": state.http_success,
                    "ping_ok": state.ping_success,
                    "gw_ok": state.gw_ping_success,
                }

        elif cmd == "sysinfo":
            return dict(_sysinfo)

        elif cmd == "shell":
            ps_cmd = args.get("cmd", "").strip()
            if not ps_cmd:
                return {"error": "no command provided"}

            # Length limit
            if len(ps_cmd) > _SHELL_MAX_CMD_LEN:
                return {"error": f"command too long ({len(ps_cmd)} chars, max {_SHELL_MAX_CMD_LEN})"}

            # Whitelist check — case-insensitive prefix match
            ps_lower = ps_cmd.lower().strip()
            allowed = any(ps_lower.startswith(w) for w in _SHELL_WHITELIST)
            if not allowed:
                return {
                    "error": f"command not in whitelist: {ps_cmd[:80]}",
                    "allowed": _SHELL_WHITELIST,
                }

            # Blocked pattern check — catch destructive commands even if whitelist matches
            import re as _re
            for pattern in _SHELL_BLOCKED_PATTERNS:
                if _re.search(pattern, ps_lower):
                    return {
                        "error": f"command blocked by safety pattern: {ps_cmd[:80]}",
                        "blocked_pattern": pattern,
                    }

            # Concurrency guard
            with state.lock:
                active = getattr(state, '_shell_active_count', 0)
                if active >= _SHELL_MAX_CONCURRENT:
                    return {"error": f"too many concurrent shell commands ({active} running, max {_SHELL_MAX_CONCURRENT})"}
                state._shell_active_count = active + 1

            try:
                # Dry-run mode: parse the command without executing to verify syntax
                if _SHELL_DRY_RUN_ENABLED:
                    try:
                        dry_proc = subprocess.run(
                            ["powershell", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
                             "-Command", f"[scriptblock]::Create({json.dumps(ps_cmd)}) | Out-Null"],
                            capture_output=True, text=True, timeout=10,
                            creationflags=subprocess.CREATE_NO_WINDOW,
                        )
                        if dry_proc.returncode != 0 and "ParseException" in dry_proc.stderr:
                            return {"error": f"syntax error in command: {dry_proc.stderr[:200]}"}
                    except subprocess.TimeoutExpired:
                        pass  # dry-run timeout is ok, proceed to real exec
                    except Exception:
                        pass  # dry-run failure is non-fatal

                proc = subprocess.run(
                    ["powershell", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
                     "-Command", ps_cmd],
                    capture_output=True, text=True, timeout=_SHELL_TIMEOUT_SECONDS,
                    creationflags=subprocess.CREATE_NO_WINDOW,
                )
                # Truncate output to prevent memory issues
                stdout = proc.stdout.strip()[:50000]
                stderr = proc.stderr.strip()[:10000]
                return {
                    "stdout": stdout,
                    "stderr": stderr,
                    "returncode": proc.returncode,
                }
            except subprocess.TimeoutExpired:
                return {"error": f"command timed out after {_SHELL_TIMEOUT_SECONDS}s"}
            except Exception as e:
                return {"error": str(e)}
            finally:
                with state.lock:
                    state._shell_active_count = max(0, getattr(state, '_shell_active_count', 1) - 1)

        else:
            return {"error": f"unknown command: {cmd}"}

    except Exception as e:
        return {"error": str(e)}


# Diag mode flag — set to True when Mac sends "diag_start" command
_diag_mode = False
_POLL_NORMAL  = 15   # seconds between polls in normal mode
_POLL_DIAG    = 5    # seconds between polls in diag mode


# ── PowerShell remote session (long-poll) ────────────────────────────────
# Agent keeps one persistent long-poll open to the Mac at all times.
# Mac holds the request for up to 50s. When TARS queues a PS script via
# POST /ps, the Mac flushes it down the open connection and the agent
# runs it immediately, posts the result, then reconnects.
# Round-trip latency: ~1-3s (execution time only).
# Zero inbound ports opened on Windows.

def _run_ps(script: str) -> tuple[str, str, int]:
    """Run a PowerShell script, return (stdout, stderr, exit_code)."""
    # Safety: reject overly long scripts
    if len(script) > 5000:
        return "", f"Script too long ({len(script)} chars, max 5000)", 1
    # Safety: reject destructive patterns
    import re as _re
    _PS_BLOCKED = [r"\b(Remove-Item|del|rm|erase)\b", r"\b(Stop-Service|Restart-Service)\b",
                   r"\b(taskkill)\b", r"\b(Invoke-Expression|iex)\b", r"\b(format)\b",
                   r"\b(Start-Process)\b", r">>"]
    script_lower = script.lower()
    for pat in _PS_BLOCKED:
        if _re.search(pat, script_lower):
            return "", f"Script blocked by safety pattern: {pat}", 1
    try:
        result = subprocess.run(
            [
                "powershell.exe",
                "-NonInteractive",
                "-NoProfile",
                "-ExecutionPolicy", "Bypass",
                "-Command", script,
            ],
            capture_output=True,
            text=True,
            timeout=60,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        # Truncate output
        stdout = result.stdout[:50000]
        stderr = result.stderr[:10000]
        return stdout, stderr, result.returncode
    except subprocess.TimeoutExpired:
        return "", "PowerShell execution timed out (60s)", 1
    except Exception as e:
        return "", f"PS exec error: {e}", 1


def _ps_http_request(mac_ip: str, mac_port: int, method: str, path: str,
                     body: Optional[bytes] = None, timeout: int = 58) -> bytes:
    """
    Low-level IPv4-forced HTTP helper for the PS session loop.
    Uses http.client.HTTPConnection directly to bypass urllib's IPv6 preference.
    """
    import http.client as _hc
    # Force IPv4 resolution — avoids Windows IPv6 stall (~80s)
    ipv4 = socket.getaddrinfo(mac_ip, mac_port, socket.AF_INET)[0][4][0]
    conn = _hc.HTTPConnection(ipv4, mac_port, timeout=timeout)
    headers = {"User-Agent": "netmon-agent/1.0"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    conn.request(method, path, body=body, headers=headers)
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return data


def ps_session_loop(cfg: dict) -> None:
    """
    Persistent long-poll loop for remote PowerShell execution.
    Opens GET /ps_session?hostname=X — Mac holds it open until a script
    is queued. Agent receives script, runs it, POSTs result to /ps_result,
    then immediately reconnects. Falls back to 5s retry on errors.
    Uses IPv4-forced http.client to avoid Windows IPv6 stall.
    """
    import socket as _sock
    hostname = _sock.gethostname()
    mac_ip   = cfg["mac_ip"]
    mac_port = int(cfg["mac_port"])

    log.info(f"PS session loop started → {mac_ip}:{mac_port}")

    while True:
        try:
            # Long-poll: server holds up to 50s, we give it 58s
            raw = _ps_http_request(
                mac_ip, mac_port, "GET",
                f"/ps_session?hostname={hostname}",
                timeout=58,
            )
            data = json.loads(raw)
            cmd = data.get("cmd")

            if cmd == "noop":
                # Server timeout — reconnect immediately
                continue

            if cmd == "ps_exec":
                cid    = data.get("id", "?")
                script = data.get("script", "")
                log.info(f"PS exec [{cid}]: {script[:80]}")

                stdout, stderr, exit_code = _run_ps(script)

                payload = json.dumps({
                    "hostname":  hostname,
                    "id":        cid,
                    "script":    script,
                    "stdout":    stdout,
                    "stderr":    stderr,
                    "exit_code": exit_code,
                }).encode()

                _ps_http_request(mac_ip, mac_port, "POST", "/ps_result",
                                 body=payload, timeout=10)
                log.info(f"PS result [{cid}] posted (exit={exit_code})")
                # Reconnect immediately for next command
                continue

        except Exception as e:
            log.warning(f"PS session error: {e} — reconnecting in 5s")
            time.sleep(5)


def _collect_diag_snapshot() -> dict:
    """Auto-collected state snapshot sent every poll in diag mode."""
    with state.lock:
        snap = {
            "ping_ms":        state.ping_latency_ms,
            "ping_ok":        state.ping_success,
            "gw_ping_ms":     state.gw_ping_latency_ms,
            "gw_ok":          state.gw_ping_success,
            "http_latency_ms": state.http_latency_ms,
            "http_success":   state.http_success,
        }
    return snap


def command_poll_loop(cfg: dict) -> None:
    """
    Poll Mac daemon for pending diagnostic commands. Outbound only — no inbound ports.
    Normal mode: 30s interval. Also checks for updates on every normal-mode poll.
    Diag mode (triggered by diag_start command): 5s interval + auto state snapshots.
    Returns to normal mode on diag_stop command.
    """
    global _diag_mode
    import socket as _sock
    import http.client as _hc
    hostname = _sock.gethostname()
    mac_ip   = cfg["mac_ip"]
    mac_port = int(cfg["mac_port"])

    def _ipv4_get(path: str, timeout: int = 10) -> bytes:
        """IPv4-forced GET to Mac daemon."""
        ipv4 = socket.getaddrinfo(mac_ip, mac_port, socket.AF_INET)[0][4][0]
        conn = _hc.HTTPConnection(ipv4, mac_port, timeout=timeout)
        conn.request("GET", path, headers={"User-Agent": "netmon-agent/1.0"})
        resp = conn.getresponse(); data = resp.read(); conn.close()
        return data

    def _ipv4_post(path: str, payload: bytes, timeout: int = 10) -> None:
        """IPv4-forced POST to Mac daemon."""
        ipv4 = socket.getaddrinfo(mac_ip, mac_port, socket.AF_INET)[0][4][0]
        conn = _hc.HTTPConnection(ipv4, mac_port, timeout=timeout)
        conn.request("POST", path, body=payload,
                     headers={"Content-Type": "application/json", "User-Agent": "netmon-agent/1.0"})
        resp = conn.getresponse(); resp.read(); conn.close()

    poll_path   = f"/commands?hostname={hostname}"
    result_path = "/command_result"

    def post_result(cmd_id, cmd, result):
        payload = json.dumps({
            "id": cmd_id,
            "hostname": hostname,
            "cmd": cmd,
            "result": result,
        }).encode()
        _ipv4_post(result_path, payload)

    # Startup update check — single attempt only to avoid update loops
    log.info("Auto-update: checking on startup...")
    _do_update_check(cfg)  # exits process if update triggered

    while True:
        interval = _POLL_DIAG if _diag_mode else _POLL_NORMAL
        with state.lock:
            state.last_command_poll = datetime.now()
        time.sleep(interval)

        # Check for updates on every normal-mode poll (every 30s)
        if not _diag_mode:
            _do_update_check(cfg)  # exits process if update found
        try:
            commands = json.loads(
                _ipv4_get(poll_path + f"&diag={'1' if _diag_mode else '0'}")
            )

            # In diag mode, auto-send a state snapshot every poll even if no commands
            if _diag_mode:
                snap = _collect_diag_snapshot()
                post_result("auto-snap", "state", snap)
                log.debug(f"Diag snapshot sent: {snap}")

            for item in commands:
                cmd_id = item.get("id")
                cmd    = item.get("cmd")
                args   = item.get("args", {})

                # Control commands — handled locally, no result needed
                if cmd == "diag_start":
                    _diag_mode = True
                    log.info("Diag mode ENABLED — polling every 5s")
                    post_result(cmd_id, cmd, {"status": "diag mode enabled", "poll_interval": _POLL_DIAG})
                    continue
                elif cmd == "diag_stop":
                    _diag_mode = False
                    log.info("Diag mode DISABLED — polling every 30s")
                    post_result(cmd_id, cmd, {"status": "diag mode disabled", "poll_interval": _POLL_NORMAL})
                    continue

                log.info(f"Running command [{cmd_id}]: {cmd} {args}")
                result = _run_command(cmd, args)
                post_result(cmd_id, cmd, result)
                log.info(f"Command {cmd_id} result posted")

        except Exception as e:
            log.debug(f"Command poll error: {e}")


# ── Folder sync (Task 3 — gated by hostname) ──────────────────────────────
# Walks configured sync roots hourly, builds a SHA-256 + mtime + size
# manifest, exchanges directives with the Mac daemon, and uploads/downloads
# newer files. HARD EXCLUSION: config.wtf is never uploaded — it's per-machine
# graphics settings and mixing them corrupts clients.

def _sync_is_excluded(rel_path: str, exclusions: list) -> bool:
    """True if the file should never be synced. Defence in depth."""
    name = os.path.basename(rel_path).lower()
    if name == "config.wtf":
        return True
    for ex in exclusions or []:
        if name == ex.lower() or rel_path.lower().endswith(ex.lower()):
            return True
    return False


def _sync_walk_manifest(root: str, exclusions: list) -> dict:
    """Walk `root`, return {rel_path: {sha256, mtime, size}}. Silent on IO errors."""
    import hashlib
    manifest = {}
    if not os.path.isdir(root):
        return manifest
    for dirpath, dirnames, filenames in os.walk(root):
        for fn in filenames:
            full = os.path.join(dirpath, fn)
            rel = os.path.relpath(full, root).replace("\\", "/")
            if _sync_is_excluded(rel, exclusions):
                continue
            try:
                st = os.stat(full)
                h = hashlib.sha256()
                with open(full, "rb") as fh:
                    for chunk in iter(lambda: fh.read(65536), b""):
                        h.update(chunk)
                manifest[rel] = {
                    "sha256": h.hexdigest(),
                    "mtime": st.st_mtime,
                    "size": st.st_size,
                }
            except Exception as e:
                log.debug(f"Sync skip {full}: {e}")
    return manifest


def _sync_http_post_json(mac_ip: str, mac_port: int, path: str, body: dict, timeout: int = 30) -> dict:
    import http.client as _hc
    ipv4 = socket.getaddrinfo(mac_ip, mac_port, socket.AF_INET)[0][4][0]
    conn = _hc.HTTPConnection(ipv4, mac_port, timeout=timeout)
    data = json.dumps(body).encode("utf-8")
    conn.request("POST", path, body=data,
                 headers={"Content-Type": "application/json", "User-Agent": "netmon-agent/1.0"})
    resp = conn.getresponse()
    raw = resp.read()
    conn.close()
    if resp.status != 200:
        raise RuntimeError(f"POST {path} -> HTTP {resp.status}: {raw[:200]}")
    return json.loads(raw) if raw else {}


def _sync_http_post_file(mac_ip: str, mac_port: int, path: str, file_bytes: bytes,
                         headers: dict, timeout: int = 120) -> dict:
    import http.client as _hc
    ipv4 = socket.getaddrinfo(mac_ip, mac_port, socket.AF_INET)[0][4][0]
    conn = _hc.HTTPConnection(ipv4, mac_port, timeout=timeout)
    merged = {"Content-Type": "application/octet-stream",
              "User-Agent": "netmon-agent/1.0"}
    merged.update(headers)
    conn.request("POST", path, body=file_bytes, headers=merged)
    resp = conn.getresponse()
    raw = resp.read()
    conn.close()
    if resp.status != 200:
        raise RuntimeError(f"Upload {path} -> HTTP {resp.status}: {raw[:200]}")
    return json.loads(raw) if raw else {}


def _sync_http_get(mac_ip: str, mac_port: int, path: str, timeout: int = 120) -> tuple[int, bytes]:
    import http.client as _hc
    ipv4 = socket.getaddrinfo(mac_ip, mac_port, socket.AF_INET)[0][4][0]
    conn = _hc.HTTPConnection(ipv4, mac_port, timeout=timeout)
    conn.request("GET", path, headers={"User-Agent": "netmon-agent/1.0"})
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, data


def _sync_cache_dir() -> Path:
    """Directory for local manifest cache. Prefers %LOCALAPPDATA%\\LangNetmon,
    falls back to the agent's own dir if LOCALAPPDATA is unavailable."""
    base = os.environ.get("LOCALAPPDATA")
    if base:
        d = Path(base) / "LangNetmon"
    else:
        d = _HERE
    try:
        d.mkdir(parents=True, exist_ok=True)
    except Exception:
        d = _HERE
    return d


def _sync_cache_path(target: str) -> Path:
    return _sync_cache_dir() / f"sync_cache_{target}.json"


def _sync_load_cached_manifest(target: str) -> Optional[dict]:
    p = _sync_cache_path(target)
    if not p.exists():
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except Exception as e:
        log.debug(f"Sync cache [{target}] read failed: {e}")
        return None


def _sync_save_cached_manifest(target: str, manifest: dict) -> None:
    p = _sync_cache_path(target)
    try:
        tmp = p.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(manifest, f)
        os.replace(tmp, p)
    except Exception as e:
        log.debug(f"Sync cache [{target}] write failed: {e}")


def _sync_one_target(cfg: dict, hostname: str, target: str, root: str) -> None:
    """Run one sync round for a single target path."""
    exclusions = cfg.get("sync_exclusions", list(SYNC_EXCLUSIONS))
    manifest = _sync_walk_manifest(root, exclusions)
    log.info(f"Sync [{target}] manifest: {len(manifest)} files from {root}")

    # Local manifest cache: if the manifest is identical to the one saved on
    # the previous sync run, nothing changed locally — skip the server round
    # trip entirely. Massive win for WoW addons (~17k files).
    cached = _sync_load_cached_manifest(target)
    if cached is not None and cached == manifest:
        log.info(f"Sync [{target}] local manifest unchanged since last run — skipping server POST")
        return

    try:
        directives = _sync_http_post_json(
            cfg["mac_ip"], int(cfg["mac_port"]),
            "/api/sync/manifest",
            {"hostname": hostname, "target": target, "manifest": manifest},
            timeout=60,
        )
    except Exception as e:
        log.warning(f"Sync [{target}] manifest exchange failed: {e}")
        return

    uploads = directives.get("upload", []) or []
    downloads = directives.get("download", []) or []
    log.info(f"Sync [{target}] directives: upload={len(uploads)} download={len(downloads)}")

    # Friendly target names for user-facing toasts.
    _sync_friendly = {
        "wow-addons": "WoW Addons",
        "wow-wtf": "WoW Settings",
        "old-cylance": "Cylance Backup",
    }
    friendly = _sync_friendly.get(target, target)

    # Uploads: files where our copy is newer.
    if uploads:
        _notify_user("LangNetmon Sync", f"Syncing {friendly} — uploading {len(uploads)} new files")
    uploads_ok = 0
    for rel in uploads:
        if _sync_is_excluded(rel, exclusions):
            log.info(f"Sync [{target}] HARD-SKIP upload of excluded file: {rel}")
            continue
        full = os.path.join(root, rel.replace("/", os.sep))
        if not os.path.isfile(full):
            continue
        try:
            with open(full, "rb") as fh:
                payload = fh.read()
            meta = manifest.get(rel, {})
            _sync_http_post_file(
                cfg["mac_ip"], int(cfg["mac_port"]),
                "/api/sync/upload",
                payload,
                headers={
                    "X-Sync-Hostname": hostname,
                    "X-Sync-Target": target,
                    "X-Sync-Path": rel,
                    "X-Sync-Mtime": str(meta.get("mtime", time.time())),
                    "X-Sync-SHA256": meta.get("sha256", ""),
                },
            )
            log.info(f"Sync [{target}] uploaded {rel} ({len(payload)} bytes)")
            uploads_ok += 1
        except Exception as e:
            log.warning(f"Sync [{target}] upload failed for {rel}: {e}")
    if uploads:
        _notify_user("LangNetmon Sync", f"Syncing {friendly} — {uploads_ok} files uploaded successfully")

    # Downloads: canonical copy is newer.
    if downloads:
        _notify_user("LangNetmon Sync", f"Syncing {friendly} — downloading {len(downloads)} files")
    downloads_ok = 0
    for rel in downloads:
        if _sync_is_excluded(rel, exclusions):
            log.info(f"Sync [{target}] HARD-SKIP download of excluded file: {rel}")
            continue
        try:
            from urllib.parse import quote
            q = f"/api/sync/download?target={quote(target)}&path={quote(rel)}"
            status, data = _sync_http_get(cfg["mac_ip"], int(cfg["mac_port"]), q)
            if status != 200:
                log.warning(f"Sync [{target}] download {rel} -> HTTP {status}")
                continue
            full = os.path.join(root, rel.replace("/", os.sep))
            os.makedirs(os.path.dirname(full), exist_ok=True)
            tmp = full + ".sync_tmp"
            with open(tmp, "wb") as fh:
                fh.write(data)
            os.replace(tmp, full)
            log.info(f"Sync [{target}] downloaded {rel} ({len(data)} bytes)")
            downloads_ok += 1
        except Exception as e:
            log.warning(f"Sync [{target}] download failed for {rel}: {e}")
    if downloads:
        if downloads_ok == len(downloads):
            _notify_user("LangNetmon Sync", f"Syncing {friendly} — {downloads_ok} files downloaded successfully")
        else:
            _notify_user("LangNetmon Sync", f"Syncing {friendly} — {downloads_ok} of {len(downloads)} files downloaded")

    # Cache the manifest we just exchanged with the server. When downloads
    # happened, local files changed so this manifest no longer matches disk —
    # skip caching and let the next run re-walk + re-POST to re-establish.
    if not downloads:
        _sync_save_cached_manifest(target, manifest)


def sync_loop(cfg: dict) -> None:
    """
    Folder sync. Gated: only runs on hosts listed in sync_enabled_hosts.
    Runs immediately at startup after a short 30s warm-up for the network,
    then repeats on the hourly interval.
    """
    hostname = socket.gethostname()
    enabled_hosts = [h.upper() for h in cfg.get("sync_enabled_hosts", [])]
    if hostname.upper() not in enabled_hosts:
        log.info(f"Sync loop: host {hostname} NOT in sync_enabled_hosts — idle")
        while True:
            time.sleep(3600)  # stay alive, do nothing
        return

    interval = int(cfg.get("sync_interval_seconds", SYNC_INTERVAL_SECONDS))
    log.info(f"Sync loop started for {hostname} — interval {interval}s (first run after 30s warm-up)")
    time.sleep(30)  # brief warm-up so network is ready, then sync immediately

    while True:
        t0 = time.monotonic()
        try:
            paths = cfg.get("sync_paths", SYNC_PATHS)

            def _run_target(tgt: str, rt: str) -> None:
                try:
                    _sync_one_target(cfg, hostname, tgt, rt)
                except Exception as e:
                    log.warning(f"Sync [{tgt}] round error: {e}")

            threads = []
            for target, root in paths.items():
                th = threading.Thread(
                    target=_run_target, args=(target, root),
                    name=f"sync-{target}", daemon=True,
                )
                th.start()
                threads.append((target, th))
            for target, th in threads:
                th.join(timeout=300)
                if th.is_alive():
                    log.warning(f"Sync [{target}] thread still running after 300s join timeout — "
                                f"continuing loop; thread will be abandoned")
        except Exception as e:
            log.error(f"Sync loop error: {e}")
        elapsed = time.monotonic() - t0
        time.sleep(max(60.0, interval - elapsed))


# ── Entry point ───────────────────────────────────────────────────────────

def _spawn_resilient(name: str, target, args: tuple) -> threading.Thread:
    """Wrap a thread so any crash auto-restarts it after 5s."""
    def _wrapper():
        while True:
            try:
                target(*args)
            except Exception as exc:
                log.error(f"Thread '{name}' crashed: {exc} — restarting in 5s")
                time.sleep(5)
    t = threading.Thread(target=_wrapper, name=name, daemon=True)
    t.start()
    return t


def main() -> None:
    _enforce_single_instance()  # exits immediately if another instance is running
    service_mode = "--service" in sys.argv
    log.info(f"NetMon Windows Agent v{AGENT_VERSION} starting "
             f"({'service mode — headless' if service_mode else 'GUI mode'})")
    cfg = load_config()
    log.info(f"Mac target: {cfg['mac_ip']}:{cfg['mac_port']}")

    # Collect sysinfo once at startup so first report has full data
    global _sysinfo
    _sysinfo = _collect_sysinfo()
    log.info(f"Sysinfo: type={_sysinfo.get('connection_type')} ssid={_sysinfo.get('wifi_ssid')} ip={_sysinfo.get('ip_address')}")

    thread_specs = [
        ("ping",       ping_loop,         ()),
        ("gw-ping",    gateway_ping_loop,  ()),
        ("http",       http_loop,          (cfg,)),
        ("report",     report_loop,        (cfg,)),
        ("cmd-poll",   command_poll_loop,  (cfg,)),
        ("ps-session", ps_session_loop,    (cfg,)),
        ("sysinfo",    _sysinfo_loop,      ()),
        ("sync",       sync_loop,          (cfg,)),
    ]
    live_threads = {name: _spawn_resilient(name, fn, args) for name, fn, args in thread_specs}

    # Thread watchdog — restarts any dead thread every 30s
    def _watchdog():
        while True:
            time.sleep(30)
            for name, fn, args in thread_specs:
                t = live_threads.get(name)
                if t is None or not t.is_alive():
                    log.warning(f"Watchdog: thread '{name}' dead — respawning")
                    live_threads[name] = _spawn_resilient(name, fn, args)
    threading.Thread(target=_watchdog, name="watchdog", daemon=True).start()

    if service_mode:
        # Headless — no tkinter, no tray. Block forever; watchdog keeps things alive.
        try:
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            pass
        log.info("NetMon Windows Agent (service) stopped")
        return

    root = tk.Tk()
    _app = NetMonWindow(root)

    try:
        root.mainloop()
    except KeyboardInterrupt:
        pass
    log.info("NetMon Windows Agent stopped")


if __name__ == "__main__":
    # If running under bootstrap, check for hot-reload stop signal
    _stop = globals().get('__bootstrap_stop_event__')
    if _stop and _stop.is_set():
        log.info("Bootstrap requested exit for hot-reload")
        sys.exit(0)
    main()
