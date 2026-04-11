#!/usr/bin/env python3
"""
NetMon Windows Agent
Monitors network health, displays a floating status window,
and reports metrics to the Mac Studio daemon.
"""

import json
import logging
import os
import re
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

import tkinter as tk
import pystray
from PIL import Image, ImageDraw

# ── Paths & config ─────────────────────────────────────────────────────────

_HERE = Path(__file__).parent
CONFIG_PATH = _HERE / "config.json"
LOG_PATH = _HERE / "netmon_agent.log"

DEFAULT_CONFIG = {
    "mac_ip": "192.168.1.161",
    "mac_port": 9876,
}

def load_config() -> dict:
    cfg = DEFAULT_CONFIG.copy()
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, encoding="utf-8") as f:
                cfg.update(json.load(f))
        except Exception as e:
            logging.warning(f"Failed to load config: {e}")
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
        self.mac_reachable: bool = False
        self.last_update: datetime = datetime.now()

state = _State()

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
    if latency_ms > 250:
        return C_RED
    if latency_ms > 150:
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
            conn = http.client.HTTPSConnection(ipv4, port, timeout=10, context=ctx)
            conn.set_tunnel(hostname)  # SNI + Host header
        else:
            conn = http.client.HTTPConnection(ipv4, port, timeout=10)

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

def http_loop() -> None:
    log.info("HTTP check loop started -> google.com every 5s (IPv4-forced)")
    while True:
        t0 = time.monotonic()
        try:
            latency, success = _http_check()
            with state.lock:
                state.http_latency_ms = latency
                state.http_success = success
            log.debug(f"HTTP result: {'OK' if success else 'FAIL'} {latency}ms")
        except Exception as e:
            log.error(f"HTTP loop error: {e}")
        time.sleep(max(0.0, 5.0 - (time.monotonic() - t0)))

# ── Report to Mac ─────────────────────────────────────────────────────────

def report_loop(cfg: dict) -> None:
    mac_url = f"http://{cfg['mac_ip']}:{cfg['mac_port']}/report"
    log.info(f"Report loop started → {mac_url} every 10s")
    while True:
        t0 = time.monotonic()
        try:
            with state.lock:
                results = list(state.ping_results)
                gw_results = list(state.gateway_ping_results)
                http_lat = state.http_latency_ms
                http_ok = state.http_success

            lats = [r["latency_ms"] for r in results if r["success"] and r["latency_ms"] is not None]
            avg_ping = sum(lats) / len(lats) if lats else None
            ping_ok = any(r["success"] for r in results) if results else False

            gw_lats = [r["latency_ms"] for r in gw_results if r["success"] and r["latency_ms"] is not None]
            avg_gw_ping = sum(gw_lats) / len(gw_lats) if gw_lats else None
            gw_ping_ok = any(r["success"] for r in gw_results) if gw_results else False

            payload = {
                "hostname": os.environ.get("COMPUTERNAME", "windows-agent"),
                "ping_latency_ms": avg_ping,
                "ping_success": ping_ok,
                "gateway_ping_latency_ms": avg_gw_ping,
                "gateway_ping_success": gw_ping_ok,
                "http_latency_ms": http_lat,
                "http_success": http_ok,
                "timestamp": datetime.utcnow().isoformat(),
            }
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                mac_url, data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                reachable = resp.status == 200
                with state.lock:
                    state.mac_reachable = reachable
                log.debug(f"Report sent to Mac (HTTP {resp.status})")
        except Exception as e:
            log.info(f"Report to Mac failed (unreachable?): {e}")
            with state.lock:
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

_BG = "#1e1e1e"
_FG_DIM = "#888888"
_FG_LABEL = "#aaaaaa"
_INDICATOR_PX = 18
_FONT_SMALL = ("Segoe UI", 7)
_FONT_LABEL = ("Segoe UI", 7, "bold")

class NetMonWindow:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Net Monitor")
        self.root.geometry("260x95")
        self.root.resizable(False, False)
        self.root.configure(bg=_BG)
        self.root.attributes("-topmost", True)

        self._always_on_top = tk.BooleanVar(value=True)
        self._startup_var = tk.BooleanVar(value=get_startup_enabled())

        self._build_indicators()
        self._build_time_label()
        self._build_checkboxes()

        self._tray_icon: Optional[pystray.Icon] = None
        threading.Thread(target=self._run_tray, daemon=True, name="tray").start()

        self.root.protocol("WM_DELETE_WINDOW", self._hide_window)
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

    def _quit(self, icon=None, item=None):
        if self._tray_icon:
            self._tray_icon.stop()
        self.root.after(0, self.root.destroy)

    # ── Tray ──────────────────────────────────────────────────────────────

    def _run_tray(self):
        menu = pystray.Menu(
            pystray.MenuItem("Show",  self._show_window, default=True),
            pystray.MenuItem("Hide",  self._hide_window),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("Quit",  self._quit),
        )
        self._tray_icon = pystray.Icon(
            "NetMon",
            _make_dot_image(C_GREY),
            "Net Monitor",
            menu=menu,
        )
        self._tray_icon.run()

    # ── Update loop ───────────────────────────────────────────────────────

    def _set_indicator(self, label: str, color: str):
        c, oval_id = self._canvases[label]
        c.itemconfig(oval_id, fill=color)

    def _schedule_update(self):
        self._do_update()
        self.root.after(1000, self._schedule_update)

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
            self._time_lbl.config(text=f"Updated: {last_upd.strftime('%H:%M:%S')}")

            if self._tray_icon is not None:
                tray_color = worst_color(pc, gwc, hc, mc)
                try:
                    self._tray_icon.icon = _make_dot_image(tray_color)
                except Exception:
                    pass
        except Exception as e:
            log.error(f"GUI update error: {e}")


# ── Version & auto-update ──────────────────────────────────────────────────

AGENT_VERSION = "1.4"


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

    def ver_tuple(v):
        try:
            return tuple(int(x) for x in str(v).split("."))
        except Exception:
            return (0,)

    while True:
        time.sleep(3600)
        try:
            req = urllib.request.Request(version_url, headers={"User-Agent": "netmon-agent/1.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
            remote_version = data.get("version", "0")
            exe_url = data.get("url", "")

            if not exe_url or ver_tuple(remote_version) <= ver_tuple(AGENT_VERSION):
                log.debug(f"Auto-update: up to date (local={AGENT_VERSION} remote={remote_version})")
                continue

            log.info(f"Auto-update: new version {remote_version} available, downloading...")

            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".exe", dir=tempfile.gettempdir())
            dl_req = urllib.request.Request(exe_url, headers={"User-Agent": "netmon-agent/1.0"})
            with urllib.request.urlopen(dl_req, timeout=120) as resp:
                tmp.write(resp.read())
            tmp.close()

            current_exe = sys.executable
            bat = tempfile.NamedTemporaryFile(
                delete=False, suffix=".bat", dir=tempfile.gettempdir(), mode="w"
            )
            bat.write(
                f"@echo off\r\n"
                f"timeout /t 3 /nobreak >nul\r\n"
                f"copy /Y \"{tmp.name}\" \"{current_exe}\"\r\n"
                f"del \"{tmp.name}\"\r\n"
                f"start \"\" \"{current_exe}\"\r\n"
                f"del \"%~f0\"\r\n"
            )
            bat.close()

            subprocess.Popen(
                ["cmd", "/c", bat.name],
                creationflags=subprocess.CREATE_NO_WINDOW | subprocess.DETACHED_PROCESS,
                close_fds=True,
            )
            log.info(f"Auto-update: v{remote_version} launcher started, exiting")
            os._exit(0)

        except Exception as e:
            log.debug(f"Auto-update check failed: {e}")


# ── Remote diagnostics server ─────────────────────────────────────────────
# Listens on port 9877 — Mac can query for on-demand network diagnostics

import http.server as _http_server
import socketserver as _socketserver

DIAG_PORT = 9877

class DiagHandler(_http_server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        log.debug(f"DiagServer: {fmt % args}")

    def do_GET(self):
        import subprocess, socket, time, json as _json, urllib.parse

        path = self.path.split("?")[0]
        params = dict(urllib.parse.parse_qsl(urllib.parse.urlparse(self.path).query))

        result = {}
        try:
            if path == "/diag/dns":
                host = params.get("host", "www.google.com")
                t0 = time.monotonic()
                ipv4 = socket.getaddrinfo(host, 80, socket.AF_INET)[0][4][0]
                ipv6 = None
                try:
                    ipv6 = socket.getaddrinfo(host, 80, socket.AF_INET6)[0][4][0]
                except Exception:
                    pass
                result = {"host": host, "ipv4": ipv4, "ipv6": ipv6, "dns_ms": round((time.monotonic()-t0)*1000,1)}

            elif path == "/diag/tcp":
                host = params.get("host", "www.google.com")
                port = int(params.get("port", 443))
                af = socket.AF_INET
                addrs = socket.getaddrinfo(host, port, af)
                ip = addrs[0][4][0]
                t0 = time.monotonic()
                s = socket.socket(af, socket.SOCK_STREAM)
                s.settimeout(10)
                s.connect((ip, port))
                s.close()
                result = {"host": host, "ip": ip, "port": port, "connect_ms": round((time.monotonic()-t0)*1000,1)}

            elif path == "/diag/http":
                import ssl, http.client
                from urllib.parse import urlparse as _up
                url = params.get("url", "https://www.google.com")
                parsed = _up(url)
                hostname = parsed.hostname
                port = parsed.port or (443 if parsed.scheme == "https" else 80)
                # DNS
                t_dns = time.monotonic()
                ipv4 = socket.getaddrinfo(hostname, port, socket.AF_INET)[0][4][0]
                dns_ms = round((time.monotonic()-t_dns)*1000, 1)
                # Connect + request
                t0 = time.monotonic()
                if parsed.scheme == "https":
                    ctx = ssl.create_default_context()
                    conn = http.client.HTTPSConnection(ipv4, port, timeout=15, context=ctx)
                    conn.set_tunnel(hostname)
                else:
                    conn = http.client.HTTPConnection(ipv4, port, timeout=15)
                conn.request("GET", parsed.path or "/", headers={"Host": hostname, "User-Agent": "netmon-diag/1.0"})
                resp = conn.getresponse()
                total_ms = round((time.monotonic()-t0)*1000, 1)
                resp.read(); conn.close()
                result = {"url": url, "ip": ipv4, "dns_ms": dns_ms, "total_ms": total_ms, "status": resp.status}

            elif path == "/diag/tracert":
                host = params.get("host", "8.8.8.8")
                proc = subprocess.run(
                    ["tracert", "-d", "-w", "1000", "-h", "15", host],
                    capture_output=True, text=True, timeout=30,
                    creationflags=subprocess.CREATE_NO_WINDOW
                )
                result = {"host": host, "output": proc.stdout}

            elif path == "/diag/ping":
                host = params.get("host", "8.8.8.8")
                count = min(int(params.get("count", "4")), 10)
                proc = subprocess.run(
                    ["ping", "-n", str(count), host],
                    capture_output=True, text=True, timeout=30,
                    creationflags=subprocess.CREATE_NO_WINDOW
                )
                result = {"host": host, "output": proc.stdout}

            elif path == "/diag/ipconfig":
                proc = subprocess.run(
                    ["ipconfig", "/all"],
                    capture_output=True, text=True, timeout=10,
                    creationflags=subprocess.CREATE_NO_WINDOW
                )
                result = {"output": proc.stdout}

            elif path == "/diag/state":
                with state.lock:
                    result = {
                        "version": AGENT_VERSION,
                        "ping_ms": state.ping_latency_ms,
                        "gw_ping_ms": state.gw_ping_latency_ms,
                        "http_latency_ms": state.http_latency_ms,
                        "http_success": state.http_success,
                        "ping_ok": state.ping_success,
                        "gw_ok": state.gw_ping_success,
                    }
            else:
                result = {"error": f"unknown path {path}", "available": [
                    "/diag/state", "/diag/dns?host=", "/diag/tcp?host=&port=",
                    "/diag/http?url=", "/diag/tracert?host=", "/diag/ping?host=&count=",
                    "/diag/ipconfig"
                ]}

        except Exception as e:
            result = {"error": str(e)}

        body = _json.dumps(result, indent=2).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def diag_server_loop() -> None:
    try:
        server = _socketserver.TCPServer(("0.0.0.0", DIAG_PORT), DiagHandler)
        server.allow_reuse_address = True
        log.info(f"Diag server listening on port {DIAG_PORT}")
        server.serve_forever()
    except Exception as e:
        log.error(f"Diag server failed: {e}")


# ── Entry point ───────────────────────────────────────────────────────────

def main() -> None:
    log.info(f"NetMon Windows Agent v{AGENT_VERSION} starting")
    cfg = load_config()
    log.info(f"Mac target: {cfg['mac_ip']}:{cfg['mac_port']}")

    for target, args, name in [
        (ping_loop,         (),        "ping"),
        (gateway_ping_loop, (),        "gw-ping"),
        (http_loop,         (),        "http"),
        (report_loop,       (cfg,),    "report"),
        (_check_for_update, (cfg,),    "autoupdate"),
        (diag_server_loop,  (),        "diag-server"),
    ]:
        t = threading.Thread(target=target, args=args, name=name, daemon=True)
        t.start()

    root = tk.Tk()
    _app = NetMonWindow(root)

    try:
        root.mainloop()
    except KeyboardInterrupt:
        pass
    log.info("NetMon Windows Agent stopped")


if __name__ == "__main__":
    main()
