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
        self.mac_fail_since: Optional[datetime] = None  # when consecutive failures started
        self.last_update: datetime = datetime.now()
        self.last_command_poll: datetime = datetime.now()  # tracks command poll loop timing

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
                if resp.status == 200:
                    with state.lock:
                        state.mac_reachable = True
                        state.mac_fail_since = None  # reset on success
                    log.debug(f"Report sent to Mac (HTTP {resp.status})")
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

_BG = "#1e1e1e"
_FG_DIM = "#888888"
_FG_LABEL = "#aaaaaa"
_INDICATOR_PX = 20
_FONT_SMALL = ("Segoe UI", 9)
_FONT_LABEL = ("Segoe UI", 9, "bold")

class NetMonWindow:
    def __init__(self, root: tk.Tk):
        self.root = root
        import socket as _s
        _hostname = _s.gethostname()
        self.root.title(f"LangNetmon v{AGENT_VERSION} — {_hostname}")
        self.root.geometry("300x115")
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

                # Fire a toast notification when status goes red and window is hidden
                window_hidden = not self.root.winfo_viewable()
                went_red = (tray_color == C_RED and self._prev_tray_color != C_RED)
                if went_red and window_hidden and self._tray_icon:
                    # Build a human-friendly message describing what went red
                    problems = []
                    if pc == C_RED:  problems.append("Internet ping failing")
                    if gwc == C_RED: problems.append("Router unreachable")
                    if hc == C_RED:  problems.append("HTTP checks failing")
                    if mc == C_RED:  problems.append("Mac Studio unreachable")
                    msg = " | ".join(problems) if problems else "Network issue detected"
                    try:
                        self._tray_icon.notify(msg, title="LangNetmon Alert")
                    except Exception:
                        pass
                self._prev_tray_color = tray_color
        except Exception as e:
            log.error(f"GUI update error: {e}")


# ── Version & auto-update ──────────────────────────────────────────────────

AGENT_VERSION = "1.8.0"


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


def _do_update_check(cfg: dict) -> bool:
    """
    Check the Mac daemon for a newer agent version and self-update if found.
    Returns True if an update was triggered (process will restart).
    Only runs when frozen as a PyInstaller exe.
    """
    if not getattr(sys, "frozen", False):
        log.debug("Auto-update: skipped (not frozen exe)")
        return False

    import tempfile
    mac_base = f"http://{cfg['mac_ip']}:{cfg['mac_port']}"
    version_url = f"{mac_base}/version"

    try:
        req = urllib.request.Request(version_url, headers={"User-Agent": "netmon-agent/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        remote_version = data.get("version", "0")
        exe_url = data.get("url", "")

        if not exe_url or _ver_tuple(remote_version) <= _ver_tuple(AGENT_VERSION):
            log.debug(f"Auto-update: up to date (local={AGENT_VERSION} remote={remote_version})")
            return False

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
    return False


# ── Command poll (outbound-only diagnostics) ──────────────────────────────
# Agent polls Mac for pending commands every 30s. No inbound ports opened.
# Supported commands: dns, tcp, http, ping, tracert, ipconfig, state

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
        return result.stdout, result.stderr, result.returncode
    except subprocess.TimeoutExpired:
        return "", "PowerShell execution timed out (60s)", 1
    except Exception as e:
        return "", f"PS exec error: {e}", 1


def ps_session_loop(cfg: dict) -> None:
    """
    Persistent long-poll loop for remote PowerShell execution.
    Opens GET /ps_session?hostname=X — Mac holds it open until a script
    is queued. Agent receives script, runs it, POSTs result to /ps_result,
    then immediately reconnects. Falls back to 5s retry on errors.
    """
    import socket as _sock
    hostname = _sock.gethostname()
    mac_base = f"http://{cfg['mac_ip']}:{cfg['mac_port']}"
    session_url = f"{mac_base}/ps_session?hostname={hostname}"
    result_url  = f"{mac_base}/ps_result"

    log.info(f"PS session loop started → {mac_base}")

    while True:
        try:
            # Long-poll: timeout slightly longer than server hold (55s)
            req = urllib.request.Request(
                session_url,
                headers={"User-Agent": "netmon-agent/1.0"},
            )
            with urllib.request.urlopen(req, timeout=58) as resp:
                data = json.loads(resp.read())

            cmd = data.get("cmd")

            if cmd == "noop":
                # Server timeout, reconnect immediately
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

                post_req = urllib.request.Request(
                    result_url,
                    data=payload,
                    headers={"Content-Type": "application/json", "User-Agent": "netmon-agent/1.0"},
                    method="POST",
                )
                urllib.request.urlopen(post_req, timeout=10)
                log.info(f"PS result [{cid}] posted (exit={exit_code})")
                # Reconnect immediately for next command
                continue

        except Exception as e:
            log.debug(f"PS session error: {e} — reconnecting in 5s")
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
    hostname = _sock.gethostname()
    mac_base = f"http://{cfg['mac_ip']}:{cfg['mac_port']}"
    poll_url = f"{mac_base}/commands?hostname={hostname}"
    result_url = f"{mac_base}/command_result"

    def post_result(cmd_id, cmd, result):
        payload = json.dumps({
            "id": cmd_id,
            "hostname": hostname,
            "cmd": cmd,
            "result": result,
        }).encode()
        req = urllib.request.Request(
            result_url,
            data=payload,
            headers={"Content-Type": "application/json", "User-Agent": "netmon-agent/1.0"},
            method="POST"
        )
        urllib.request.urlopen(req, timeout=10)

    # Startup update check — runs immediately before first poll sleep
    log.info("Auto-update: checking on startup...")
    _do_update_check(cfg)  # exits process if update found

    while True:
        interval = _POLL_DIAG if _diag_mode else _POLL_NORMAL
        with state.lock:
            state.last_command_poll = datetime.now()
        time.sleep(interval)

        # Check for updates on every normal-mode poll (every 30s)
        if not _diag_mode:
            _do_update_check(cfg)  # exits process if update found
        try:
            req = urllib.request.Request(
                poll_url + f"&diag={'1' if _diag_mode else '0'}",
                headers={"User-Agent": "netmon-agent/1.0"}
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                commands = json.loads(resp.read())

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


# ── Entry point ───────────────────────────────────────────────────────────

def main() -> None:
    _enforce_single_instance()  # exits immediately if another instance is running
    log.info(f"NetMon Windows Agent v{AGENT_VERSION} starting")
    cfg = load_config()
    log.info(f"Mac target: {cfg['mac_ip']}:{cfg['mac_port']}")

    for target, args, name in [
        (ping_loop,         (),        "ping"),
        (gateway_ping_loop, (),        "gw-ping"),
        (http_loop,         (),        "http"),
        (report_loop,       (cfg,),    "report"),
        (command_poll_loop, (cfg,),    "cmd-poll"),   # includes startup + 30s update checks
        (ps_session_loop,   (cfg,),    "ps-session"),
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
