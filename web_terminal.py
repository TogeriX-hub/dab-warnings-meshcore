"""
web_terminal.py – shell on the WarnBridge machine, in the dashboard (Shift+T).

Meant for maintenance and debugging. Off by default; switched on only in
config.yaml (terminal.enabled: true), never from the dashboard itself.

The dashboard has no login, so the websocket is guarded in two ways:
  - Origin check: only the dashboard page itself may open it. Stops a random
    web page in your own browser from talking to the Pi.
  - Host check: the address in the browser must be an IP, localhost, a .local
    or Tailscale (.ts.net) name, or listed in terminal.allowed_hosts.
    Stops DNS rebinding, where a foreign domain is pointed at the Pi's IP.

Sessions:
  - With tmux installed, the shell runs inside a tmux session "warnbridge" on
    its own tmux server (tmux -L warnbridge). Closing the tab or reloading
    keeps it running; reopening reattaches.
  - On Linux with systemd and passwordless sudo, the session is started in its
    own systemd scope, outside warnbridge.service. Then it also survives a
    restart of WarnBridge, so ./update.sh can be run from here.
  - Without tmux it is a plain shell that ends with the tab.

Protocol on /ws/terminal:
  server -> client  binary: terminal output
                    text:   JSON {"type": "info"|"error"|"exit", ...}
  client -> server  text:   JSON {"t": "i", "d": "<input>"} or
                              {"t": "r", "c": <cols>, "r": <rows>}
"""

import asyncio
import fcntl
import ipaddress
import json
import logging
import os
import pwd
import shutil
import signal
import struct
import subprocess
import sys
import termios
from pathlib import Path
from typing import Optional
from urllib.parse import urlsplit

from aiohttp import web, WSMsgType

logger = logging.getLogger(__name__)

REPO_DIR = Path(__file__).resolve().parent
TMUX_SESSION = "warnbridge"
MAX_SESSIONS = 3
READ_CHUNK = 65536

_active_sessions = 0
_scope_ok: Optional[bool] = None  # probed once, see _scope_available()


# ----------------------------------------------------------------------
# Access checks
# ----------------------------------------------------------------------

def _host_name(hostport: str) -> str:
    """'warnbridge.local:8080' -> 'warnbridge.local', '[::1]:8080' -> '::1'"""
    hostport = hostport.strip().lower()
    if hostport.startswith("["):
        return hostport[1:hostport.find("]")]
    if hostport.count(":") == 1:
        return hostport.rsplit(":", 1)[0]
    return hostport


def _host_allowed(host: str, extra: list) -> bool:
    if not host:
        return False
    if host in ("localhost",) or host in [h.lower() for h in extra]:
        return True
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        pass
    if host.endswith(".local") or host.endswith(".ts.net"):
        return True
    # plain machine name without a domain, e.g. "warnbridge"
    return "." not in host


def check_request(request: web.Request, cfg: dict) -> Optional[str]:
    """Returns None when the request may open a terminal, else the reason."""
    origin = request.headers.get("Origin", "")
    if not origin:
        return "Origin fehlt"
    o = urlsplit(origin)
    if o.scheme not in ("http", "https"):
        return f"Origin nicht erlaubt: {origin}"
    if o.netloc.lower() != request.host.lower():
        return f"Origin {origin} passt nicht zu {request.host}"
    host = _host_name(request.host)
    if not _host_allowed(host, cfg.get("allowed_hosts", []) or []):
        return f"Hostname {host} nicht erlaubt (terminal.allowed_hosts)"
    return None


# ----------------------------------------------------------------------
# Command to start
# ----------------------------------------------------------------------

def _scope_available() -> bool:
    """True if the shell can be started in its own systemd scope via sudo."""
    global _scope_ok
    if _scope_ok is not None:
        return _scope_ok
    _scope_ok = False
    if not sys.platform.startswith("linux") or os.getuid() == 0:
        return False
    if not (shutil.which("systemd-run") and shutil.which("sudo") and shutil.which("runuser")):
        return False
    user = pwd.getpwuid(os.getuid()).pw_name
    try:
        r = subprocess.run(
            ["sudo", "-n", "systemd-run", "--scope", "--quiet", "--collect",
             "--", "runuser", "-u", user, "--", "true"],
            capture_output=True, timeout=10,
        )
        _scope_ok = r.returncode == 0
        if not _scope_ok:
            logger.info("Terminal: kein eigener systemd-Scope möglich (%s)",
                        r.stderr.decode(errors="replace").strip()[:200])
    except Exception as e:
        logger.info("Terminal: Scope-Test fehlgeschlagen: %s", e)
    return _scope_ok


def _build_command() -> tuple[list[str], dict, str]:
    """argv, environment and mode ('persistent', 'tmux' or 'simple')."""
    pw = pwd.getpwuid(os.getuid())
    shell = pw.pw_shell or "/bin/bash"
    if not os.path.exists(shell):
        shell = "/bin/sh"

    env = dict(os.environ)
    env.update({
        "TERM": "xterm-256color",
        "COLORTERM": "truecolor",
        "HOME": pw.pw_dir,
        "USER": pw.pw_name,
        "LOGNAME": pw.pw_name,
        "SHELL": shell,
    })
    # systemd services usually run without a locale – umlauts would break
    if "UTF-8" not in env.get("LANG", "").upper().replace("UTF8", "UTF-8"):
        env["LANG"] = "C.UTF-8"
    env.pop("NOTIFY_SOCKET", None)    # never let the shell talk to our watchdog
    env.pop("WATCHDOG_PID", None)
    env.pop("WATCHDOG_USEC", None)
    env.pop("INVOCATION_ID", None)

    tmux = shutil.which("tmux")
    if not tmux:
        return [shell, "-l"], env, "simple"

    # own tmux server (-L) so these settings never touch tmux sessions you
    # start yourself over ssh. Attach from ssh with: tmux -L warnbridge attach
    inner = [tmux, "-L", TMUX_SESSION, "new-session", "-A", "-s", TMUX_SESSION,
             "-c", str(REPO_DIR),
             ";", "set-option", "-g", "mouse", "on",
             ";", "set-option", "-g", "status-style", "bg=colour235,fg=colour245",
             ";", "set-option", "-g", "status-right", "#H  %H:%M",
             ";", "set-option", "-g", "history-limit", "10000"]

    if _scope_available():
        # sudo drops most of the environment, so pass it on explicitly
        keep = ["TERM", "COLORTERM", "LANG", "PATH"]
        env_args = [f"{k}={env[k]}" for k in keep if k in env]
        argv = (["sudo", "-n", "systemd-run", "--scope", "--quiet", "--collect",
                 "--", "runuser", "-u", pw.pw_name, "--", "/usr/bin/env"]
                + env_args + inner)
        return argv, env, "persistent"

    return inner, env, "tmux"


# ----------------------------------------------------------------------
# Session
# ----------------------------------------------------------------------

def _set_winsize(fd: int, cols: int, rows: int):
    cols = max(10, min(int(cols), 1000))
    rows = max(4, min(int(rows), 500))
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))


async def handle(request: web.Request, cfg: dict) -> web.StreamResponse:
    global _active_sessions

    reason = check_request(request, cfg)
    if reason:
        logger.warning("Terminal abgelehnt (%s): %s", request.remote, reason)
        return web.Response(status=403, text="Forbidden")

    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)

    async def send_json(obj: dict):
        if not ws.closed:
            await ws.send_str(json.dumps(obj))

    if not cfg.get("enabled", False):
        await send_json({"type": "error", "message":
                         "Terminal ist aus. Einschalten in config.yaml: "
                         "terminal: enabled: true – danach WarnBridge neu starten."})
        await ws.close()
        return ws

    if _active_sessions >= MAX_SESSIONS:
        await send_json({"type": "error",
                         "message": f"Schon {MAX_SESSIONS} Terminals offen."})
        await ws.close()
        return ws

    argv, env, mode = await asyncio.get_running_loop().run_in_executor(None, _build_command)

    import pty
    try:
        pid, fd = pty.fork()
    except OSError as e:
        await send_json({"type": "error", "message": f"Terminal konnte nicht starten: {e}"})
        await ws.close()
        return ws

    if pid == 0:  # child: only chdir + exec, nothing else after fork
        try:
            os.chdir(REPO_DIR)
            os.execvpe(argv[0], argv, env)
        finally:
            os._exit(127)

    _active_sessions += 1
    logger.info("Terminal geöffnet von %s (Modus: %s, pid %d)", request.remote, mode, pid)
    loop = asyncio.get_running_loop()
    os.set_blocking(fd, False)
    _set_winsize(fd, 80, 24)

    out_queue: asyncio.Queue = asyncio.Queue(maxsize=256)
    write_buf = bytearray()
    closed = asyncio.Event()

    def on_readable():
        try:
            data = os.read(fd, READ_CHUNK)
        except BlockingIOError:
            return
        except OSError:
            data = b""
        if not data:  # shell ended
            loop.remove_reader(fd)
            closed.set()
            return
        try:
            out_queue.put_nowait(data)
        except asyncio.QueueFull:
            # browser too slow – pause reading until the sender catches up
            loop.remove_reader(fd)
            asyncio.ensure_future(_requeue(data))

    async def _requeue(data: bytes):
        await out_queue.put(data)
        if not closed.is_set():
            loop.add_reader(fd, on_readable)

    def on_writable():
        nonlocal write_buf
        try:
            n = os.write(fd, write_buf)
            del write_buf[:n]
        except BlockingIOError:
            return
        except OSError:
            write_buf.clear()
        if not write_buf:
            loop.remove_writer(fd)

    def write_input(data: bytes):
        was_empty = not write_buf
        write_buf.extend(data)
        if was_empty:
            on_writable()
            if write_buf:
                loop.add_writer(fd, on_writable)

    async def sender():
        while True:
            get = asyncio.ensure_future(out_queue.get())
            stop = asyncio.ensure_future(closed.wait())
            done, _ = await asyncio.wait({get, stop}, return_when=asyncio.FIRST_COMPLETED)
            if get in done:
                stop.cancel()
                chunk = get.result()
                # coalesce whatever else is waiting into one frame
                while not out_queue.empty() and len(chunk) < 262144:
                    chunk += out_queue.get_nowait()
                if ws.closed:
                    return
                await ws.send_bytes(chunk)
            else:
                get.cancel()
                while not out_queue.empty():
                    if not ws.closed:
                        await ws.send_bytes(out_queue.get_nowait())
                return

    loop.add_reader(fd, on_readable)
    sender_task = asyncio.ensure_future(sender())
    await send_json({"type": "info", "mode": mode})

    try:
        recv_task = None
        while not closed.is_set():
            recv_task = asyncio.ensure_future(ws.receive())
            stop = asyncio.ensure_future(closed.wait())
            done, _ = await asyncio.wait({recv_task, stop}, return_when=asyncio.FIRST_COMPLETED)
            if recv_task not in done:
                recv_task.cancel()
                break
            stop.cancel()
            msg = recv_task.result()
            if msg.type == WSMsgType.TEXT:
                try:
                    m = json.loads(msg.data)
                except ValueError:
                    continue
                if m.get("t") == "i":
                    write_input(str(m.get("d", "")).encode("utf-8", errors="replace"))
                elif m.get("t") == "r":
                    try:
                        _set_winsize(fd, m.get("c", 80), m.get("r", 24))
                    except (OSError, ValueError, TypeError):
                        pass
            elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING,
                              WSMsgType.CLOSED, WSMsgType.ERROR):
                break
    finally:
        closed.set()
        try:
            await asyncio.wait_for(sender_task, timeout=2)
        except (asyncio.TimeoutError, Exception):
            sender_task.cancel()
        try:
            loop.remove_reader(fd)
            loop.remove_writer(fd)
        except Exception:
            pass
        try:
            os.close(fd)  # hangup: tmux detaches, a plain shell exits
        except OSError:
            pass
        await _reap(pid)
        _active_sessions -= 1
        if not ws.closed:
            await send_json({"type": "exit"})
            await ws.close()
        logger.info("Terminal geschlossen (pid %d)", pid)

    return ws


async def _reap(pid: int):
    """Wait for the child after hangup; escalate if it does not go away."""
    for sig, wait in ((None, 2.0), (signal.SIGHUP, 2.0), (signal.SIGKILL, 1.0)):
        if sig is not None:
            try:
                os.killpg(pid, sig)
            except (ProcessLookupError, PermissionError):
                pass
        steps = int(wait / 0.1)
        for _ in range(steps):
            try:
                done, _ = os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                return
            if done:
                return
            await asyncio.sleep(0.1)
