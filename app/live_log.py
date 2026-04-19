from __future__ import annotations

import html
import json
import logging
import os
import threading
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

_BUFFER_LIMIT = max(200, int(os.getenv("LIVE_LOG_BUFFER_SIZE", "2000")))
_BUFFER: deque[dict[str, Any]] = deque(maxlen=_BUFFER_LIMIT)
_BUFFER_LOCK = threading.Lock()
_HANDLER_INSTALLED = False
_JOBS_PROVIDER: Any = None


class LiveLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            formatted = self.format(record)
        except Exception:
            formatted = record.getMessage()
        entry = {
            "time": getattr(record, "asctime", None),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "formatted": formatted,
        }
        with _BUFFER_LOCK:
            _BUFFER.append(entry)


def install_live_log_handler() -> None:
    global _HANDLER_INSTALLED
    if _HANDLER_INSTALLED:
        return

    handler = LiveLogHandler(level=logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    logging.getLogger().addHandler(handler)
    _HANDLER_INSTALLED = True


def set_jobs_provider(provider: Any) -> None:
    global _JOBS_PROVIDER
    _JOBS_PROVIDER = provider


def get_jobs_snapshot() -> list[dict[str, Any]]:
    if _JOBS_PROVIDER is None:
        return []
    try:
        jobs = _JOBS_PROVIDER() or {}
    except Exception:
        return []

    items: list[dict[str, Any]] = []
    for job_id, job in jobs.items():
        if not isinstance(job, dict) or job.get("deleted_by_client"):
            continue
        raw = job.get("raw") or {}
        progress_raw = raw.get("progress")
        try:
            progress = float(progress_raw)
        except Exception:
            progress = 0.0
        if progress > 1:
            progress = progress / 100.0
        if job.get("status") in {"ready_for_arr", "scan_pending", "imported"}:
            progress = 1.0
        elif job.get("status") == "staged":
            progress = max(progress, 0.95)

        seeds = raw.get("seeders") or raw.get("num_seeds") or 0
        peers = raw.get("peers") or raw.get("num_leechs") or raw.get("leechers") or 0
        speed = raw.get("speed") or raw.get("downloadSpeed") or 0
        items.append(
            {
                "job_id": str(job_id),
                "client_hash": str(job.get("client_hash") or ""),
                "name": str(job.get("filename") or job_id),
                "status": str(job.get("status") or "queued"),
                "rd_status": str(job.get("rd_status") or ""),
                "progress": max(0.0, min(progress, 1.0)),
                "seeds": int(seeds or 0),
                "peers": int(peers or 0),
                "speed": int(speed or 0),
                "arr_ready_reason": str(job.get("arr_ready_reason") or ""),
                "last_error": str(job.get("last_error") or ""),
            }
        )

    items.sort(key=lambda item: (item["status"] == "imported", item["name"].lower()))
    return items


def get_recent_logs(limit: int = 300) -> list[dict[str, Any]]:
    bounded = max(1, min(limit, _BUFFER_LIMIT))
    with _BUFFER_LOCK:
        return list(_BUFFER)[-bounded:]


def _format_speed(value: int) -> str:
    numeric = float(value or 0)
    units = ["B/s", "KB/s", "MB/s", "GB/s"]
    for unit in units:
        if numeric < 1024 or unit == units[-1]:
            return f"{numeric:.1f} {unit}"
        numeric /= 1024
    return "0.0 B/s"


def get_log_view_html(limit: int = 500, refresh_seconds: int = 2) -> str:
    entries = get_recent_logs(limit)
    lines = "\n".join(item.get("formatted") or item.get("message") or "" for item in entries)
    if not lines:
        lines = "Waiting for log events..."

    jobs = get_jobs_snapshot()
    if jobs:
        job_items = []
        for job in jobs:
            pct = int(round(job["progress"] * 100))
            color = "#22c55e" if pct >= 100 else ("#3b82f6" if pct >= 1 else "#f59e0b")
            error_html = f"<div class='error'>Error: {html.escape(job['last_error'])}</div>" if job.get("last_error") else ""
            reason_html = f"<div class='reason'>Reason: {html.escape(job['arr_ready_reason'])}</div>" if job.get("arr_ready_reason") else ""
            job_items.append(
                f"""
                <li class='job'>
                  <div class='job-head'>
                    <strong>{html.escape(job['name'])}</strong>
                    <span class='badge'>{html.escape(job['status'])}</span>
                  </div>
                  <div class='meta'>RD: {html.escape(job['rd_status']) or '-'} • Seeds: {job['seeds']} • Peers: {job['peers']} • Speed: {_format_speed(job['speed'])}</div>
                  <div class='meta'>Job: {html.escape(job['job_id'])}</div>
                  <div class='bar'><div class='fill' style='width:{pct}%; background:{color};'></div></div>
                  <div class='meta'>Progress: {pct}%</div>
                  {reason_html}
                  {error_html}
                </li>
                """
            )
        jobs_html = "<ul class='jobs'>" + "".join(job_items) + "</ul>"
    else:
        jobs_html = "<p class='hint'>No active jobs yet.</p>"

    return f"""<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <meta http-equiv=\"refresh\" content=\"{max(1, refresh_seconds)}\" />
  <title>rd-cache-gateway live log</title>
  <style>
    body {{ background: #0b1020; color: #e5e7eb; font-family: Arial, sans-serif; margin: 0; }}
    header {{ padding: 12px 16px; background: #111827; position: sticky; top: 0; }}
    h1 {{ font-size: 18px; margin: 0 0 4px; }}
    h2 {{ font-size: 16px; margin: 0 0 12px; }}
    .status {{ color: #86efac; font-family: monospace; }}
    .hint {{ color: #93c5fd; font-size: 12px; }}
    .section {{ padding: 16px; border-top: 1px solid #1f2937; }}
    .jobs {{ list-style: none; margin: 0; padding: 0; display: grid; gap: 12px; }}
    .job {{ background: #111827; border: 1px solid #1f2937; border-radius: 8px; padding: 12px; }}
    .job-head {{ display: flex; justify-content: space-between; gap: 12px; margin-bottom: 8px; }}
    .badge {{ background: #1f2937; border-radius: 999px; padding: 2px 8px; font-size: 12px; font-family: monospace; }}
    .meta {{ color: #cbd5e1; font-size: 13px; margin-top: 4px; font-family: monospace; }}
    .reason {{ color: #93c5fd; font-size: 12px; margin-top: 6px; font-family: monospace; }}
    .error {{ color: #fca5a5; font-size: 12px; margin-top: 6px; font-family: monospace; }}
    .bar {{ margin-top: 8px; width: 100%; height: 12px; background: #0f172a; border-radius: 999px; overflow: hidden; border: 1px solid #334155; }}
    .fill {{ height: 100%; }}
    pre {{ white-space: pre-wrap; word-break: break-word; margin: 0; padding: 16px; font-family: monospace; }}
  </style>
</head>
<body>
  <header>
    <h1>rd-cache-gateway dashboard</h1>
    <div class=\"status\">live</div>
    <div class=\"hint\">Auto-refreshes every {max(1, refresh_seconds)}s</div>
  </header>
  <div class='section'>
    <h2>Active jobs</h2>
    {jobs_html}
  </div>
  <div class='section'>
    <h2>Live API log</h2>
  </div>
  <pre>{html.escape(lines)}</pre>
</body>
</html>"""


class _LogRequestHandler(BaseHTTPRequestHandler):
    server_version = "rd-cache-gateway-log/1.0"

    def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
        content = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(content)

    def _send_html(self, content: str, status: int = 200) -> None:
        data = content.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path in {"/", "/index.html", "/debug/live"}:
            self._send_html(get_log_view_html())
            return

        if parsed.path in {"/logs", "/debug/logs"}:
            params = parse_qs(parsed.query)
            try:
                limit = int((params.get("limit") or ["300"])[0])
            except ValueError:
                limit = 300
            self._send_json({"entries": get_recent_logs(limit)})
            return

        if parsed.path == "/debug/logs.txt":
            data = "\n".join(item.get("formatted") or item.get("message") or "" for item in get_recent_logs())
            payload = data.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path in {"/jobs", "/debug/jobs"}:
            self._send_json({"jobs": get_jobs_snapshot()})
            return

        if parsed.path in {"/healthz", "/debug/status"}:
            self._send_json({"status": "ok", "entries": len(get_recent_logs()), "jobs": len(get_jobs_snapshot())})
            return

        self._send_json({"error": "not_found"}, status=404)

    def log_message(self, format: str, *args: Any) -> None:
        return


class LiveLogServer:
    def __init__(self, host: str = "0.0.0.0", port: int = 8888):
        self.host = host
        self.port = port
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._server is not None:
            return
        try:
            self._server = ThreadingHTTPServer((self.host, self.port), _LogRequestHandler)
        except OSError as exc:
            logging.getLogger(__name__).warning("LIVELOG failed to bind %s:%s: %s", self.host, self.port, exc)
            self._server = None
            return
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True, name="live-log-server")
        self._thread.start()
        logging.getLogger(__name__).info("LIVELOG started on %s:%s", self.host, self.port)

    def stop(self) -> None:
        if self._server is None:
            return
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._server = None
        self._thread = None
