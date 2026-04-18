from __future__ import annotations

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


def get_recent_logs(limit: int = 300) -> list[dict[str, Any]]:
    bounded = max(1, min(limit, _BUFFER_LIMIT))
    with _BUFFER_LOCK:
        return list(_BUFFER)[-bounded:]


_HTML = """<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <title>rd-cache-gateway live log</title>
  <style>
    body { background: #0b1020; color: #e5e7eb; font-family: monospace; margin: 0; }
    header { padding: 12px 16px; background: #111827; position: sticky; top: 0; }
    h1 { font-size: 16px; margin: 0 0 4px; }
    #status { color: #93c5fd; }
    pre { white-space: pre-wrap; word-break: break-word; margin: 0; padding: 16px; }
  </style>
</head>
<body>
  <header>
    <h1>rd-cache-gateway live log</h1>
    <div id=\"status\">connecting…</div>
  </header>
  <pre id=\"log\"></pre>
  <script>
    const logEl = document.getElementById('log');
    const statusEl = document.getElementById('status');
    async function refresh() {
      try {
        const response = await fetch('/logs?limit=500', { cache: 'no-store' });
        const data = await response.json();
        logEl.textContent = data.entries.map(x => x.formatted).join('\n');
        statusEl.textContent = 'live';
        window.scrollTo(0, document.body.scrollHeight);
      } catch (err) {
        statusEl.textContent = 'disconnected';
      }
    }
    refresh();
    setInterval(refresh, 1500);
  </script>
</body>
</html>
"""


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
        if parsed.path in {"/", "/index.html"}:
            self._send_html(_HTML)
            return

        if parsed.path == "/logs":
            params = parse_qs(parsed.query)
            try:
                limit = int((params.get("limit") or ["300"])[0])
            except ValueError:
                limit = 300
            self._send_json({"entries": get_recent_logs(limit)})
            return

        if parsed.path == "/healthz":
            self._send_json({"status": "ok"})
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
