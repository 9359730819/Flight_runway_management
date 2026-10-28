"""Local web server for the airport runway simulation. Run with: python dashboard_server.py"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import threading
import webbrowser

from airport_runway import PRESETS, RunwaySimulation, validate_config

ROOT = Path(__file__).resolve().parent
PORT = 8000
current: RunwaySimulation | None = None
guard = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def _send(self, status: int, payload: bytes, content_type: str):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        if self.path == "/api/state":
            with guard:
                sim = current
            data = sim.snapshot() if sim else {"running": False, "presets": PRESETS}
            self._send(200, json.dumps(data).encode(), "application/json; charset=utf-8")
        elif self.path == "/":
            self._send(200, (ROOT / "dashboard.html").read_bytes(), "text/html; charset=utf-8")
        else:
            self._send(404, b"Not found", "text/plain; charset=utf-8")

    def do_POST(self):
        global current
        if self.path in ("/api/pause", "/api/resume", "/api/reset"):
            with guard:
                sim = current
            if not sim:
                return self._send(409, b'{"error":"There is no simulation to control."}', "application/json")
            if self.path == "/api/pause":
                sim.pause()
            elif self.path == "/api/resume":
                sim.resume()
            else:
                sim.stop()
                with guard:
                    if current is sim:
                        current = None
            return self._send(200, b'{"ok":true}', "application/json")
        if self.path != "/api/start":
            return self._send(404, b'{"error":"Not found"}', "application/json")
        try:
            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length) or b"{}")
            if body.get("preset"):
                name = body["preset"]
                if name not in PRESETS: raise ValueError("Unknown preset configuration.")
                config = validate_config(PRESETS[name])
            else:
                config = validate_config(body)
            speed = float(body.get("speed", 1.0))
            if speed not in (0.6, 1.0, 1.8):
                raise ValueError("Animation speed must be Slow, Normal, or Fast.")
            with guard:
                if current and not current.done:
                    return self._send(409, b'{"error":"A simulation is already running."}', "application/json")
                current = RunwaySimulation(config, speed)
                sim = current
            sim.start()
            self._send(202, json.dumps({"ok": True}).encode(), "application/json; charset=utf-8")
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            self._send(400, json.dumps({"error": str(exc)}).encode(), "application/json; charset=utf-8")


if __name__ == "__main__":
    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    print(f"Airport Runway Control is ready at http://127.0.0.1:{PORT}")
    threading.Timer(0.8, lambda: webbrowser.open(f"http://127.0.0.1:{PORT}")).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")
        server.server_close()
