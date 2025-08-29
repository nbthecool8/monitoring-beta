import os
import json
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
from prometheus_client import start_http_server, Counter

# Logging setup
logging.basicConfig(level=logging.INFO)

# Prometheus counter
REQUEST_COUNT = Counter("demo_hits_total", "Total requests", ["path"])


class MyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            msg = f"hello from CI/CD v5"
            self._send_response(200, msg, "text/plain")
        elif self.path == "/version":
            data = {"app": os.getenv("APP", "demo"), "ver": os.getenv("VER", "v1")}
            self._send_response(200, json.dumps(data), "application/json")
        elif self.path == "/healthz":
            self._send_response(200, "ok", "text/plain")
        else:
            self._send_response(404, "not found", "text/plain")

    def _send_response(self, code, msg, content_type="text/plain"):
        self.send_response(code)
        self.send_header("Content-type", content_type)
        self.end_headers()
        self.wfile.write(msg.encode())
        REQUEST_COUNT.labels(path=self.path).inc()
        logging.info("Served %s with %d", self.path, code)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    metrics_port = int(os.getenv("METRICS_PORT", "9000"))

    # Start Prometheus metrics server
    start_http_server(metrics_port)

    # Start HTTP app server
    srv = HTTPServer(("", port), MyHandler)
    logging.info("server running on %d", port)
    srv.serve_forever()
