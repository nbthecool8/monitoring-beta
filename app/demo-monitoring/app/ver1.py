import os, time, json
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from prometheus_client import start_http_server, Counter

app = os.getenv("APP", "demo")
ver = os.getenv("VER", "v1")
port = int(os.getenv("PORT", "8080"))
mport = int(os.getenv("MPORT", "9000"))

hits = Counter("demo_hits_total", "Total hits")

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        p = urlparse(self.path).path
        q = parse_qs(urlparse(self.path).query)

        if p == "/":
            d = int(q.get("sleep", [0])[0])
            if d > 0: time.sleep(d/1000)
            msg = f"hello from {app} {ver}\n".encode()
            self.send_response(200); self.end_headers(); self.wfile.write(msg)
            hits.inc()
        elif p == "/version":
            data = {"app": app, "ver": ver}
            self.send_response(200); self.end_headers()
            self.wfile.write(json.dumps(data).encode())
        elif p in ("/healthz", "/ready"):
            self.send_response(200); self.end_headers(); self.wfile.write(b"ok\n")
        else:
            self.send_response(404); self.end_headers(); self.wfile.write(b"nope\n")

if __name__ == "__main__":
    start_http_server(mport)
    print(f"metrics on {mport}")
    s = HTTPServer(("", port), H)
    print(f"server on {port}")
    s.serve_forever()
