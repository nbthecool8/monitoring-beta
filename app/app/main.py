from http.server import BaseHTTPRequestHandler, HTTPServer
class H(BaseHTTPRequestHandler):
    def do_GET(self):
        msg = b"hello from CI/CD\n"
        self.send_response(200); self.end_headers(); self.wfile.write(msg)
HTTPServer(('', 8080), H).serve_forever()
