#!/usr/bin/env python3
"""webhook_sink.py — a fake webhook HTTP server for the cdrom e2e suite (F-18).

Usage: webhook_sink.py <port> <outfile> [status]

Records every request it receives (method, path, headers, and body) to
<outfile> (one "=== REQUEST ===" block per request) and responds with the
given status (default 200). The e2e test points a webhook step's url at it and
inspects the recorded request to verify the API performed the HTTP request with
the step's rendered headers/body. A second instance can return a non-2xx status
(e.g. 500) to exercise the failure path.

The response is well-formed (HTTP/1.1, an explicit Content-Length, and
Connection: close) so the API's webhook client — which has no global timeout,
so a per-request timeout is honored — reads the body to its declared length and
completes instead of waiting for the connection to close. A threaded server
handles the concurrent requests a fan-out job (one per worker) makes.
"""
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def main():
    if len(sys.argv) < 3:
        print("usage: webhook_sink.py <port> <outfile> [status]", file=sys.stderr)
        sys.exit(2)
    port = int(sys.argv[1])
    outfile = sys.argv[2]
    status = int(sys.argv[3]) if len(sys.argv) > 3 else 200

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):  # keep the sink quiet
            pass

        def _record(self):
            length = int(self.headers.get("Content-Length", 0) or 0)
            body = self.rfile.read(length) if length else b""
            with open(outfile, "ab") as f:
                f.write(b"=== REQUEST ===\n")
                f.write(f"METHOD: {self.command}\n".encode())
                f.write(f"PATH: {self.path}\n".encode())
                for k, v in self.headers.items():
                    f.write(f"{k}: {v}\n".encode())
                f.write(b"\n")
                f.write(body)
                f.write(b"\n")

        def _respond(self):
            body = b"ok"
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)
            self.close_connection = True

        def do_GET(self):
            self._record()
            self._respond()

        def do_POST(self):
            self._record()
            self._respond()

        def do_PUT(self):
            self._record()
            self._respond()

    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    main()
