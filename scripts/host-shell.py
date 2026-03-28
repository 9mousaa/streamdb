#!/usr/bin/env python3
"""Minimal shell API server. Runs directly on host as systemd service.
Listens on 127.0.0.1:7777 — Traefik routes sh.plaio.cc to it.
"""
import subprocess, os, signal
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == '/':
            self._ok(b'Shell API: /run?cmd=whoami')
        elif parsed.path == '/run':
            cmd = parse_qs(parsed.query).get('cmd', [''])[0]
            if not cmd:
                self._ok(b'No command')
                return
            try:
                r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=120)
                self._ok((r.stdout + r.stderr).encode())
            except subprocess.TimeoutExpired:
                self._resp(504, b'Command timed out after 120s')
            except Exception as e:
                self._resp(500, str(e).encode())
        else:
            self._resp(404, b'Not found')

    def _ok(self, body):
        self._resp(200, body)

    def _resp(self, code, body):
        self.send_response(code)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass  # silent

if __name__ == '__main__':
    port = int(os.environ.get('SHELL_PORT', '7777'))
    server = HTTPServer(('0.0.0.0', port), Handler)
    signal.signal(signal.SIGTERM, lambda *a: (server.shutdown(), exit(0)))
    print(f'Host shell listening on :{port}')
    server.serve_forever()
