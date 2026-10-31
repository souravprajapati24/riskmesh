#!/usr/bin/env python3
"""Local mock payment gateway for RiskMesh Phase 4 manual testing.

Endpoints
  POST /api/v1/holds       place a hold (behaviour depends on the current mode)
  POST /__mode/<mode>      switch mode at runtime: accept | reject | error

Modes
  accept  -> 200 {"holdReference": "hold_<transactionId>"}   (idempotent per transactionId)
  reject  -> 422 {"reason": "Insufficient funds (mock)"}
  error   -> 500 {"error": "mock gateway failure"}

Environment: GATEWAY_PORT (default 9099), GATEWAY_MODE (default accept)
"""
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("GATEWAY_PORT", "9099"))
STATE = {"mode": os.environ.get("GATEWAY_MODE", "accept")}
VALID_MODES = ("accept", "reject", "error")


class Handler(BaseHTTPRequestHandler):
    def _send(self, status, body):
        data = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b"{}"


        if self.path.startswith("/__mode/"):
            mode = self.path.rsplit("/", 1)[1]
            if mode not in VALID_MODES:
                self._send(400, {"error": "unknown mode", "valid": list(VALID_MODES)})
                return
            STATE["mode"] = mode
            self._send(200, {"mode": mode})
            return

        if self.path == "/api/v1/holds":
            payload = json.loads(raw or b"{}")
            txn = payload.get("transactionId", "unknown")
            mode = STATE["mode"]
            print(f"hold request transactionId={txn} amount={payload.get('amount')} mode={mode}", flush=True)
            if mode == "accept":
                self._send(200, {"holdReference": f"hold_{txn}"})
            elif mode == "reject":
                self._send(422, {"reason": "Insufficient funds (mock)"})
            else:
                self._send(500, {"error": "mock gateway failure"})
            return

        self._send(404, {"error": "not found"})

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    print(f"mock gateway listening on :{PORT} (mode={STATE['mode']})", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()