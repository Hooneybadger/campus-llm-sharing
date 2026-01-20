#!/usr/bin/env python3
"""Alertmanager webhook 수신기 — 알림을 시각과 함께 기록한다(운영에서는 Slack·메일 수신기 자리).
장애 주입 실험에서 "장애 발생 → 알림 도착" 감지 시간을 재는 데 쓴다.
GET /alerts?since=<epoch>  최근 수신 목록(JSON)
"""
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

LOG: list[dict] = []


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, obj):
        data = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        now = time.time()
        for a in body.get("alerts", []):
            rec = {"received": now, "status": a.get("status"), "alertname": a["labels"].get("alertname"),
                   "labels": a.get("labels"), "summary": a.get("annotations", {}).get("summary"), "startsAt": a.get("startsAt")}
            LOG.append(rec)
            print(json.dumps(rec, ensure_ascii=False), flush=True)
        self._send(200, {"ok": True})

    def do_GET(self):
        q = parse_qs(urlparse(self.path).query)
        since = float(q.get("since", ["0"])[0])
        self._send(200, [r for r in LOG if r["received"] >= since])


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 9095), H).serve_forever()
