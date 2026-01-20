#!/usr/bin/env python3
"""GPU Xid 오류 exporter — 커널 로그(/dev/kmsg)의 "NVRM: Xid (PCI:...): <code>, ..." 줄을 세어 노출한다.

GeForce(RTX 5090)는 DCGM의 XID 필드를 지원하지 않는다(Phase 0 확인). 그래서 GPU 하드웨어·드라이버 오류는
커널 로그로 감지한다. 메트릭: gpu_xid_errors_total{node, pci, xid}, :9500/metrics
표준 라이브러리만 사용(가벼운 DaemonSet용).
"""
import os
import re
import threading
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

NODE = os.environ.get("NODE_NAME", "unknown")
PAT = re.compile(r"NVRM: Xid \(PCI:([0-9a-fA-F:.]+)\): (\d+)")
counts: Counter = Counter()
lock = threading.Lock()


def follow():
    # /dev/kmsg는 열 때 처음부터 읽힌다 → 기동 이전 Xid도 1회 반영된다(누적 카운터 의미상 맞음)
    with open("/dev/kmsg", "rb", buffering=0) as f:
        while True:
            try:
                line = f.read(8192).decode(errors="replace")
            except OSError:      # EPIPE: 링버퍼가 덮어써짐 → 계속 읽는다
                continue
            m = PAT.search(line)
            if m:
                with lock:
                    counts[(m.group(1), m.group(2))] += 1
                print(f"[xid] {NODE} {line.strip()[:200]}", flush=True)


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        with lock:
            body = "# TYPE gpu_xid_errors_total counter\n" + "".join(
                f'gpu_xid_errors_total{{node="{NODE}",pci="{p}",xid="{x}"}} {n}\n' for (p, x), n in counts.items())
        data = body.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


if __name__ == "__main__":
    threading.Thread(target=follow, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", 9500), H).serve_forever()
