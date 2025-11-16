#!/usr/bin/env python3
"""GPU 1장에서 vLLM 프로세스 여러 개를 띄우고 관리하는 슈퍼바이저 (소형 3종 동시 상주용).

계획서 구조: "소형 모델은 한 GPU를 점유하는 Serving Pod 내부에서 별도 vLLM 프로세스로 실행".
Pod의 nvidia.com/gpu: 1 은 컨테이너 하나에만 줄 수 있으므로, 컨테이너 하나 안에서 이 슈퍼바이저가
모델별 vLLM 프로세스를 자식으로 띄운다.

- startup: sequential(이전 모델이 /health 200이 된 뒤 다음 기동 — 프로파일링 간섭 방지)
           parallel  (동시 기동 — P2-1 장애 재현용)
- 자식이 죽으면 그 모델만 백오프 후 재기동한다(다른 모델은 계속 서비스). 프로세스 그룹이 사라지고 그 모델이 쓰던
  GPU 메모리가 돌아온 것을 확인한 뒤에 띄운다.
- 재기동 한도는 크래시 루프 판정용이다. stable_s 이상 정상 서비스한 뒤의 장애는 연속 횟수를 0부터 다시 센다.
- :<health_port>/health  모든 모델 ready면 200 (readinessProbe)
  :<health_port>/livez   슈퍼바이저 생존 (livenessProbe). 재기동 한도를 넘긴 모델이 있으면 503 → Pod 재시작
  :<health_port>/status  모델별 상태·재기동 횟수·기동 시간(JSON)
  :<health_port>/metrics multi_vllm_* 메트릭(Prometheus)
의존성: 표준 라이브러리 + PyYAML (vLLM 이미지에 포함)
"""
import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import yaml

MODELS_ROOT = os.environ.get("MODELS_ROOT", "/models")


def log(msg: str):
    print(f"[multi-vllm {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def gpu_free_mib() -> int | None:
    """이 컨테이너가 보는 GPU의 여유 메모리. nvidia-smi를 쓸 수 없으면 None(확인 없이 진행)."""
    try:
        out = subprocess.run(["nvidia-smi", "--query-gpu=memory.free", "--format=csv,noheader,nounits"],
                             capture_output=True, text=True, timeout=5).stdout
        return int(out.split()[0])
    except (OSError, subprocess.SubprocessError, ValueError, IndexError):
        return None


class Child:
    def __init__(self, spec: dict, reg_model: dict, restart_cfg: dict):
        self.id = spec["id"]
        self.port = int(spec["port"])
        self.reg = reg_model
        self.budget = spec.get("budget", {})
        self.extra = spec.get("extra_args", [])
        self.restart_cfg = restart_cfg
        self.proc: subprocess.Popen | None = None
        self.state = "pending"          # pending → starting → ready | failed → backoff → starting ...
        self.restarts = 0               # 누적 재기동 수(메트릭)
        self.streak = 0                 # 정상 서비스 없이 이어진 재기동 수(재기동 한도와 백오프의 기준)
        self.free_before = self.held_mib = None
        self.exits: list[dict] = []
        self.t_start = self.t_ready = None
        self.last_startup_s = None
        self.next_start = 0.0

    def cmd(self) -> list[str]:
        c = ["vllm", "serve", f"{MODELS_ROOT}/{self.reg['dir']}", "--served-model-name", self.reg["served_name"],
             "--host", "0.0.0.0", "--port", str(self.port), *self.reg.get("vllm_args", [])]
        for k, v in self.budget.items():
            flag = "--" + k.replace("_", "-")
            if v is True:
                c.append(flag)
            elif v not in (False, None):
                c += [flag, str(v)]
        return c + list(self.extra)

    def start(self):
        cmd = self.cmd()
        log(f"start {self.id}: {' '.join(cmd)}")
        self.free_before = gpu_free_mib()
        self.proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                     start_new_session=True, bufsize=1)
        self.state, self.t_start, self.t_ready = "starting", time.time(), None
        threading.Thread(target=self._pump, args=(self.proc,), daemon=True).start()

    def _pump(self, proc):
        for line in proc.stdout:
            sys.stdout.write(f"[{self.id}] {line}")
        sys.stdout.flush()

    def healthy(self) -> bool:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/health", timeout=2) as r:
                return r.status == 200
        except Exception:  # noqa: BLE001 — 연결 거부·타임아웃 모두 "아직 아님"
            return False

    def stop(self, sig=signal.SIGTERM):
        # 리더(API 서버)가 이미 죽었어도 같은 프로세스 그룹의 EngineCore가 남아 GPU 메모리를 쥐고 있을 수 있다(TS-021)
        # → 생존 여부와 무관하게 그룹 전체에 신호를 보낸다
        if self.proc:
            try:
                os.killpg(self.proc.pid, sig)
            except ProcessLookupError:
                pass

    def group_alive(self) -> list[int]:
        """이 모델 프로세스 그룹(pgid = 리더 pid)에 남은 프로세스 목록.
        리더가 죽으면 고아는 PID 1인 이 슈퍼바이저의 자식이 되고, 죽은 뒤에도 거둬들일 때까지 좀비로 남아 getpgid가 성공한다.
        좀비는 자원을 쥐지 않으므로 거둬들이고 세지 않는다(세면 메모리가 이미 돌아왔는데도 시한까지 기다린다, P5-3)."""
        if not self.proc:
            return []
        left = []
        for d in os.listdir("/proc"):
            if d.isdigit():
                pid = int(d)
                try:
                    if os.getpgid(pid) != self.proc.pid:
                        continue
                    with open(f"/proc/{pid}/stat") as f:
                        state = f.read().rsplit(")", 1)[1].split()[0]
                except (ProcessLookupError, PermissionError, FileNotFoundError):
                    continue
                if state == "Z":
                    try:
                        os.waitpid(pid, os.WNOHANG)
                    except ChildProcessError:
                        pass
                    continue
                left.append(pid)
        return left

    def status(self) -> dict:
        return {"id": self.id, "model": self.reg["served_name"], "port": self.port, "state": self.state,
                "pid": self.proc.pid if self.proc else None, "restarts": self.restarts, "streak": self.streak,
                "held_mib": self.held_mib, "last_startup_s": self.last_startup_s, "recent_exits": self.exits[-5:],
                "budget": self.budget}


class Supervisor:
    def __init__(self, profile: dict, registry: dict):
        reg = {m["id"]: m for m in registry["models"]}
        self.profile = profile
        rc = profile.get("restart", {"max_restarts": 5, "backoff_s": [5, 10, 30, 60]})
        self.children = [Child(s, reg[s["id"]], rc) for s in profile["models"]]
        self.startup = profile.get("startup", "sequential")
        self.ready_timeout = float(profile.get("ready_timeout_s", 900))
        self.stopping = False

    def wait_ready(self, c: Child) -> bool:
        while time.time() - c.t_start < self.ready_timeout:
            if c.proc.poll() is not None:
                return False
            if c.healthy():
                return True
            time.sleep(1)
        return False

    def mark_ready(self, c: Child):
        c.state, c.t_ready = "ready", time.time()
        c.last_startup_s = round(c.t_ready - c.t_start, 1)
        free = gpu_free_mib()
        if c.free_before is not None and free is not None:
            c.held_mib = max(0, c.free_before - free)   # 기동 전후 차이 = 이 모델이 쥔 GPU 메모리(순차 기동 기준)
        log(f"ready {c.id} in {c.last_startup_s}s (holds {c.held_mib} MiB)")

    def wait_gpu_released(self, c: Child) -> tuple[float, int | None]:
        """프로세스가 사라져도 드라이버가 메모리를 돌려주기 전에 띄우면 OOM이 난다. 이 모델이 쥐고 있던 만큼 비었는지 본다.
        시한을 넘기면 그대로 진행한다 — 재기동이 실패하면 재기동 한도가 Pod 재시작으로 넘긴다."""
        t0, free = time.time(), gpu_free_mib()
        while free is not None and c.held_mib and free < c.held_mib and time.time() - t0 < 30:
            time.sleep(0.25)
            free = gpu_free_mib()
        waited = round(time.time() - t0, 2)
        if free is not None and c.held_mib and free < c.held_mib:
            log(f"{c.id}: GPU memory not released after {waited}s: free {free} MiB < held {c.held_mib} MiB")
        return waited, free

    def handle_exit(self, c: Child):
        code = c.proc.returncode
        # 오래 정상 서비스한 뒤의 장애는 크래시 루프가 아니다. 누적해 세면 수명이 긴 Pod일수록 장애 한 번에 Pod 전체가
        # 재시작된다(P5-3 반복 주입에서 같은 모델의 네 번째 장애에 재현)
        if c.t_ready and time.time() - c.t_ready >= c.restart_cfg.get("stable_s", 120):
            c.streak = 0
        # 고아가 된 EngineCore 정리 → GPU 메모리가 풀릴 때까지 기다린 뒤에야 재기동한다(TS-021)
        t_exit = time.time()
        orphans = c.group_alive()
        if orphans:
            log(f"{c.id}: leader exited but {len(orphans)} process(es) remain in group {orphans[:4]} → SIGKILL")
            c.stop(signal.SIGKILL)
            t0 = time.time()
            while c.group_alive() and time.time() - t0 < 30:
                time.sleep(0.5)
            log(f"{c.id}: group cleaned in {time.time() - t0:.1f}s")
        group_s = round(time.time() - t_exit, 2)
        released_s, free = self.wait_gpu_released(c)
        log(f"{c.id}: GPU memory check {released_s}s: free {free} MiB, held {c.held_mib} MiB")
        c.exits.append({"t": time.time(), "code": code, "after_s": round(time.time() - (c.t_start or time.time()), 1),
                        "was": c.state, "orphans": len(orphans), "group_s": group_s, "released_s": released_s,
                        "free_mib": free, "held_mib": c.held_mib})
        bo = c.restart_cfg.get("backoff_s", [5, 10, 30, 60])
        if c.streak >= c.restart_cfg.get("max_restarts", 5):
            c.state = "failed"
            log(f"EXIT {c.id} code={code}; restart limit reached → failed (manual action needed)")
            return
        delay = bo[min(c.streak, len(bo) - 1)]
        c.state, c.next_start = "backoff", time.time() + delay
        log(f"EXIT {c.id} code={code} (was {c.exits[-1]['was']}); restart #{c.restarts + 1} in {delay}s")

    def run(self):
        if self.startup == "parallel":
            for c in self.children:
                c.start()
        else:
            for c in self.children:
                c.start()
                if self.wait_ready(c):
                    self.mark_ready(c)
                else:
                    self.handle_exit(c) if c.proc.poll() is not None else log(f"timeout waiting {c.id}")
        while not self.stopping:
            for c in self.children:
                if c.state in ("starting", "ready") and c.proc.poll() is not None:
                    self.handle_exit(c)
                elif c.state == "starting" and c.healthy():
                    self.mark_ready(c)
                elif c.state == "backoff" and time.time() >= c.next_start:
                    # 다른 모델이 기동 중이면 기다린다(프로파일링 간섭 방지)
                    if self.startup == "sequential" and any(o.state == "starting" for o in self.children if o is not c):
                        continue
                    c.restarts += 1
                    c.streak += 1
                    c.start()
            time.sleep(1)

    def shutdown(self, *_):
        self.stopping = True
        log("SIGTERM: stopping children")
        for c in self.children:
            c.stop(signal.SIGTERM)
        deadline = time.time() + 25
        for c in self.children:
            while c.proc and c.proc.poll() is None and time.time() < deadline:
                time.sleep(0.5)
            c.stop(signal.SIGKILL)
        os._exit(0)


def serve_status(sup: Supervisor, port: int):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code, body, ctype="application/json"):
            data = body.encode() if isinstance(body, str) else body
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path == "/livez":
                # 장애 대응 단계: 모델별 재기동(슈퍼바이저) → 재기동 한도 초과 시 livenessProbe 실패로 Pod 전체 재시작(kubelet)
                failed = [c.id for c in sup.children if c.state == "failed"]
                if failed and sup.profile.get("escalate_on_failed", True):
                    return self._send(503, f"failed: {failed}", "text/plain")
                return self._send(200, "ok", "text/plain")
            if self.path == "/health":
                ok = all(c.state == "ready" for c in sup.children)
                return self._send(200 if ok else 503, json.dumps({c.id: c.state for c in sup.children}))
            if self.path == "/status":
                return self._send(200, json.dumps([c.status() for c in sup.children], ensure_ascii=False, indent=1))
            if self.path == "/metrics":
                lines = ["# TYPE multi_vllm_child_up gauge", "# TYPE multi_vllm_restarts_total counter",
                         "# TYPE multi_vllm_last_startup_seconds gauge"]
                for c in sup.children:
                    lab = f'model="{c.reg["served_name"]}",id="{c.id}"'
                    lines += [f"multi_vllm_child_up{{{lab}}} {1 if c.state == 'ready' else 0}",
                              f"multi_vllm_restarts_total{{{lab}}} {c.restarts}",
                              f"multi_vllm_last_startup_seconds{{{lab}}} {c.last_startup_s or 0}"]
                return self._send(200, "\n".join(lines) + "\n", "text/plain; version=0.0.4")
            self._send(404, "{}")

    ThreadingHTTPServer(("0.0.0.0", port), H).serve_forever()


def main():
    profile = yaml.safe_load(open(sys.argv[1]))
    registry = yaml.safe_load(open(sys.argv[2] if len(sys.argv) > 2 else "/etc/pknu/registry.yaml"))
    sup = Supervisor(profile, registry)
    signal.signal(signal.SIGTERM, sup.shutdown)
    signal.signal(signal.SIGINT, sup.shutdown)
    threading.Thread(target=serve_status, args=(sup, int(profile.get("health_port", 8000))), daemon=True).start()
    sup.run()


if __name__ == "__main__":
    main()
