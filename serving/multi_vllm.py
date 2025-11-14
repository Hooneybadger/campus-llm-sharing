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
