"""슈퍼바이저의 프로세스 그룹 경계 테스트 — 실제 프로세스를 띄워 한 모델의 정리가 다른 모델과 슈퍼바이저에 닿지 않는지 본다.
vLLM 대신 python 자식을 쓰므로 그룹 경계만 검증한다. vLLM이 실제로 만드는 프로세스 구성은 P5-3의 Pod 프로세스 표로 확인한다.
"""
import ctypes
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "serving"))
import multi_vllm  # noqa: E402

# API 서버를 흉내 낸 리더가 엔진 역할의 자식을 만들고 자식의 PID를 알린다
LEADER = "import subprocess,sys,time; p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); print(p.pid,flush=True); time.sleep(60)"


class FakeChild(multi_vllm.Child):
    def __init__(self, cid):
        super().__init__({"id": cid, "port": 1}, {"dir": "x", "served_name": cid}, {"max_restarts": 3, "backoff_s": [1], "stable_s": 1})

    def start(self):
        self.proc = subprocess.Popen([sys.executable, "-c", LEADER], stdout=subprocess.PIPE, text=True, start_new_session=True)
        self.engine = int(self.proc.stdout.readline())
        self.state, self.t_start, self.t_ready = "ready", time.time(), time.time()


def alive(pid):
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().rsplit(")", 1)[1].split()[0] != "Z"
    except FileNotFoundError:
        return False


def sup(*children):
    s = multi_vllm.Supervisor.__new__(multi_vllm.Supervisor)
    s.children, s.startup, s.stopping = list(children), "sequential", False
    return s


def test_each_model_is_its_own_group_separate_from_supervisor():
    a, b = FakeChild("a"), FakeChild("b")
    a.start(), b.start()
    try:
        assert os.getpgid(a.proc.pid) == a.proc.pid != os.getpgrp()
        assert os.getpgid(a.engine) == a.proc.pid                  # 자식은 리더의 그룹에 속한다
        assert os.getpgid(b.proc.pid) != a.proc.pid
    finally:
        a.stop(signal.SIGKILL), b.stop(signal.SIGKILL)


def test_group_cleanup_kills_orphan_but_not_other_model():
    a, b = FakeChild("a"), FakeChild("b")
    a.start(), b.start()
    try:
        os.kill(a.proc.pid, signal.SIGKILL)                        # API 서버만 죽는다
        a.proc.wait()
        assert alive(a.engine)                                     # 엔진은 고아로 남는다
        sup(a, b).handle_exit(a)
        for _ in range(40):
            if not alive(a.engine):
                break
            time.sleep(0.05)
        assert not alive(a.engine)
        assert alive(b.proc.pid) and alive(b.engine)               # 다른 모델은 그대로
        assert a.exits[-1]["orphans"] >= 1
    finally:
        b.stop(signal.SIGKILL)


def test_zombie_orphans_do_not_delay_restart():
    ctypes.CDLL(None).prctl(36, 1, 0, 0, 0)                        # PR_SET_CHILD_SUBREAPER: 컨테이너의 PID 1처럼 고아를 물려받는다
    a = FakeChild("a")
    a.start()
    os.kill(a.proc.pid, signal.SIGKILL)
    a.proc.wait()
    os.kill(a.engine, signal.SIGKILL)                              # 고아가 죽어 좀비가 된다
    time.sleep(0.2)
    with open(f"/proc/{a.engine}/stat") as f:
        assert f.read().rsplit(")", 1)[1].split()[0] == "Z"
    assert a.group_alive() == []                                   # 좀비는 남은 프로세스로 세지 않고 거둬들인다
    assert not os.path.exists(f"/proc/{a.engine}")


def test_streak_resets_after_stable_service_but_counts_crash_loop():
    a = FakeChild("a")
    a.start()
    a.stop(signal.SIGKILL)
    a.proc.wait()
    a.streak, a.t_ready = 3, time.time() - 5                       # stable_s(1초)보다 오래 정상 서비스했다
    sup(a).handle_exit(a)
    assert a.state == "backoff" and a.streak == 0
    a.streak, a.t_ready = 3, None                                  # 기동도 못 하고 또 죽었다
    a.proc = subprocess.Popen(["true"])
    a.proc.wait()
    sup(a).handle_exit(a)
    assert a.state == "failed"


def test_restart_waits_until_gpu_memory_returns(monkeypatch):
    a = FakeChild("a")
    a.held_mib = 1000
    seq = iter([200, 400, 1200])
    monkeypatch.setattr(multi_vllm, "gpu_free_mib", lambda: next(seq))
    waited, free = sup(a).wait_gpu_released(a)
    assert free == 1200 and waited > 0
