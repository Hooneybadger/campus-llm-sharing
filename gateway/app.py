#!/usr/bin/env python3
"""교내 LLM API 게이트웨이 — 인증, 모델 라우팅, 요청 제한, 과부하 제어, 헬스 기반 라우팅 제외.

응답 코드 정책
  401  API 키 없음·무효
  404  등록되지 않은 모델
  400  컨텍스트 초과(프롬프트가 max_model_len 또는 백엔드 한 대의 토큰 예산을 넘음) — 백엔드에 보내기 전에 거부
  429  사용자 한도 초과(RPM, 동시 요청) — 사용자 쪽 문제. Retry-After 포함
  503  시스템 과부하(모델별 대기열 가득 참·대기 시한 초과) 또는 정상 백엔드 없음. Retry-After 포함
  502  백엔드 오류(스트림 시작 전 연결 실패는 다른 정상 백엔드로 1회 재시도)

스트리밍 오류 정책
  200 헤더를 보낸 뒤 업스트림이 끊기거나, 오류 이벤트를 보내거나, [DONE] 없이 끝나면 `data: {"error": {...}}` 이벤트
  하나를 보내고 [DONE] 없이 닫는다.
  마지막 usage 청크를 못 받은 요청의 출력 토큰은 전달한 텍스트 이벤트 수로 추정해 기록하고 추정치임을 표시한다.

정책 모드
  enforce     : 위 정책 전부 적용 (기본)
  passthrough : 인증·라우팅만. 백엔드(vLLM) 내부 대기열에 무제한으로 쌓인다 — P2-5 "정책 전" 기준선

설정: gateway/routes.yaml (모델 → 백엔드·정책). 파일 변경을 감지해 재시작 없이 반영한다(P3-5 자동 등록).
"""
import argparse
import asyncio
import collections
import hashlib
import json
import os
import time
from pathlib import Path

import httpx
import uvicorn
import yaml
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest
from tokenizers import Tokenizer

REG = CollectorRegistry()
# 입장 방식: "tokens"(kv_tokens_per_backend가 있는 모델은 토큰 예산) | "count"(건수 한도만). P2-5 비교용으로 관리 API에서 전환
ADMISSION = {"mode": os.environ.get("GW_ADMISSION", "tokens")}
M_REQ = Counter("gw_requests_total", "requests by outcome", ["model", "code", "reason"], registry=REG)
M_INFLIGHT = Gauge("gw_inflight", "in-flight requests at backends", ["model"], registry=REG)
M_QUEUE = Gauge("gw_queue_length", "requests waiting in gateway queue", ["model"], registry=REG)
M_QWAIT = Histogram("gw_queue_wait_seconds", "time spent in gateway queue", ["model"], registry=REG,
                    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 4, 8, 15, 30, 60])
M_TTFT = Histogram("gw_ttft_seconds", "TTFT observed at gateway (incl. queue)", ["model"], registry=REG,
                   buckets=[0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 5, 8, 13, 20, 40, 80])
M_TOK = Counter("gw_tokens_total", "tokens by user tier", ["model", "tier", "kind"], registry=REG)
M_BACKEND_UP = Gauge("gw_backend_up", "backend health (1 healthy)", ["model", "backend"], registry=REG)
M_BACKEND_ERR = Counter("gw_backend_errors_total", "backend errors", ["model", "backend", "kind"], registry=REG)
M_USAGE_EST = Counter("gw_usage_estimated_total", "streams recorded without a final usage chunk", ["model", "outcome"], registry=REG)
# 끊긴 스트림의 마지막 이벤트. OpenAI SDK는 data의 error 키를 보고 예외를 던진다 (TS-006)
STREAM_ERROR = (b'data: {"error": {"message": "Upstream model server connection lost mid-stream. '
                b'Retry the request.", "type": "server_error", "code": "backend_stream_broken"}}\n\n')


class Backend:
    def __init__(self, url: str, healthy: bool = True):
        self.url = url.rstrip("/")
        self.healthy = healthy
        self.draining = False   # 대여 회수 전 drain: 새 요청은 보내지 않고 진행 중 요청만 마저 처리
        self.fail = 0
        self.ok = 0
        self.inflight = 0
        self.tokens = 0         # 이 백엔드에 예약된 토큰(프롬프트 + max_tokens 합). 예약 상한은 ModelRoute.backend_budget


class Slot:
    """입장 허가 한 건. 토큰 모드에서는 입장하는 순간 어느 백엔드의 KV 몫을 쓸지 정해 둔다."""

    def __init__(self, cost: int = 0):
        self.cost = cost
        self.backend: Backend | None = None
        self.t_queued = 0.0


class ModelRoute:
    """모델 하나의 백엔드 풀과 입장 제어(동시 처리 슬롯 + 유한 FIFO 대기열)."""

    def __init__(self, name: str, cfg: dict):
        self.name = name
        self.backends: dict[str, Backend] = {}
        self.discovered: list[str] = []
        self.update(cfg)
        self.inflight = 0
        self.inflight_tokens = 0
        self.waiters: collections.deque = collections.deque()   # (future, cost) — 엄격한 FIFO

    def update(self, cfg: dict):
        self.cfg = cfg
        # 백엔드당 한도가 있으면 "백엔드당 × 정상 백엔드 수"로 동적 계산한다. 복제 하나가 죽으면 수용량도 줄어
        # 남은 복제가 KV 포화(TS-017 불안정 구간)로 밀려나지 않게 한다.
        self.per_backend = int(cfg["max_inflight_per_backend"]) if "max_inflight_per_backend" in cfg else None
        self._static_max = int(cfg.get("max_inflight", 64))
        # 토큰 기준 입장 제어(P2-4 v2, P5-4 백엔드별 예약): 백엔드마다 예약 합이 KV × kv_fill 이내일 때만 받는다
        self.kv_tokens = int(cfg["kv_tokens_per_backend"]) if "kv_tokens_per_backend" in cfg else None
        self.kv_fill = float(cfg.get("kv_fill", 0.9))
        self.queue_max = int(cfg.get("queue_max", 64))
        self.queue_timeout = float(cfg.get("queue_timeout_s", 10))
        # 이 크기 이하의 요청은 막힌 맨 앞을 앞지를 수 있다(0이면 엄격한 FIFO). 맨 앞이 bypass_age_s 넘게 기다리면 앞지르기를 멈춘다
        self.bypass_tokens = int(cfg.get("bypass_tokens", 0))
        self.bypass_age = float(cfg.get("bypass_age_s", 10))
        self.max_model_len = int(cfg.get("max_model_len", 8192))
        self.min_max_tokens = int(cfg.get("min_max_tokens", 0))
        tok = cfg.get("tokenizer")
        self.tok = Tokenizer.from_file(tok) if tok and os.path.exists(tok) else None
        self.sync_backends()

    def sync_backends(self):
        """정적 backends와 k8s 디스커버리 결과를 합쳐 백엔드 목록을 맞춘다(기존 백엔드의 헬스 상태는 유지)."""
        want = list(dict.fromkeys(list(self.cfg.get("backends", [])) + self.discovered))
        static = set(self.cfg.get("backends", []))
        for u in want:
            if u not in self.backends:
                # 디스커버리는 준비 안 된 Pod 주소까지 받아 온다(publishNotReadyAddresses). 새 주소를 정상으로 시작하면
                # 모델 포트가 열리기 전에 요청을 보내고 정상으로 집계한다(2026-11-05 Pod 삭제 주입에서 확인).
                # 디스커버리로 새로 찾은 주소는 헬스체크를 ok_threshold번 통과한 뒤에 보낸다.
                self.backends[u] = Backend(u, healthy=u in static)
        for u in list(self.backends):
            if u not in want:
                del self.backends[u]
                # 사라진 백엔드의 게이지 시계열을 지운다. 남겨 두면 Pod 재시작(IP 변경) 때마다
                # gw_backend_up=0이 영구히 남아 BackendExcluded 거짓 알림이 쌓인다(TS-022)
                for metric in (M_BACKEND_UP,):
                    try:
                        metric.remove(self.name, u)
                    except KeyError:
                        pass

    @property
    def max_inflight(self) -> int:
        n = max(1, len(self.healthy()))
        # 토큰 모드에서는 건수 상한을 엔진의 max_num_seqs로 둔다(입장은 토큰 예산이 결정)
        if self.token_budget is not None and "max_seqs_per_backend" in self.cfg:
            return int(self.cfg["max_seqs_per_backend"]) * n
        if self.per_backend is None:
            return self._static_max
        return self.per_backend * n

    def healthy(self) -> list[Backend]:
        return [b for b in self.backends.values() if b.healthy and not b.draining]

    def pick(self, exclude=(), slot: Slot | None = None) -> Backend | None:
        """입장 때 예약한 백엔드로 보낸다. 그 백엔드가 빠졌으면 자리가 남은 다른 백엔드로 예약을 옮긴다."""
        hs = [b for b in self.healthy() if b.url not in exclude]
        if slot is None or slot.backend is None:
            return min(hs, key=lambda b: b.inflight) if hs else None
        if slot.backend in hs:
            return slot.backend
        slot.backend.tokens -= slot.cost
        slot.backend = self.place(slot.cost, hs)
        if slot.backend:
            slot.backend.tokens += slot.cost
        return slot.backend

    @property
    def backend_budget(self) -> int | None:
        """백엔드 한 대에 예약할 수 있는 토큰. KV는 프로세스마다 따로라 다른 백엔드의 여유를 빌려 쓸 수 없다."""
        if self.kv_tokens is None or ADMISSION["mode"] != "tokens":
            return None
        return int(self.kv_tokens * self.kv_fill)

    @property
    def token_budget(self) -> int | None:
        """모델 전체 예산(표시용). 입장 판단은 backend_budget과 백엔드별 예약으로 한다."""
        bb = self.backend_budget
        return None if bb is None else bb * max(1, len(self.healthy()))

    def place(self, cost: int, candidates: list[Backend] | None = None) -> Backend | None:
        """이 요청이 들어갈 자리가 있는 백엔드 중 예약이 가장 적은 곳."""
        room = [b for b in (self.healthy() if candidates is None else candidates) if b.tokens + cost <= self.backend_budget]
        return min(room, key=lambda b: b.tokens) if room else None

    def fits(self, cost: int) -> bool:
        if self.inflight >= self.max_inflight:
            return False
        return self.backend_budget is None or self.place(cost) is not None

    def may_bypass(self, slot: Slot) -> bool:
        return (0 < slot.cost <= self.bypass_tokens and bool(self.waiters)
                and time.monotonic() - self.waiters[0][1].t_queued < self.bypass_age)

    def _admit(self, slot: Slot):
        self.inflight += 1
        self.inflight_tokens += slot.cost
        if slot.cost and self.backend_budget is not None:
            slot.backend = self.place(slot.cost)
            if slot.backend:
                slot.backend.tokens += slot.cost

    async def acquire(self, enforce: bool, slot: Slot | None = None) -> str | None:
        """슬롯을 얻으면 None, 거부하면 사유 문자열. slot.cost = 프롬프트 토큰 + max_tokens(토큰 모드에서만 의미)."""
        slot = slot or Slot()
        if not enforce:
            self._admit(slot)
            return None
        if (not self.waiters or self.may_bypass(slot)) and self.fits(slot.cost):
            self._admit(slot)
            return None
        if len(self.waiters) >= self.queue_max:
            return "queue_full"
        fut = asyncio.get_running_loop().create_future()
        slot.t_queued = time.monotonic()
        entry = (fut, slot)
        self.waiters.append(entry)
        M_QUEUE.labels(self.name).set(len(self.waiters))
        try:
            await asyncio.wait_for(asyncio.shield(fut), self.queue_timeout)
            return None
        except asyncio.TimeoutError:
            if fut.done() and not fut.cancelled():   # 시한 직전에 입장한 경우
                return None
            fut.cancel()
            return "queue_timeout"
        finally:
            if entry in self.waiters:
                self.waiters.remove(entry)
            M_QUEUE.labels(self.name).set(len(self.waiters))

    def release(self, slot: Slot | None = None):
        slot = slot or Slot()
        self.inflight -= 1
        self.inflight_tokens -= slot.cost
        if slot.backend:
            slot.backend.tokens -= slot.cost
        self.pump()

    def pump(self):
        """대기열 앞에서부터 들어갈 수 있는 만큼 입장시킨다(앞이 안 들어가면 멈추고, 설정된 경우 작은 요청만 앞지른다).
        반납 때와, 백엔드 복구로 한도가 늘었을 때(헬스 루프) 호출된다."""
        while self.waiters:
            fut, slot = self.waiters[0]
            if fut.done():
                self.waiters.popleft()
                continue
            if not self.fits(slot.cost):
                break
            self.waiters.popleft()
            self._admit(slot)
            fut.set_result(True)
        if self.bypass_tokens:
            for entry in list(self.waiters)[1:]:
                fut, slot = entry
                if not fut.done() and self.may_bypass(slot) and self.fits(slot.cost):
                    self.waiters.remove(entry)
                    self._admit(slot)
                    fut.set_result(True)
        M_QUEUE.labels(self.name).set(len(self.waiters))


def kst_day(ts: float | None = None) -> str:
    """한국 표준시(UTC+9, 일광 절약 없음) 날짜. 컨테이너 TZ와 무관하게 자정 기준을 맞춘다."""
    return time.strftime("%Y-%m-%d", time.gmtime((time.time() if ts is None else ts) + 9 * 3600))


class UsageBook:
    """사용자별 오늘 사용량과 최근 요청 기록 — 실습 포털의 '사용량' 화면과 운영 화면의 사용자 표가 읽는다.
    게이트웨이 메모리에만 두므로 재시작하면 0부터 다시 센다(장기 집계는 Prometheus gw_tokens_total)."""

    def __init__(self, recent: int = 20):
        self.day = kst_day()
        self.users: dict[str, dict] = {}
        self.recent = recent

    def get(self, uid: str) -> dict:
        today = kst_day()
        if today != self.day:
            self.day, self.users = today, {}
        if uid not in self.users:
            self.users[uid] = {"requests": 0, "codes": collections.Counter(), "prompt_tokens": 0,
                               "completion_tokens": 0, "last_ts": None,
                               "recent": collections.deque(maxlen=self.recent)}
        return self.users[uid]

    def record(self, uid: str, model: str, code: int | str, reason: str, prompt_tokens: int = 0,
               completion_tokens: int = 0, ttft_s: float | None = None, dur_s: float | None = None,
               estimated: bool = False):
        u = self.get(uid)
        ts = time.time()
        u["requests"] += 1
        u["codes"][str(code)] += 1
        u["prompt_tokens"] += prompt_tokens
        u["completion_tokens"] += completion_tokens
        u["last_ts"] = ts
        u["recent"].appendleft({"ts": ts, "model": model, "code": str(code), "reason": reason,
                                "prompt_tokens": prompt_tokens, "completion_tokens": completion_tokens,
                                "usage": "estimated" if estimated else "reported",
                                "ttft_s": None if ttft_s is None else round(ttft_s, 3),
                                "dur_s": None if dur_s is None else round(dur_s, 3)})

    def summary(self, uid: str) -> dict:
        u = self.get(uid)
        return {"day": self.day, "requests": u["requests"], "codes": dict(u["codes"]),
                "prompt_tokens": u["prompt_tokens"], "completion_tokens": u["completion_tokens"], "last_ts": u["last_ts"]}


class Gateway:
    def __init__(self, routes_path: Path, keys_path: Path, mode: str):
        self.routes_path, self.keys_path, self.cli_mode = routes_path, keys_path, mode
        self.mode = mode or "enforce"
        self.routes: dict[str, ModelRoute] = {}
        self.routes_mtime = 0.0
        kj = json.loads(keys_path.read_text())
        self.keys, self.tiers = kj["keys"], kj["tiers"]
        self.usage = UsageBook()
        self.user_inflight: dict[str, int] = collections.defaultdict(int)
        self.user_hits: dict[str, collections.deque] = collections.defaultdict(collections.deque)
        self.client = httpx.AsyncClient(limits=httpx.Limits(max_connections=4000, max_keepalive_connections=1000))
        self.health_cfg = {"interval_s": 2.0, "timeout_s": 1.5, "fail_threshold": 2, "ok_threshold": 2}
        self.load_routes()

    def load_routes(self):
        mt = self.routes_path.stat().st_mtime
        if mt == self.routes_mtime:
            return
        cfg = yaml.safe_load(self.routes_path.read_text())
        self.health_cfg.update(cfg.get("health", {}))
        # 정책 모드 우선순위: CLI(--mode) > 환경변수 GW_MODE > routes 파일 mode > enforce
        self.mode = self.cli_mode or os.environ.get("GW_MODE") or cfg.get("mode") or "enforce"
        new = cfg["models"]
        for name, mc in new.items():
            if name in self.routes:
                self.routes[name].update(mc)
            else:
                self.routes[name] = ModelRoute(name, mc)
        for name in list(self.routes):
            if name not in new:
                del self.routes[name]
        self.routes_mtime = mt
        print(f"[gateway] routes loaded: {list(self.routes)} mode={self.mode}", flush=True)

    async def watch_routes(self):
        while True:
            await asyncio.sleep(2)
            try:
                self.load_routes()
            except Exception as e:  # noqa: BLE001 — 잘못된 파일이 와도 기존 라우트를 유지
                print(f"[gateway] routes reload failed, keeping previous: {e}", flush=True)

    async def discovery_loop(self):
        """routes의 discovery 항목: 헤드리스 서비스의 EndpointSlice에서 Pod IP를 찾아 백엔드로 쓴다.
        소형 3종 Pod는 모델 3개가 한 Pod에 있어 k8s readiness가 Pod 단위(전부/전무)다. 그래서 준비 안 된 주소까지
        받아 오고(publishNotReadyAddresses), 모델 포트별 헬스 판정은 게이트웨이 헬스체크가 맡는다."""
        if not any(r.cfg.get("discovery") for r in self.routes.values()):
            return
        from kubernetes import client, config
        config.load_incluster_config()
        api = client.DiscoveryV1Api()

        def fetch(ns, svc):
            return api.list_namespaced_endpoint_slice(ns, label_selector=f"kubernetes.io/service-name={svc}").items

        while True:
            for r in list(self.routes.values()):
                ds = r.cfg.get("discovery")
                if not ds:
                    continue
                ds = ds if isinstance(ds, list) else [ds]   # 본 서비스 + 대여 풀 확장 서비스(serving-*-x)
                try:
                    urls = []
                    for d in ds:
                        slices = await asyncio.to_thread(fetch, d.get("namespace", "llm-serving"), d["service"])
                        for sl in slices:
                            port = next((p.port for p in (sl.ports or []) if p.name == d.get("port_name")), d.get("port"))
                            for ep in sl.endpoints or []:
                                urls += [f"http://{a}:{port}" for a in ep.addresses]
                    if sorted(urls) != sorted(r.discovered):
                        print(f"[gateway] discovery {r.name}: {r.discovered} -> {urls}", flush=True)
                        r.discovered = urls
                        r.sync_backends()
                except Exception as e:  # noqa: BLE001 — API 일시 오류 시 이전 목록 유지
                    print(f"[gateway] discovery error {r.name}: {e}", flush=True)
            await asyncio.sleep(5)

    async def health_loop(self):
        hc = self.health_cfg
        while True:
            checks = []
            for r in list(self.routes.values()):
                for b in list(r.backends.values()):
                    checks.append(self.check(r, b))
            await asyncio.gather(*checks)
            for r in list(self.routes.values()):
                r.pump()
            await asyncio.sleep(hc["interval_s"])

    async def check(self, r: ModelRoute, b: Backend):
        hc = self.health_cfg
        try:
            resp = await self.client.get(b.url + "/health", timeout=hc["timeout_s"])
            good = resp.status_code == 200
        except httpx.HTTPError:
            good = False
        self.mark(r, b, good, "health")

    def mark(self, r: ModelRoute, b: Backend, good: bool, kind: str):
        hc = self.health_cfg
        if good:
            b.ok, b.fail = b.ok + 1, 0
            if not b.healthy and b.ok >= hc["ok_threshold"]:
                b.healthy = True
                print(f"[gateway] backend RECOVERED {r.name} {b.url}", flush=True)
        else:
            b.fail, b.ok = b.fail + 1, 0
            M_BACKEND_ERR.labels(r.name, b.url, kind).inc()
            if b.healthy and b.fail >= hc["fail_threshold"]:
                b.healthy = False
                print(f"[gateway] backend EXCLUDED {r.name} {b.url} ({kind})", flush=True)
        M_BACKEND_UP.labels(r.name, b.url).set(1 if b.healthy else 0)

    # ------------------------------------------------------------------ user limits
    def user_limit(self, user: dict) -> tuple[str, float] | None:
        t = self.tiers[user["tier"]]
        uid = user["user_id"]
        now = time.monotonic()
        hits = self.user_hits[uid]
        while hits and now - hits[0] > 60:
            hits.popleft()
        if len(hits) >= t["rpm"]:
            return "user_rpm", round(60 - (now - hits[0]) + 0.05, 2)
        if self.user_inflight[uid] >= t["max_concurrent"]:
            return "user_concurrency", 1.0
        hits.append(now)
        return None

    def rpm_used(self, uid: str) -> int:
        now = time.monotonic()
        return sum(1 for h in self.user_hits.get(uid, ()) if now - h <= 60)

    def user_of(self, req: Request) -> dict | None:
        auth = req.headers.get("authorization", "")
        key = auth[7:] if auth.lower().startswith("bearer ") else ""
        return self.keys.get(hashlib.sha256(key.encode()).hexdigest())

    def count_prompt(self, r: ModelRoute, body: dict) -> int:
        if not r.tok:
            return 0
        n = 0
        for m in body.get("messages", []):
            c = m.get("content")
            if isinstance(c, str):
                n += len(r.tok.encode(c).ids) + 4
            elif isinstance(c, list):
                n += sum(len(r.tok.encode(p.get("text", "")).ids) for p in c if p.get("type") == "text")
            for tc in m.get("tool_calls") or []:
                n += len(r.tok.encode(tc.get("function", {}).get("arguments", "")).ids) + 8
        if body.get("tools"):
            n += len(r.tok.encode(json.dumps(body["tools"], ensure_ascii=False)).ids)
        return n


def err(code: int, msg: str, model: str, reason: str, retry_after: float | None = None, etype: str = "invalid_request_error"):
    M_REQ.labels(model, str(code), reason).inc()
    headers = {"Retry-After": str(max(1, int(round(retry_after))))} if retry_after else None
    return JSONResponse({"error": {"message": msg, "type": etype, "code": reason}}, status_code=code, headers=headers)


def synthesize_sse(raw: bytes):
    """비스트리밍 chat.completion 응답을 OpenAI 스트리밍 청크 형식으로 바꾼다(TS-019 우회)."""
    try:
        d = json.loads(raw)
    except json.JSONDecodeError:
        yield b'data: {"error": {"message": "invalid upstream response", "code": "bad_upstream"}}\n\n'
        return
    choice = (d.get("choices") or [{}])[0]
    msg = choice.get("message") or {}
    delta = {"role": "assistant"}
    if msg.get("content"):
        delta["content"] = msg["content"]
    if msg.get("tool_calls"):
        delta["tool_calls"] = [{"index": i, **tc} for i, tc in enumerate(msg["tool_calls"])]
    base = {"id": d.get("id"), "object": "chat.completion.chunk", "created": d.get("created"), "model": d.get("model")}
    yield ("data: " + json.dumps({**base, "choices": [{"index": 0, "delta": delta, "finish_reason": None}]}, ensure_ascii=False) + "\n\n").encode()
    yield ("data: " + json.dumps({**base, "choices": [{"index": 0, "delta": {}, "finish_reason": choice.get("finish_reason")}],
                                  "usage": d.get("usage")}, ensure_ascii=False) + "\n\n").encode()
    yield b"data: [DONE]\n\n"


def build_app(gw: Gateway) -> FastAPI:
    app = FastAPI()

    @app.on_event("startup")
    async def _bg():
        asyncio.create_task(gw.health_loop())
        asyncio.create_task(gw.watch_routes())
        asyncio.create_task(gw.discovery_loop())

    @app.get("/healthz")
    async def healthz():
        return PlainTextResponse("ok")

    @app.get("/metrics")
    async def metrics():
        for r in gw.routes.values():
            M_INFLIGHT.labels(r.name).set(r.inflight)
        return PlainTextResponse(generate_latest(REG).decode(), media_type="text/plain; version=0.0.4")

    @app.get("/admin/state")
    async def state():
        return {"mode": gw.mode, "admission": ADMISSION["mode"], "models": {n: {
            "inflight": r.inflight, "queue": len(r.waiters), "max_inflight": r.max_inflight, "queue_max": r.queue_max,
            "inflight_tokens": r.inflight_tokens, "token_budget": r.token_budget, "backend_budget": r.backend_budget,
            "backends": {b.url: {"healthy": b.healthy, "draining": b.draining, "inflight": b.inflight, "fail": b.fail,
                                 "tokens": b.tokens}
                         for b in r.backends.values()}}
            for n, r in gw.routes.items()}}

    @app.get("/admin/usage")
    async def usage_all(req: Request):
        """오늘(KST) 요청이 많은 사용자 순. 학번이 담기므로 운영자 토큰을 요구한다."""
        tok = os.environ.get("GW_ADMIN_TOKEN")
        if tok and req.headers.get("x-admin-token") != tok:
            return JSONResponse({"error": {"message": "admin token required"}}, status_code=403)
        limit = int(req.query_params.get("limit", "20"))
        meta = {u["user_id"]: u for u in gw.keys.values()}
        rows = []
        for uid in list(gw.usage.users):
            s = gw.usage.summary(uid)
            m = meta.get(uid, {})
            rows.append({"user_id": uid, "group": m.get("group"), "tier": m.get("tier"),
                         "inflight": gw.user_inflight.get(uid, 0), **{k: v for k, v in s.items() if k != "day"}})
        rows.sort(key=lambda x: x["requests"], reverse=True)
        return {"day": gw.usage.day, "users": rows[:limit], "active_users": len(rows)}

    @app.get("/v1/me")
    async def me(req: Request):
        """API 키 주인의 등급 한도, 지금 쓰고 있는 양, 오늘 사용량, 최근 요청 — 포털 '사용량' 화면용."""
        user = gw.user_of(req)
        if not user:
            return err(401, "Invalid API key. 정보전산원에서 발급받은 키를 확인하세요.", "-", "auth", etype="authentication_error")
        uid, t = user["user_id"], gw.tiers[user["tier"]]
        return {"user_id": uid, "group": user.get("group"), "team": user.get("team"), "tier": user["tier"],
                "limits": {"rpm": t["rpm"], "max_concurrent": t["max_concurrent"], "max_tokens_cap": t["max_tokens_cap"]},
                "now": {"rpm_used": gw.rpm_used(uid), "inflight": gw.user_inflight.get(uid, 0)},
                "today": gw.usage.summary(uid), "recent": list(gw.usage.get(uid)["recent"])}

    @app.post("/admin/drain")
    async def drain(req: Request):
        """대여 관리자가 풀 GPU를 회수하기 전에 호출: {"host": "<pod ip>", "drain": true|false}
        해당 호스트의 모든 백엔드를 신규 라우팅에서 빼고, 남은 진행 중 요청 수를 돌려준다."""
        # 사용자에게 열린 NodePort로도 /admin이 노출되므로 변경 계열은 운영자 토큰을 요구한다
        tok = os.environ.get("GW_ADMIN_TOKEN")
        if tok and req.headers.get("x-admin-token") != tok:
            return JSONResponse({"error": {"message": "admin token required"}}, status_code=403)
        body = await req.json()
        host, flag = body["host"], bool(body.get("drain", True))
        left = 0
        for r in gw.routes.values():
            for b in r.backends.values():
                if f"//{host}:" in b.url:
                    b.draining = flag
                    left += b.inflight
        return {"host": host, "draining": flag, "inflight": left}

    @app.post("/admin/mode")
    async def set_mode(req: Request):
        """정책 모드 즉시 전환(P2-5 ABAB 비교용): {"mode": "enforce"|"passthrough"}. 다음 routes 재적재 전까지 유지."""
        tok = os.environ.get("GW_ADMIN_TOKEN")
        if tok and req.headers.get("x-admin-token") != tok:
            return JSONResponse({"error": {"message": "admin token required"}}, status_code=403)
        body = await req.json()
        mode = body.get("mode", gw.mode)
        if mode not in ("enforce", "passthrough") or body.get("admission", "tokens") not in ("tokens", "count"):
            return JSONResponse({"error": {"message": "mode: enforce|passthrough, admission: tokens|count"}}, status_code=400)
        gw.cli_mode = mode     # routes 파일 재적재 때도 유지되도록 최우선 값으로 둔다
        gw.mode = mode
        if "admission" in body:
            ADMISSION["mode"] = body["admission"]
        print(f"[gateway] mode -> {mode}, admission -> {ADMISSION['mode']}", flush=True)
        return {"mode": gw.mode, "admission": ADMISSION["mode"]}

    @app.get("/v1/models")
    async def models():
        return {"object": "list", "data": [{"id": n, "object": "model", "max_model_len": r.max_model_len,
                                            "available": bool(r.healthy())} for n, r in gw.routes.items()]}


    return app


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--routes", default="gateway/routes.k8s.yaml")
    ap.add_argument("--keys", default=".secrets/gateway_keys.json")
    ap.add_argument("--mode", default=None, choices=["enforce", "passthrough"])
    ap.add_argument("--port", type=int, default=8080)
    a = ap.parse_args()
    gw = Gateway(Path(a.routes), Path(a.keys), a.mode)
    uvicorn.run(build_app(gw), host="0.0.0.0", port=a.port, log_level="warning", timeout_keep_alive=75)


if __name__ == "__main__":
    main()
