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
