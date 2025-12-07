#!/usr/bin/env python3
"""확장 풀 관리자 — 확장 풀 노드(라벨 pknu.ac.kr/gpu-role=pool)의 GPU를 추론 확장에 배정하고 회수한다 (P1-4, P3-5).

GPU 대여 기능은 없앴다(2026-10-28). 풀 GPU는 모두 로컬 LLM 추론에 쓴다.
GPU 슬롯 = 확장 풀 노드의 GPU 1장. 상태는 그 노드의 GPU Pod로 판정한다.
  free       : GPU를 요청한 Pod 없음
  expansion  : llm-serving 네임스페이스의 추론 확장 Pod (라벨 pknu.ac.kr/expansion=true)
  other      : 그 밖의 GPU Pod (확장 대상에서 제외)

확장: 서빙 Deployment의 Pod 템플릿을 복제해 빈 풀 GPU에 띄운다(운영자 /admin/expand, 전용 노드 장애 시 자동 페일오버).
회수: 게이트웨이 drain → 진행 중 요청 소진 대기 → Pod 삭제(/admin/reclaim).
상태는 매번 k8s에서 다시 읽는다(무상태).
"""
import asyncio
import hashlib
import json
import os
import time
from datetime import datetime, timezone

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from kubernetes import client, config
from prometheus_client import CollectorRegistry, Counter, Gauge, generate_latest

from autoscale import Config, Policy, UnitView, unit_load
from fastapi.responses import PlainTextResponse

NS_SERVING = os.environ.get("SERVING_NAMESPACE", "llm-serving")
GATEWAY = os.environ.get("GATEWAY_URL", "http://llm-gateway.llm-system.svc:8080")
GW_ADMIN_TOKEN = os.environ.get("GW_ADMIN_TOKEN", "")
DRAIN_TIMEOUT_S = float(os.environ.get("DRAIN_TIMEOUT_S", "90"))
FAILOVER_ENABLED = os.environ.get("FAILOVER_ENABLED", "true") == "true"
FAILOVER_GRACE_S = float(os.environ.get("FAILOVER_GRACE_S", "20"))
SERVING_UNITS = os.environ.get("SERVING_UNITS", "serving-small,serving-gptoss,serving-qwen30b").split(",")
AUTOSCALE_ENABLED = os.environ.get("AUTOSCALE_ENABLED", "true") == "true"
AUTOSCALE_CONFIG = json.loads(os.environ.get("AUTOSCALE_CONFIG", "{}"))   # autoscale.Config 필드(임계값, 시간표)
UNIT_MODELS = json.loads(os.environ.get("UNIT_MODELS", json.dumps({
    "serving-small": ["Qwen3-4B-Instruct-2507", "Ministral-3-3B-Instruct-2512", "Phi-4-mini-instruct"],
    "serving-gptoss": ["gpt-oss-20b"],
    "serving-qwen30b": ["Qwen3-30B-A3B-GPTQ-Int4"]})))
REG = CollectorRegistry()
M_SLOTS = Gauge("rental_slots", "pool GPU slots by state", ["state"], registry=REG)
M_EVENTS = Counter("rental_events_total", "rental events", ["event"], registry=REG)
M_ACT = Counter("autoscale_actions_total", "elastic placement actions", ["action", "unit"], registry=REG)
M_UTIL = Gauge("autoscale_unit_util", "max token-budget use among the unit's models", ["unit"], registry=REG)
M_EXTRAS = Gauge("autoscale_unit_extras", "expansion replicas per unit", ["unit", "phase"], registry=REG)

KEYS = json.loads(open(os.environ.get("KEYS_FILE", "/etc/gateway-keys/gateway_keys.json")).read())["keys"]


def now() -> float:
    return time.time()


def iso(t: float) -> str:
    return datetime.fromtimestamp(t, timezone.utc).astimezone().isoformat(timespec="seconds")


class Manager:
    def __init__(self):
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()
        self.core = client.CoreV1Api()
        self.lock = asyncio.Lock()   # 슬롯 선택~Pod 생성 구간 직렬화(같은 슬롯 이중 배정 방지)
        self.http = httpx.AsyncClient(timeout=10)

    # ---------------------------------------------------------------- 상태 판정
    def slots(self) -> list[dict]:
        nodes = self.core.list_node(label_selector="pknu.ac.kr/gpu-role=pool").items
        out = []
        for n in nodes:
            name = n.metadata.name
            gpus = int((n.status.allocatable or {}).get("nvidia.com/gpu", "0"))
            ready = any(c.type == "Ready" and c.status == "True" for c in n.status.conditions or [])
            pods = self.core.list_pod_for_all_namespaces(field_selector=f"spec.nodeName={name}").items
            gpu_pods = [p for p in pods if p.status.phase in ("Pending", "Running") and not p.metadata.deletion_timestamp
                        and any((c.resources.limits or {}).get("nvidia.com/gpu") for c in p.spec.containers)]
            state, occ, pod_ready, ready_since = "free", None, False, None
            for p in gpu_pods:
                for c in p.status.conditions or []:
                    if c.type == "Ready" and c.status == "True":
                        pod_ready, ready_since = True, c.last_transition_time.timestamp() if c.last_transition_time else now()
                lab = p.metadata.labels or {}
                occ = p
                if lab.get("pknu.ac.kr/expansion") == "true":
                    state = "expansion"
                else:
                    state = "other"
                break
            out.append({"node": name, "gpus": gpus, "ready": ready and gpus > 0, "state": state,
                        "pod": occ.metadata.name if occ else None, "namespace": occ.metadata.namespace if occ else None,
                        "pod_ip": occ.status.pod_ip if occ else None, "pod_ready": pod_ready, "ready_since": ready_since,
                        "labels": dict(occ.metadata.labels or {}) if occ else {},
                        "annotations": dict(occ.metadata.annotations or {}) if occ else {}})
        for st in ("free", "expansion", "other"):
            M_SLOTS.labels(st).set(sum(1 for s in out if s["state"] == st))
        return out

    # ---------------------------------------------------------------- 회수
    async def reclaim_expansion(self, slot: dict) -> dict:
        """확장 Pod 회수: 게이트웨이 drain → 진행 중 요청 소진(최대 DRAIN_TIMEOUT_S) → Pod 삭제 → 노드 GPU 반환 대기"""
        t0 = now()
        hdr = {"x-admin-token": GW_ADMIN_TOKEN} if GW_ADMIN_TOKEN else {}
        left = None
        try:
            while now() - t0 < DRAIN_TIMEOUT_S:
                r = await self.http.post(f"{GATEWAY}/admin/drain", json={"host": slot["pod_ip"], "drain": True}, headers=hdr)
                left = r.json().get("inflight", 0)
                if left == 0:
                    break
                await asyncio.sleep(2)
        except httpx.HTTPError as e:
            print(f"[rental] drain call failed ({e}); deleting anyway", flush=True)
        drained_s = round(now() - t0, 1)
        self.core.delete_namespaced_pod(slot["pod"], slot["namespace"], grace_period_seconds=30)
        while now() - t0 < DRAIN_TIMEOUT_S + 120:     # GPU가 실제로 비워질 때까지 기다린다
            if not any(s["node"] == slot["node"] and s["state"] == "expansion" for s in self.slots()):
                break
            await asyncio.sleep(2)
        M_EVENTS.labels("reclaim_expansion").inc()
        return {"node": slot["node"], "drain_s": drained_s, "inflight_left": left, "total_s": round(now() - t0, 1)}

    async def refresh(self):
        """슬롯 지표(rental_slots) 갱신(30초 주기)."""
        while True:
            try:
                self.slots()
            except Exception as e:  # noqa: BLE001
                print(f"[pool] refresh error: {e}", flush=True)
            await asyncio.sleep(30)

    # ---------------------------------------------------------------- 자동 페일오버
    async def failover_loop(self):
        """전용 노드 장애로 서빙 유닛의 Ready Pod가 0개인 상태가 FAILOVER_GRACE_S 이상 이어지면
        빈 풀 GPU에 확장 Pod를 띄운다(단일 전용 노드 모델의 단일 장애점 완화). 확장이 이미 있으면 건너뛴다."""
        apps = client.AppsV1Api()
        down_since: dict[str, float] = {}
        while FAILOVER_ENABLED:
            try:
                for unit in SERVING_UNITS:
                    dep = apps.read_namespaced_deployment(unit, NS_SERVING)
                    want = dep.spec.replicas or 0
                    ready = dep.status.ready_replicas or 0
                    exp = self.core.list_namespaced_pod(NS_SERVING, label_selector=f"app={unit}-x").items
                    if want == 0 or ready > 0 or exp:
                        down_since.pop(unit, None)
                        continue
                    t = down_since.setdefault(unit, now())
                    if now() - t < FAILOVER_GRACE_S:
                        continue
                    async with self.lock:
                        free = [sl for sl in self.slots() if sl["ready"] and sl["state"] == "free"]
                        if not free:
                            print(f"[rental] FAILOVER {unit}: down {now()-t:.0f}s but no free pool GPU", flush=True)
                            continue
                        r = self.expand(unit, free[0]["node"])
                    M_EVENTS.labels("failover").inc()
                    print(f"[rental] FAILOVER {unit}: down {now()-t:.0f}s -> expansion {r}", flush=True)
                    down_since.pop(unit, None)
            except Exception as e:  # noqa: BLE001
                print(f"[rental] failover loop error: {e}", flush=True)
            await asyncio.sleep(5)

    # ---------------------------------------------------------------- 탄력 배치
    def unit_views(self, state: dict, slots: list[dict], t: float) -> dict[str, UnitView]:
        """게이트웨이 /admin/state + 슬롯 → 묶음별 관측값. 사용률 = 처리 중 토큰 / 토큰 예산(없으면 건수 / 건수 한도)."""
        views = {}
        for unit, models in UNIT_MODELS.items():
            util, queue = unit_load(state, models)
            mine = [s for s in slots if s["state"] == "expansion" and s["labels"].get("app") == f"{unit}-x"]
            v = UnitView(util, queue,
                         [t - s["ready_since"] for s in mine if s["pod_ready"]], sum(1 for s in mine if not s["pod_ready"]))
            views[unit] = v
            M_UTIL.labels(unit).set(util)
            M_EXTRAS.labels(unit, "ready").set(len(v.ready_ages))
            M_EXTRAS.labels(unit, "starting").set(v.starting)
        return views

    async def autoscale_loop(self):
        """5초마다 관측 → autoscale.Policy 판단 → 확장(expand)·회수(reclaim)·넘겨주기(steal) 실행."""
        pol = Policy(Config(**AUTOSCALE_CONFIG), t0=now())
        busy: set[str] = set()      # 회수 중인 풀 노드

        def oldest(unit: str, slots: list[dict]) -> dict | None:
            mine = [s for s in slots if s["state"] == "expansion" and s["labels"].get("app") == f"{unit}-x" and s["pod_ready"]
                    and s["node"] not in busy and now() - s["ready_since"] >= pol.cfg.min_life_s]
            return min(mine, key=lambda s: s["ready_since"]) if mine else None

        async def take_back(slot: dict, then_unit: str | None):
            busy.add(slot["node"])
            try:
                r = await self.reclaim_expansion(slot)
                print(f"[pool] reclaim {slot['pod']} -> {r}", flush=True)
                if then_unit:
                    async with self.lock:
                        print(f"[pool] expand {then_unit} on {slot['node']} -> {self.expand(then_unit, slot['node'])}", flush=True)
            finally:
                busy.discard(slot["node"])

        while AUTOSCALE_ENABLED:
            try:
                st = (await self.http.get(f"{GATEWAY}/admin/state")).json()
                async with self.lock:
                    slots = [s for s in self.slots() if s["ready"]]
                    views = self.unit_views(st, slots, now())
                    free = [s for s in slots if s["state"] == "free" and s["node"] not in busy]
                    for act in pol.decide(now(), views, len(free)):
                        M_ACT.labels(act[0], act[-1]).inc()
                        print(f"[pool] decide {act} util={ {u: round(v.util, 2) for u, v in views.items()} }", flush=True)
                        if act[0] == "expand":
                            self.expand(act[1], free.pop(0)["node"])
                        elif act[0] in ("reclaim", "steal"):
                            slot = oldest(act[1], slots)
                            if slot:
                                asyncio.create_task(take_back(slot, act[2] if act[0] == "steal" else None))
            except Exception as e:  # noqa: BLE001 — 게이트웨이·API 일시 오류는 다음 주기에 다시
                print(f"[pool] autoscale error: {e}", flush=True)
            await asyncio.sleep(5)

    # ---------------------------------------------------------------- 추론 확장
    def expand(self, model_unit: str, node: str) -> dict:
        """대여 풀의 빈 GPU에 추론 확장 Pod를 띄운다. serving-<unit> Deployment의 Pod 템플릿을 복제한다."""
        apps = client.AppsV1Api()
        dep = apps.read_namespaced_deployment(model_unit, NS_SERVING)
        tpl = dep.spec.template
        name = f"{model_unit}-x-{node}"[:63]
        labels = {"app": f"{model_unit}-x", "pknu.ac.kr/component": "serving", "pknu.ac.kr/expansion": "true"}
        spec = tpl.spec
        spec.node_selector = {"kubernetes.io/hostname": node, "pknu.ac.kr/gpu-role": "pool"}
        pod = client.V1Pod(metadata=client.V1ObjectMeta(name=name, namespace=NS_SERVING, labels=labels), spec=spec)
        self.core.create_namespaced_pod(NS_SERVING, pod)
        M_EVENTS.labels("expand").inc()
        return {"pod": name, "node": node, "unit": model_unit}


def auth(req: Request) -> dict | None:
    h = req.headers.get("authorization", "")
    key = h[7:] if h.lower().startswith("bearer ") else ""
    return KEYS.get(hashlib.sha256(key.encode()).hexdigest())


def build_app(m: Manager) -> FastAPI:
    app = FastAPI()

    @app.on_event("startup")
    async def _bg():
        asyncio.create_task(m.refresh())
        asyncio.create_task(m.failover_loop())
        asyncio.create_task(m.autoscale_loop())

    @app.get("/healthz")
    async def healthz():
        return PlainTextResponse("ok")

    @app.get("/metrics")
    async def metrics():
        return PlainTextResponse(generate_latest(REG).decode(), media_type="text/plain; version=0.0.4")

    @app.get("/admin/slots")
    async def slots(req: Request):
        user = auth(req)
        if not user or user["tier"] != "staff":
            return JSONResponse({"error": "staff only"}, 403)
        return {"slots": [{k: v for k, v in s.items() if k not in ("labels", "annotations")} for s in m.slots()]}

    @app.post("/admin/expand")
    async def expand(req: Request):
        user = auth(req)
        if not user or user["tier"] != "staff":
            return JSONResponse({"error": "staff only"}, 403)
        body = await req.json()
        free = [s for s in m.slots() if s["ready"] and s["state"] == "free"]
        if not free:
            return JSONResponse({"error": "no free pool GPU"}, 409)
        return m.expand(body.get("unit", "serving-gptoss"), body.get("node") or free[0]["node"])

    @app.post("/admin/reclaim")
    async def reclaim(req: Request):
        """확장 Pod 회수: {"node": "<풀 노드>"} → drain 후 삭제"""
        user = auth(req)
        if not user or user["tier"] != "staff":
            return JSONResponse({"error": "staff only"}, 403)
        body = await req.json()
        exp = [s for s in m.slots() if s["state"] == "expansion" and s["node"] == body.get("node")]
        if not exp:
            return JSONResponse({"error": "no expansion pod on that node"}, 404)
        async with m.lock:
            return await m.reclaim_expansion(exp[0])

    return app


if __name__ == "__main__":
    uvicorn.run(build_app(Manager()), host="0.0.0.0", port=int(os.environ.get("PORT", "8090")), log_level="warning")
