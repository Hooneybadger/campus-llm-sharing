#!/usr/bin/env python3
"""부경대학교 컴퓨터인공지능공학부 AI 실습 플랫폼 — 웹 포털(실습과 대화 기록, 모델 상태, 사용량, API 안내, 운영 현황).

브라우저는 이 서버 하나만 부른다. 포털이 게이트웨이·확장 풀 관리자·Prometheus·Alertmanager를 대신 불러
  1) 출처가 하나라 교차 출처 설정이 필요 없고
  2) 운영자 토큰(GW_ADMIN_TOKEN)이 브라우저로 나가지 않으며
  3) 화면에 필요한 모양으로 합쳐 보낸다.
인증은 게이트웨이와 같은 API 키(Bearer). 운영 화면은 게이트웨이가 staff 등급으로 확인한 키만 연다.
"""
import asyncio
import json
import math
import os
import re
import sqlite3
import threading
import time
from pathlib import Path

import httpx
import uvicorn
import yaml
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette.background import BackgroundTask

GATEWAY = os.environ.get("GATEWAY_URL", "http://llm-gateway.llm-system.svc:8080")
RENTAL = os.environ.get("RENTAL_URL", "http://rental-manager.llm-system.svc:8090")
PROM = os.environ.get("PROM_URL", "http://kps-kube-prometheus-stack-prometheus.monitoring.svc:9090")
ALERTMANAGER = os.environ.get("ALERTMANAGER_URL", "http://kps-kube-prometheus-stack-alertmanager.monitoring.svc:9093")
GW_ADMIN_TOKEN = os.environ.get("GW_ADMIN_TOKEN", "")
PUBLIC = {   # 사용자에게 보여 줄 외부 주소(API 예제 코드, Grafana 링크)
    "api_base": os.environ.get("PUBLIC_API_BASE", "http://localhost:30080/v1"),
    "grafana": os.environ.get("PUBLIC_GRAFANA_URL", "http://localhost:30030"),
}
HERE = Path(__file__).resolve().parent
STATIC = Path(os.environ.get("PORTAL_STATIC", HERE / "static"))
NOTES = yaml.safe_load(Path(os.environ.get("PORTAL_MODELS", HERE / "models.yaml")).read_text())["models"]
QUIET_ALERTS = {"Watchdog", "InfoInhibitor"}   # 경보 경로가 살아 있음을 알리는 상시 알림

CHAT_DB = Path(os.environ.get("PORTAL_DATA", HERE / "data")) / "chats.sqlite3"


class ChatStore:
    """사용자별 대화 기록(실습 화면의 '최근 대화'). 다른 PC에서 로그인해도 이어 볼 수 있게 서버에 둔다.
    사용자 구분은 게이트웨이가 API 키로 확인한 user_id. 한 사용자의 대화는 최근 MAX_CHATS개만 남긴다."""
    MAX_CHATS, MAX_MESSAGES, MAX_BYTES = 50, 200, 512 * 1024
    ROLES = {"user", "assistant", "error"}
    KEEP = {"role", "content", "model", "reasoning", "meta", "retry"}

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.lock = threading.Lock()
        self.db.execute("CREATE TABLE IF NOT EXISTS chats (user TEXT, id TEXT, title TEXT, model TEXT, system TEXT, "
                        "messages TEXT, updated REAL, PRIMARY KEY (user, id))")
        self.db.commit()

    @classmethod
    def clean(cls, body: dict) -> dict:
        """브라우저가 보낸 대화를 저장할 모양으로 정리한다. 형식이 맞지 않으면 ValueError."""
        msgs = body.get("messages")
        if not isinstance(msgs, list) or not msgs or len(msgs) > cls.MAX_MESSAGES:
            raise ValueError("messages must be a non-empty list of at most %d items" % cls.MAX_MESSAGES)
        out = []
        for m in msgs:
            if not isinstance(m, dict) or m.get("role") not in cls.ROLES or not isinstance(m.get("content", ""), str):
                raise ValueError("each message needs role (user, assistant, error) and text content")
            out.append({k: v for k, v in m.items() if k in cls.KEEP})
        first = next((m["content"] for m in out if m["role"] == "user"), "")
        row = {"title": " ".join(first.split())[:60] or "새 대화", "model": str(body.get("model") or "")[:80],
               "system": str(body.get("system") or "")[:4000], "messages": json.dumps(out, ensure_ascii=False)}
        if len(row["messages"].encode()) > cls.MAX_BYTES:
            raise ValueError("chat is too large to save")
        return row

    def save(self, user: str, cid: str, body: dict) -> dict:
        row, now = self.clean(body), time.time()
        with self.lock:
            self.db.execute("INSERT OR REPLACE INTO chats VALUES (?,?,?,?,?,?,?)",
                            (user, cid, row["title"], row["model"], row["system"], row["messages"], now))
            self.db.execute("DELETE FROM chats WHERE user=? AND id NOT IN "
                            "(SELECT id FROM chats WHERE user=? ORDER BY updated DESC LIMIT ?)", (user, user, self.MAX_CHATS))
            self.db.commit()
        return {"id": cid, "title": row["title"], "updated": now}

    def list(self, user: str) -> list[dict]:
        with self.lock:
            rows = self.db.execute("SELECT id, title, model, updated FROM chats WHERE user=? ORDER BY updated DESC",
                                   (user,)).fetchall()
        return [{"id": i, "title": t, "model": m, "updated": u} for i, t, m, u in rows]

    def get(self, user: str, cid: str) -> dict | None:
        with self.lock:
            r = self.db.execute("SELECT id, title, model, system, messages, updated FROM chats WHERE user=? AND id=?",
                                (user, cid)).fetchone()
        if not r:
            return None
        return {"id": r[0], "title": r[1], "model": r[2], "system": r[3], "messages": json.loads(r[4]), "updated": r[5]}

    def delete(self, user: str, cid: str) -> bool:
        with self.lock:
            n = self.db.execute("DELETE FROM chats WHERE user=? AND id=?", (user, cid)).rowcount
            self.db.commit()
        return n > 0


CHAT_ID = re.compile(r"^[A-Za-z0-9-]{8,64}$")

SECURITY_HEADERS = {
    "Content-Security-Policy": "default-src 'self'; script-src 'self'; connect-src 'self'; img-src 'self' data:; "
                               "style-src 'self' https://cdn.jsdelivr.net https://fonts.googleapis.com; "
                               "font-src https://cdn.jsdelivr.net https://fonts.gstatic.com; frame-ancestors 'none'",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "same-origin",
}

http = httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=3.0),
                         limits=httpx.Limits(max_connections=500, max_keepalive_connections=100))


def auth_header(req: Request) -> dict:
    return {"authorization": req.headers.get("authorization", "")}


def passthrough(r: httpx.Response) -> JSONResponse:
    try:
        body = r.json()
    except ValueError:
        body = {"error": {"message": r.text[:300]}}
    return JSONResponse(body, status_code=r.status_code)


async def gw_me(req: Request) -> tuple[int, dict]:
    r = await http.get(f"{GATEWAY}/v1/me", headers=auth_header(req))
    return r.status_code, r.json()


async def prom(q: str) -> list[dict]:
    r = await http.get(f"{PROM}/api/v1/query", params={"query": q})
    r.raise_for_status()
    return r.json()["data"]["result"]


def num(v: str) -> float | None:
    x = float(v)
    return None if math.isnan(x) or math.isinf(x) else x


async def model_board() -> list[dict]:
    """모델별 상태: 게이트웨이 라우트(문맥 길이·가용) + 입장 제어 상태(처리 중·대기·토큰 예산) + 쓰임새 안내."""
    models, state = await asyncio.gather(http.get(f"{GATEWAY}/v1/models"), http.get(f"{GATEWAY}/admin/state"))
    st = state.json()["models"]
    rows = []
    for m in models.json()["data"]:
        s = st.get(m["id"], {})
        backends = s.get("backends", {}).values()
        note = NOTES.get(m["id"], {})
        rows.append({
            "id": m["id"], "title": note.get("title", m["id"]), "use": note.get("use", ""), "caution": note.get("caution"),
            "max_model_len": m["max_model_len"], "available": m["available"],
            "inflight": s.get("inflight", 0), "queue": s.get("queue", 0), "queue_max": s.get("queue_max"),
            "max_inflight": s.get("max_inflight"), "inflight_tokens": s.get("inflight_tokens", 0),
            "token_budget": s.get("token_budget"),
            "backends_total": len(backends), "backends_healthy": sum(1 for b in backends if b["healthy"] and not b["draining"]),
        })
    order = list(NOTES)
    rows.sort(key=lambda x: order.index(x["id"]) if x["id"] in order else len(order))
    return rows


async def section(coro):
    """운영 화면은 영역마다 따로 실패할 수 있게 한다: 한 출처가 죽어도 나머지는 보인다."""
    try:
        return {"ok": True, "data": await coro}
    except Exception as e:   # noqa: BLE001 — 화면에 원인을 그대로 보여 준다
        return {"ok": False, "error": f"{type(e).__name__}: {str(e)[:200]}"}


async def ops_traffic() -> dict:
    by_code, p95 = await asyncio.gather(
        prom("sum by (model, code) (rate(gw_requests_total[5m]))"),
        prom("histogram_quantile(0.95, sum by (le, model) (rate(gw_ttft_seconds_bucket[5m])))"))
    out: dict[str, dict] = {}
    for r in by_code:
        m = out.setdefault(r["metric"].get("model", "-"), {"rps": 0.0, "codes": {}, "ttft_p95_s": None})
        v = num(r["value"][1]) or 0.0
        m["rps"] += v
        m["codes"][r["metric"].get("code", "?")] = v
    for r in p95:
        out.setdefault(r["metric"].get("model", "-"), {"rps": 0.0, "codes": {}, "ttft_p95_s": None})["ttft_p95_s"] = num(r["value"][1])
    return out


async def ops_gpus() -> list[dict]:
    names = ["DCGM_FI_DEV_FB_USED", "DCGM_FI_DEV_FB_FREE", "DCGM_FI_DEV_FB_RESERVED", "DCGM_FI_DEV_GPU_UTIL",
             "DCGM_FI_DEV_GPU_TEMP", "DCGM_FI_DEV_POWER_USAGE"]
    res = await asyncio.gather(*(prom(n) for n in names))
    gpus: dict[str, dict] = {}
    for name, rows in zip(names, res):
        for r in rows:
            lab = r["metric"]
            g = gpus.setdefault(lab["UUID"], {"node": lab.get("Hostname") or lab.get("hostname") or lab.get("node"),
                                              "model": lab.get("modelName"), "pod": lab.get("exported_pod"),
                                              "namespace": lab.get("exported_namespace")})
            g[name] = num(r["value"][1])
    out = []
    for g in gpus.values():
        used, free, rsv = (g.get(k) or 0 for k in ("DCGM_FI_DEV_FB_USED", "DCGM_FI_DEV_FB_FREE", "DCGM_FI_DEV_FB_RESERVED"))
        out.append({"node": g["node"], "model": g["model"], "pod": g["pod"], "namespace": g["namespace"],
                    "mem_used_mib": used, "mem_total_mib": used + free + rsv, "util_pct": g.get("DCGM_FI_DEV_GPU_UTIL"),
                    "temp_c": g.get("DCGM_FI_DEV_GPU_TEMP"), "power_w": g.get("DCGM_FI_DEV_POWER_USAGE")})
    return sorted(out, key=lambda x: x["node"] or "")


async def ops_alerts() -> list[dict]:
    r = await http.get(f"{ALERTMANAGER}/api/v2/alerts", params={"active": "true", "silenced": "false", "inhibited": "false"})
    r.raise_for_status()
    out = []
    for a in r.json():
        lab, ann = a.get("labels", {}), a.get("annotations", {})
        if lab.get("alertname") in QUIET_ALERTS:
            continue
        out.append({"name": lab.get("alertname"), "severity": lab.get("severity"),
                    "target": lab.get("model") or lab.get("pod") or lab.get("node") or lab.get("namespace"),
                    "summary": ann.get("summary") or ann.get("description") or "", "starts_at": a.get("startsAt")})
    sev = {"critical": 0, "warning": 1}
    return sorted(out, key=lambda x: (sev.get(x["severity"], 2), x["starts_at"] or ""))


async def ops_users() -> dict:
    r = await http.get(f"{GATEWAY}/admin/usage", params={"limit": "15"}, headers={"x-admin-token": GW_ADMIN_TOKEN})
    r.raise_for_status()
    return r.json()


async def ops_slots(req: Request) -> list[dict]:
    r = await http.get(f"{RENTAL}/admin/slots", headers=auth_header(req))
    r.raise_for_status()
    return r.json()["slots"]


def build_app(chats: ChatStore | None = None) -> FastAPI:
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    chats = chats or ChatStore(CHAT_DB)

    async def chat_user(req: Request, cid: str | None = None) -> tuple[str | None, JSONResponse | None]:
        """API 키 주인의 user_id. 키가 틀리거나 대화 ID 형식이 맞지 않으면 바로 돌려줄 응답을 함께 준다."""
        if cid is not None and not CHAT_ID.match(cid):
            return None, JSONResponse({"error": {"message": "invalid chat id"}}, status_code=400)
        code, me = await gw_me(req)
        if code != 200:
            return None, JSONResponse({"error": {"message": "invalid API key"}}, status_code=401)
        return me["user_id"], None

    @app.middleware("http")
    async def headers(req: Request, call_next):
        resp = await call_next(req)
        resp.headers.update(SECURITY_HEADERS)
        if req.url.path.startswith("/api/"):
            resp.headers["Cache-Control"] = "no-store"
        return resp

    @app.get("/healthz")
    async def healthz():
        return {"ok": True}

    @app.get("/")
    async def index():
        return FileResponse(STATIC / "index.html", headers={"Cache-Control": "no-cache"})

    @app.get("/api/config")
    async def cfg():
        return PUBLIC

    @app.get("/api/me")
    async def me(req: Request):
        return passthrough(await http.get(f"{GATEWAY}/v1/me", headers=auth_header(req)))

    @app.get("/api/models")
    async def models(req: Request):
        code, _ = await gw_me(req)
        if code != 200:
            return JSONResponse({"error": {"message": "invalid API key"}}, status_code=401)
        return {"models": await model_board(), "at": time.time()}

    @app.post("/api/chat")
    async def chat(req: Request):
        """게이트웨이 응답을 그대로 흘려보낸다(상태 코드·Retry-After·SSE 포함). 응답 정책은 게이트웨이 한곳에 둔다."""
        upstream = await http.send(http.build_request(
            "POST", f"{GATEWAY}/v1/chat/completions", content=await req.body(),
            headers={**auth_header(req), "content-type": "application/json"},
            timeout=httpx.Timeout(600.0, connect=3.0)), stream=True)
        keep = {k: v for k, v in upstream.headers.items() if k.lower() in ("retry-after",)}
        return StreamingResponse(upstream.aiter_raw(), status_code=upstream.status_code, headers=keep,
                                 media_type=upstream.headers.get("content-type", "application/json"),
                                 background=BackgroundTask(upstream.aclose))

    @app.get("/api/chats")
    async def chat_list(req: Request):
        uid, bad = await chat_user(req)
        return bad or {"chats": chats.list(uid)}

    @app.get("/api/chats/{cid}")
    async def chat_get(req: Request, cid: str):
        uid, bad = await chat_user(req, cid)
        if bad:
            return bad
        c = chats.get(uid, cid)
        return c or JSONResponse({"error": {"message": "chat not found"}}, status_code=404)

    @app.put("/api/chats/{cid}")
    async def chat_put(req: Request, cid: str):
        uid, bad = await chat_user(req, cid)
        if bad:
            return bad
        try:
            return chats.save(uid, cid, await req.json())
        except (ValueError, json.JSONDecodeError) as e:
            return JSONResponse({"error": {"message": str(e)}}, status_code=400)

    @app.delete("/api/chats/{cid}")
    async def chat_delete(req: Request, cid: str):
        uid, bad = await chat_user(req, cid)
        if bad:
            return bad
        return {"deleted": chats.delete(uid, cid)}

    @app.get("/api/ops")
    async def ops(req: Request):
        code, me = await gw_me(req)
        if code != 200:
            return JSONResponse({"error": {"message": "invalid API key"}}, status_code=401)
        if me.get("tier") != "staff":
            return JSONResponse({"error": {"message": "운영자 등급 키만 볼 수 있습니다."}}, status_code=403)
        keys = ["models", "traffic", "gpus", "alerts", "users", "slots"]
        vals = await asyncio.gather(section(model_board()), section(ops_traffic()), section(ops_gpus()),
                                    section(ops_alerts()), section(ops_users()), section(ops_slots(req)))
        return {"at": time.time(), **dict(zip(keys, vals))}

    app.mount("/static", StaticFiles(directory=STATIC), name="static")
    # index.html은 같은 폴더 기준 상대 경로(app.css, app.js)로 부른다: 루트에서도 같은 파일을 내준다(API 경로가 먼저 잡힌다)
    app.mount("/", StaticFiles(directory=STATIC), name="root")
    return app


if __name__ == "__main__":
    uvicorn.run(build_app(), host="0.0.0.0", port=int(os.environ.get("PORT", "8088")), log_level="warning")
