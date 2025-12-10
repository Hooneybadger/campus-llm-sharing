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
