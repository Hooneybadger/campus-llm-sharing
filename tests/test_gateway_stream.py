"""스트리밍 경계 테스트 — 가짜 업스트림으로 정상 종료, 단절, [DONE] 없는 종료, usage 청크 처리를 재현한다.
모델 품질이 아니라 게이트웨이가 클라이언트에 무엇을 보내고 사용량을 어떻게 기록하는지만 검사한다.
"""
import asyncio
import hashlib
import json
import sys
from pathlib import Path

import httpx
import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "gateway"))
from app import Gateway, build_app  # noqa: E402

KEY = "sk-test"
HDR = {"Authorization": f"Bearer {KEY}"}
TOKEN = b'data: {"id":"c","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"tok"},"finish_reason":null}]}\n\n'
ROLE = b'data: {"id":"c","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}\n\n'
FINISH = b'data: {"id":"c","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\n'
USAGE = b'data: {"id":"c","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":11,"completion_tokens":3,"total_tokens":14}}\n\n'
DONE = b"data: [DONE]\n\n"


class Upstream(httpx.AsyncByteStream):
    def __init__(self, parts, fail=False):
        self.parts, self.fail = parts, fail

    async def __aiter__(self):
        for p in self.parts:
            yield p
        if self.fail:
            raise httpx.RemoteProtocolError("peer closed connection without sending complete message body")

    async def aclose(self):
        pass


@pytest.fixture
def gw(tmp_path):
    (tmp_path / "routes.yaml").write_text(yaml.safe_dump({"models": {"m": {
        "backends": ["http://up:1"], "kv_tokens_per_backend": 10000, "kv_fill": 0.9, "queue_max": 4, "queue_timeout_s": 1,
        "max_model_len": 4096, "max_seqs_per_backend": 8}}}))
    (tmp_path / "keys.json").write_text(json.dumps({
        "keys": {hashlib.sha256(KEY.encode()).hexdigest(): {"user_id": "u1", "tier": "staff", "group": "g"}},
        "tiers": {"staff": {"rpm": 1000, "max_concurrent": 10, "max_tokens_cap": 2048}}}))
    return Gateway(tmp_path / "routes.yaml", tmp_path / "keys.json", None)


def call(gw, parts, fail=False, **body):
    seen = {}

    def upstream(req):
        seen["body"] = json.loads(req.content)
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Upstream(parts, fail))

    async def go():
        gw.client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
        app = build_app(gw)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as c:
            r = await c.post("/v1/chat/completions", headers=HDR, json={
                "model": "m", "messages": [{"role": "user", "content": "hi"}], "max_tokens": 16, "stream": True, **body})
            return r
    return asyncio.run(go()), seen["body"]


def last(gw):
    return gw.usage.get("u1")["recent"][0]


def test_normal_stream_hides_usage_chunk_from_clients_that_did_not_ask(gw):
    r, up = call(gw, [ROLE, TOKEN, TOKEN, FINISH, USAGE, DONE])
    assert up["stream_options"]["include_usage"] is True          # 게이트웨이는 vLLM에 요청한다
    assert b'"choices":[]' not in r.content and r.content.endswith(DONE)
    rec = last(gw)
    assert (rec["reason"], rec["completion_tokens"], rec["usage"]) == ("ok", 3, "reported")


def test_usage_chunk_is_forwarded_when_client_asked(gw):
    r, _ = call(gw, [ROLE, TOKEN, FINISH, USAGE, DONE], stream_options={"include_usage": True})
    assert b'"completion_tokens":3' in r.content


def test_midstream_disconnect_sends_error_event_and_estimates_usage(gw):
    r, _ = call(gw, [ROLE, TOKEN, TOKEN, TOKEN], fail=True)
    assert r.status_code == 200
    tail = r.content.strip().split(b"\n\n")[-1]
    assert json.loads(tail[6:])["error"]["code"] == "backend_stream_broken"
    assert DONE not in r.content                                  # 끊긴 응답에 정상 종료 표시를 붙이지 않는다
    rec = last(gw)
    assert (rec["reason"], rec["completion_tokens"], rec["usage"]) == ("stream_broken", 3, "estimated")
    assert gw.routes["m"].inflight == 0 and gw.routes["m"].inflight_tokens == 0


def test_clean_close_without_done_is_treated_as_truncated(gw):
    r, _ = call(gw, [ROLE, TOKEN, TOKEN])                         # 오류 없이 닫혀도 [DONE]이 없다
    assert b"backend_stream_broken" in r.content
    assert last(gw)["reason"] == "stream_broken"


def test_upstream_error_event_is_not_recorded_as_ok(gw):
    # 실제 vLLM 0.32.0이 EngineCore 사망 때 보내는 순서: 오류 이벤트 다음에 [DONE]을 보내고 정상 종료한다
    upstream_err = b'data: {"error": {"message": "EngineCore encountered an issue.", "type": "InternalServerError", "param": null, "code": 500}}\n\n'
    r, _ = call(gw, [ROLE, TOKEN, TOKEN, upstream_err, DONE])
    assert b"backend_stream_broken" in r.content and DONE not in r.content and b"EngineCore" not in r.content
    assert r.content.count(b'"delta":{"content":"tok"}') == 2     # 오류 전까지 받은 토큰은 그대로 전달한다
    rec = last(gw)
    assert (rec["reason"], rec["completion_tokens"], rec["usage"]) == ("stream_broken", 2, "estimated")


def test_split_event_across_chunks_is_counted_once(gw):
    half = len(TOKEN) // 2
    r, _ = call(gw, [ROLE, TOKEN[:half], TOKEN[half:], TOKEN], fail=True)
    assert last(gw)["completion_tokens"] == 2


def test_nonstream_disconnect_returns_502_not_sse(gw):
    r, _ = call(gw, [b'{"id":"c"'], fail=True, stream=False)
    assert r.status_code == 502
    assert r.json()["error"]["code"] == "backend_stream_broken"


def test_prompt_larger_than_backend_budget_is_rejected_before_queue(gw):
    gw.routes["m"].kv_tokens = 100                                # 예약 상한 90토큰 < 프롬프트
    gw.routes["m"].tok = type("T", (), {"encode": lambda self, t: type("E", (), {"ids": list(range(200))})()})()
    r, _ = None, None

    async def go():
        gw.client = httpx.AsyncClient(transport=httpx.MockTransport(lambda q: httpx.Response(500)))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=build_app(gw)), base_url="http://gw") as c:
            return await c.post("/v1/chat/completions", headers=HDR, json={"model": "m", "messages": [{"role": "user", "content": "x"}]})
    r = asyncio.run(go())
    assert r.status_code == 400 and r.json()["error"]["code"] == "context_length"
