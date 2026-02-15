#!/usr/bin/env python3
"""배포 스모크 테스트: 게이트웨이에 등록된 모든 모델에 짧은 채팅·도구 호출을 보내 확인한다 (TS-005/008/010 재발 방지).
사용: python scripts/smoke.py [--gateway http://localhost:30080] [--key <staff key>]
종료 코드: 실패한 모델 수"""
import argparse, json, os, sys
from pathlib import Path
import httpx

ap = argparse.ArgumentParser()
ap.add_argument("--gateway", default="http://localhost:30080")
ap.add_argument("--key", default="")
ap.add_argument("--models", nargs="*")
a = ap.parse_args()
key = a.key or os.environ.get("GATEWAY_API_KEY", "")
if not key:
    ap.error("set GATEWAY_API_KEY or pass --key")
H = {"Authorization": f"Bearer {key}"}
models = a.models or [m["id"] for m in httpx.get(a.gateway + "/v1/models", headers=H, timeout=10).json()["data"]]
tool = [{"type": "function", "function": {"name": "get_weather", "description": "Get weather for a city",
         "parameters": {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]}}}]
fails = 0
for m in models:
    res = {"model": m}
    try:
        r = httpx.post(a.gateway + "/v1/chat/completions", headers=H, timeout=180, json={
            "model": m, "max_tokens": 1024, "temperature": 0,
            "messages": [{"role": "user", "content": "대한민국의 수도는 어디야? 한 문장으로 답해."}]})
        msg = r.json()["choices"][0]["message"] if r.status_code == 200 else {}
        # 의미 검사: 깨진 출력(TS-016)도 "비어 있지 않음"은 통과하므로 기대 단어를 확인한다
        res["chat"] = r.status_code == 200 and "서울" in (msg.get("content") or "")
        res["chat_sample"] = (msg.get("content") or "")[:60]
        r = httpx.post(a.gateway + "/v1/chat/completions", headers=H, timeout=180, json={
            "model": m, "max_tokens": 512, "tools": tool, "tool_choice": "auto",
            "messages": [{"role": "user", "content": "부산 날씨 알려줘."}]})
        calls = (r.json()["choices"][0]["message"].get("tool_calls") or []) if r.status_code == 200 else []
        res["tool"] = bool(calls) and calls[0]["function"]["name"] == "get_weather"
        # 스트리밍 도구 호출(TS-019: 비스트리밍은 정상인데 스트리밍에서 사라지는 경우)
        names = ""
        with httpx.stream("POST", a.gateway + "/v1/chat/completions", headers=H, timeout=180, json={
                "model": m, "max_tokens": 512, "tools": tool, "tool_choice": "auto", "stream": True,
                "messages": [{"role": "user", "content": "부산 날씨 알려줘."}]}) as st:
            for line in st.iter_lines():
                if line.startswith("data: {"):
                    for ch in json.loads(line[6:]).get("choices") or []:
                        for tc in (ch.get("delta") or {}).get("tool_calls") or []:
                            names += (tc.get("function") or {}).get("name") or ""
        res["tool_stream"] = "get_weather" in names
    except Exception as e:  # noqa: BLE001
        res["error"] = repr(e)[:200]
    ok = res.get("chat") and res.get("tool") and res.get("tool_stream")
    fails += 0 if ok else 1
    print(("PASS " if ok else "FAIL ") + json.dumps(res, ensure_ascii=False), flush=True)
sys.exit(fails)
