#!/usr/bin/env python3
"""Grafana 대시보드 생성기 — 대시보드를 코드로 관리한다(P3-1 표준화). 출력: monitoring/dashboards/llm-serving.json
행 구성: 서비스 개요 → 지연 → 부하·대기열 → KV·처리량 → GPU(DCGM) → 모델 프로세스와 추론 풀
"""
import json
from pathlib import Path

DS = {"type": "prometheus", "uid": "prometheus"}
panels, y = [], 0
pid = 0


def row(title):
    global y, pid
    pid += 1
    panels.append({"type": "row", "title": title, "id": pid, "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}, "collapsed": False})
    y += 1


def ts(title, exprs, unit="short", w=8, h=8, x=0, stack=False):
    global pid
    pid += 1
    panels.append({
        "type": "timeseries", "title": title, "id": pid, "datasource": DS,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {"defaults": {"unit": unit, "custom": {"lineWidth": 1, "fillOpacity": 10 if stack else 0,
                                                              "stacking": {"mode": "normal" if stack else "none"}}}, "overrides": []},
        "options": {"legend": {"displayMode": "table", "placement": "bottom", "calcs": ["lastNotNull", "max"]},
                    "tooltip": {"mode": "multi"}},
        "targets": [{"datasource": DS, "expr": e, "legendFormat": l, "refId": chr(65 + i)} for i, (e, l) in enumerate(exprs)],
    })


def stat(title, expr, unit="short", w=4, x=0, thresholds=None):
    global pid
    pid += 1
    panels.append({
        "type": "stat", "title": title, "id": pid, "datasource": DS, "gridPos": {"h": 4, "w": w, "x": x, "y": y},
        "fieldConfig": {"defaults": {"unit": unit, "thresholds": {"mode": "absolute", "steps": thresholds or [{"color": "green", "value": None}]}}},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background"},
        "targets": [{"datasource": DS, "expr": expr, "refId": "A"}],
    })


row("서비스 개요")
stat("요청/초", 'sum(rate(gw_requests_total[1m]))', "reqps", x=0)
stat("성공률", 'sum(rate(gw_requests_total{code="200",reason="ok"}[5m])) / sum(rate(gw_requests_total[5m]))', "percentunit", x=4,
     thresholds=[{"color": "red", "value": None}, {"color": "orange", "value": 0.9}, {"color": "green", "value": 0.98}])
stat("과부하 거절(503)/초", 'sum(rate(gw_requests_total{code="503"}[1m]))', "reqps", x=8,
     thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 0.1}, {"color": "red", "value": 1}])
stat("사용자 한도(429)/초", 'sum(rate(gw_requests_total{code="429"}[1m]))', "reqps", x=12)
stat("정상 백엔드", 'sum(gw_backend_up)', x=16, thresholds=[{"color": "red", "value": None}, {"color": "green", "value": 1}])
stat("GPU 사용 중 노드", 'count(DCGM_FI_DEV_GPU_UTIL > 5)', x=20)
y += 4
ts("응답 코드별 요청/초 (모델)", [('sum by (model, code, reason) (rate(gw_requests_total[1m]))', "{{model}} {{code}} {{reason}}")], "reqps", w=24, stack=True)
y += 8

row("지연")
ts("게이트웨이 TTFT P95 (대기열 포함)", [('histogram_quantile(0.95, sum by (model, le) (rate(gw_ttft_seconds_bucket[1m])))', "{{model}}")], "s", x=0)
ts("vLLM TTFT P95 (엔진 내부)", [('histogram_quantile(0.95, sum by (model_name, le) (rate(vllm:time_to_first_token_seconds_bucket[1m])))', "{{model_name}}")], "s", x=8)
ts("토큰 간 지연 ITL P95", [('histogram_quantile(0.95, sum by (model_name, le) (rate(vllm:inter_token_latency_seconds_bucket[1m])))', "{{model_name}}")], "s", x=16)
y += 8
ts("게이트웨이 대기열 대기 P95", [('histogram_quantile(0.95, sum by (model, le) (rate(gw_queue_wait_seconds_bucket[1m])))', "{{model}}")], "s", x=0)
ts("E2E 지연 P95", [('histogram_quantile(0.95, sum by (model_name, le) (rate(vllm:e2e_request_latency_seconds_bucket[1m])))', "{{model_name}}")], "s", x=8)
y += 8

row("부하·대기열")
ts("vLLM 실행 중 / 대기 중", [('sum by (model_name) (vllm:num_requests_running)', "{{model_name}} running"),
                         ('sum by (model_name) (vllm:num_requests_waiting)', "{{model_name}} waiting")], x=0)
ts("게이트웨이 진행 중 / 대기열", [('sum by (model) (gw_inflight)', "{{model}} inflight"), ('sum by (model) (gw_queue_length)', "{{model}} queue")], x=8)
ts("KV 캐시 사용률", [('max by (model_name) (vllm:kv_cache_usage_perc)', "{{model_name}}")], "percentunit", x=16)
y += 8

row("처리량")
ts("생성 토큰/초", [('sum by (model_name) (rate(vllm:generation_tokens_total[1m]))', "{{model_name}}")], "short", x=0, stack=True)
ts("프롬프트 토큰/초", [('sum by (model_name) (rate(vllm:prompt_tokens_total[1m]))', "{{model_name}}")], "short", x=8, stack=True)
ts("선점(preemption)/초", [('sum by (model_name) (rate(vllm:num_preemptions_total[1m]))', "{{model_name}}")], "short", x=16)
y += 8

row("GPU (DCGM, GeForce 지원 필드)")
ts("GPU 메모리 사용 (MiB)", [('DCGM_FI_DEV_FB_USED', "{{node}}")], "decmbytes", x=0)
ts("GPU 사용률 / DRAM 활성도", [('DCGM_FI_DEV_GPU_UTIL', "{{node}} util%"), ('DCGM_FI_PROF_DRAM_ACTIVE * 100', "{{node}} DRAM%")], "percent", x=8)
ts("전력·온도·SM 클럭", [('DCGM_FI_DEV_POWER_USAGE', "{{node}} W"), ('DCGM_FI_DEV_GPU_TEMP', "{{node}} °C"),
                       ('DCGM_FI_DEV_SM_CLOCK / 10', "{{node}} SM MHz/10")], x=16)
y += 8

dash = {"title": "교내 LLM 서빙 — 운영 개요", "uid": "pknu-llm-serving", "timezone": "Asia/Seoul", "refresh": "5s",
        "time": {"from": "now-30m", "to": "now"}, "schemaVersion": 39, "panels": panels, "tags": ["pknu", "llm"]}
out = Path(__file__).with_name("llm-serving.json")
out.write_text(json.dumps(dash, ensure_ascii=False, indent=1))
print(f"{out}: {len(panels)} panels")
