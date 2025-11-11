#!/usr/bin/env python3
"""P2-3 GPU 메모리 예산 계산기 — 같은 GPU에 올릴 모델들의 KV 크기(kv_cache_memory_bytes)를 정한다.

  고정분_i = CUDA 컨텍스트(ctx) + 가중치·non-torch(consumed) + 활성화(act_warm + α·cold_delta) + CUDA graph
  KV 총량  = GPU 가용(usable) − 디스플레이 등 외부 점유(external) − 안전 여유(margin) − Σ 고정분_i
  배분     = 모델별 최소 보장(floor_seqs × max_model_len 토큰) 후, 남는 몫을 수요 비율 × 토큰당 바이트로 나눈다

계수는 P2-2(A) 메모리 분해 실측(models/calibration/memory.csv)에서 읽는다.
α(cold_retention): 콜드 컴파일 때 늘어난 활성화가 기동 후에도 PyTorch 캐시에 남는 비율. 1.0 = 보수적(E4 실측 약 0.4).
사용: python models/budget.py --gpu-gib 31.36 --models qwen3-4b ministral-3b phi4-mini --kv-dtype fp8 \
        --demand qwen3-4b=0.6 ministral-3b=0.2 phi4-mini=0.2 --max-model-len 4096 --max-num-seqs 32 --out models/profiles/small3-v1.yaml
"""
import argparse
import csv
import json
from pathlib import Path

import yaml

PROJ = Path(__file__).resolve().parent.parent
REG = {m["id"]: m for m in yaml.safe_load((PROJ / "models/registry.yaml").read_text())["models"]}
GiB = 2**30


def coefficients(model: str, kv_dtype: str, mns: int) -> dict:
    rows = [r for r in csv.DictReader(open(PROJ / "models/calibration/memory.csv")) if r["model"] == model]

    def pick(cfg, key, try_k):
        v = [float(r[key]) for r in rows if r["cfg"] == cfg and r["try"] == str(try_k) and r[key]]
        return v[0] if v else None
    # 활성화·graph는 max_num_seqs가 작을수록 줄어든다. 측정한 설정 중 보수적인 쪽을 고른다.
    base = "kvfp8" if kv_dtype == "fp8" else ("mns64" if mns <= 64 else "default")
    cold_cfg = base if pick(base, "activation_gib", 1) is not None else "kvfp8"
    act_warm = pick(base, "activation_gib", 2)
    act_cold = max(pick(cold_cfg, "activation_gib", 1) or act_warm, act_warm)
    return {"consumed": pick(base, "consumed_gib", 2), "act_warm": act_warm, "cold_delta": act_cold - act_warm,
            "graph": pick("default", "cudagraph_gib", 2), "basis": base}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gpu-gib", type=float, default=31.36, help="torch가 보는 GPU 전체(5090=31.36, 4090=23.52)")
    ap.add_argument("--external-gib", type=float, default=0.05, help="디스플레이 등 다른 점유")
    ap.add_argument("--ctx-gib", type=float, default=0.50, help="프로세스당 CUDA 컨텍스트(예산 밖, P2-1 E3 실측 0.46)")
    ap.add_argument("--margin-gib", type=float, default=0.6, help="런타임 변동 여유")
    ap.add_argument("--cold-retention", type=float, default=1.0)
    ap.add_argument("--models", nargs="+", required=True)
    ap.add_argument("--kv-dtype", default="auto", help='기본값[,모델=값...] 예: "fp8,ministral-3b=auto" (TS-016: Ministral은 FP8 KV에서 출력 붕괴)')
    ap.add_argument("--max-model-len", type=int, default=4096)
    ap.add_argument("--max-num-seqs", type=int, default=32)
    ap.add_argument("--floor-seqs", type=int, default=4, help="모델별 최소 보장: max_model_len 길이 요청 N개분 KV")
    ap.add_argument("--demand", nargs="*", default=[], help="model=비율 (모델 선택 비율, 남는 KV 배분 기준)")
    ap.add_argument("--out")
    a = ap.parse_args()
    demand = {k: float(v) for k, v in (d.split("=") for d in a.demand)} or {m: 1.0 for m in a.models}
    parts = a.kv_dtype.split(",")
    kvd = {m: parts[0] for m in a.models}
    kvd.update(dict(x.split("=") for x in parts[1:]))
    rows, fixed_total = [], 0.0
    for m in a.models:
        c = coefficients(m, kvd[m], a.max_num_seqs)
        fixed = a.ctx_gib + c["consumed"] + c["act_warm"] + a.cold_retention * c["cold_delta"] + c["graph"]
        bpt = REG[m]["kv_bytes_per_token"] / (2 if kvd[m] == "fp8" else 1)
        rows.append({"model": m, **c, "fixed_gib": fixed, "bytes_per_token": bpt, "kv_dtype": kvd[m]})
        fixed_total += fixed
    kv_total = a.gpu_gib - a.external_gib - a.margin_gib - fixed_total
    floor = {r["model"]: a.floor_seqs * a.max_model_len * r["bytes_per_token"] / GiB for r in rows}
    if kv_total < sum(floor.values()):
        raise SystemExit(f"불가: KV 가용 {kv_total:.2f} GiB < 최소 보장 합 {sum(floor.values()):.2f} GiB "
                         f"(컨텍스트·floor_seqs를 줄이거나 FP8 KV 사용)")
    rest = kv_total - sum(floor.values())
    w = {r["model"]: demand.get(r["model"], 0) * r["bytes_per_token"] for r in rows}
    for r in rows:
        r["kv_gib"] = floor[r["model"]] + rest * w[r["model"]] / sum(w.values())
        r["kv_bytes"] = int(r["kv_gib"] * GiB) // (2 * 1024 * 1024) * (2 * 1024 * 1024)   # 2MiB 정렬
        r["kv_tokens"] = int(r["kv_bytes"] / r["bytes_per_token"])
        r["full_len_seqs"] = round(r["kv_tokens"] / a.max_model_len, 1)
    print(f"GPU {a.gpu_gib} GiB − 외부 {a.external_gib} − 여유 {a.margin_gib} − 고정분 {fixed_total:.2f} = KV {kv_total:.2f} GiB "
          f"(kv_dtype={a.kv_dtype}, α={a.cold_retention})")
    for r in rows:
        print(f"  {r['model']:14s} [{r['kv_dtype']}] 고정 {r['fixed_gib']:.2f} (consumed {r['consumed']:.2f}, act {r['act_warm']:.2f}+{r['cold_delta']:.2f}·α, "
              f"graph {r['graph']:.2f}) | KV {r['kv_gib']:.2f} GiB = {r['kv_tokens']:,} tok ≈ {r['full_len_seqs']}× {a.max_model_len}tok 요청")
    predicted = a.external_gib + fixed_total + sum(r["kv_bytes"] for r in rows) / GiB
    print(f"  예상 GPU 사용 {predicted:.2f} GiB / {a.gpu_gib} (여유 {a.gpu_gib - predicted:.2f})")
    if a.out:
        prof = {"role": "serving-small", "startup": "sequential", "health_port": 8000, "ready_timeout_s": 900,
                "escalate_on_failed": True, "restart": {"max_restarts": 3, "backoff_s": [5, 15, 45]},
                "budget_note": {"tool": "models/budget.py", "kv_dtype": a.kv_dtype, "cold_retention": a.cold_retention,
                                "margin_gib": a.margin_gib, "predicted_gpu_gib": round(predicted, 2), "demand": demand},
                "models": []}
        for i, r in enumerate(rows):
            # kv_cache_memory_bytes를 줘도 기동 시 "여유 메모리 ≥ gpu_memory_utilization × 전체" 검사는 남는다(기본 0.92).
            # 순차 기동에서 두 번째 모델부터 이 검사에 걸리므로 낮은 값을 함께 준다 (TS-015)
            b = {"kv_cache_memory_bytes": r["kv_bytes"], "gpu_memory_utilization": 0.05,
                 "max_model_len": a.max_model_len, "max_num_seqs": a.max_num_seqs}
            if r["kv_dtype"] == "fp8":
                b["kv_cache_dtype"] = "fp8"
            prof["models"].append({"id": r["model"], "port": 8001 + i, "budget": b})
        Path(a.out).write_text("# models/budget.py가 생성 — 직접 고치지 말고 계산기 인자를 바꿔 다시 생성한다\n" +
                               yaml.safe_dump(prof, sort_keys=False, allow_unicode=True))
        print(f"  → {a.out}")


if __name__ == "__main__":
    main()
