"""게이트웨이 입장 제어(ModelRoute) 단위 테스트 — 네트워크 없이 asyncio로 검증한다.
실행: .venv/bin/python -m pytest -q tests/
"""
import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "gateway"))
from app import ModelRoute, Slot  # noqa: E402


def route(**kw):
    cfg = {"backends": ["http://a:1", "http://b:1"], "max_inflight_per_backend": 2, "queue_max": 2, "queue_timeout_s": 0.3}
    cfg.update(kw)
    return ModelRoute("m", cfg)


def run(coro):
    return asyncio.run(coro)


def test_capacity_scales_with_healthy_backends():
    r = route()
    assert r.max_inflight == 4
    r.backends["http://a:1"].healthy = False
    assert r.max_inflight == 2          # 복제 하나가 죽으면 수용량도 절반


def test_static_max_inflight_kept_for_backward_compat():
    r = ModelRoute("m", {"backends": ["http://a:1"], "max_inflight": 7})
    assert r.max_inflight == 7


def test_queue_full_and_timeout():
    async def body():
        r = route()
        for _ in range(4):
            assert await r.acquire(True) is None
        w1 = asyncio.create_task(r.acquire(True))
        w2 = asyncio.create_task(r.acquire(True))
        await asyncio.sleep(0.01)
        assert await r.acquire(True) == "queue_full"     # 대기열 2개가 이미 참
        assert await w1 == "queue_timeout"                # 시한 0.3s 초과
        assert await w2 == "queue_timeout"
        assert r.inflight == 4
    run(body())


def test_release_hands_slot_to_waiter_fifo():
    async def body():
        r = route()
        for _ in range(4):
            await r.acquire(True)
        w = asyncio.create_task(r.acquire(True))
        await asyncio.sleep(0.01)
        r.release()
        assert await w is None
        assert r.inflight == 4                             # 슬롯을 그대로 넘김
    run(body())


def test_shrink_does_not_hand_over_when_over_capacity():
    async def body():
        r = route(queue_timeout_s=1.0)
        for _ in range(4):
            await r.acquire(True)
        w = asyncio.create_task(r.acquire(True))
        await asyncio.sleep(0.01)
        r.backends["http://a:1"].healthy = False            # 한도 4 → 2, 진행 중 4
        r.release()                                          # 4 > 2 → 넘기지 않고 감소
        assert r.inflight == 3 and not w.done()
        r.release(); r.release()                             # 3 → 2(한도와 같음) → 넘김
        assert await w is None
        assert r.inflight == 2
    run(body())


def test_pump_admits_waiters_when_capacity_grows():
    async def body():
        r = route(queue_timeout_s=2.0)
        r.backends["http://a:1"].healthy = False             # 한도 2
        await r.acquire(True); await r.acquire(True)
        w = asyncio.create_task(r.acquire(True))
        await asyncio.sleep(0.01)
        assert not w.done()
        r.backends["http://a:1"].healthy = True              # 복구 → 한도 4
        r.pump()
        assert await w is None
        assert r.inflight == 3
    run(body())


def test_passthrough_never_queues():
    async def body():
        r = route()
        for _ in range(50):
            assert await r.acquire(False) is None
        assert r.inflight == 50
    run(body())


def test_draining_backend_excluded_from_pick_and_capacity():
    r = route()
    r.backends["http://a:1"].draining = True
    assert [b.url for b in r.healthy()] == ["http://b:1"]
    assert r.pick().url == "http://b:1"
    assert r.max_inflight == 2



