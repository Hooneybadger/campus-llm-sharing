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



def troute(**kw):
    cfg = {"backends": ["http://a:1", "http://b:1"], "max_inflight_per_backend": 32, "kv_tokens_per_backend": 10000,
           "kv_fill": 1.0, "queue_max": 8, "queue_timeout_s": 0.5}
    cfg.update(kw)
    return ModelRoute("m", cfg)


def test_token_budget_admits_by_tokens_not_count():
    async def body():
        r = troute()                                   # 백엔드당 1만 토큰 × 2
        slots = [Slot(3000) for _ in range(6)]
        for sl in slots:
            assert await r.acquire(True, sl) is None   # 백엔드마다 9천
        w = asyncio.create_task(r.acquire(True, Slot(2000)))
        await asyncio.sleep(0.01)
        assert not w.done()                            # 합계는 1.8만이지만 어느 백엔드에도 2천이 들어가지 않는다
        r.release(slots[0])
        assert await w is None
        assert r.inflight == 6 and r.inflight_tokens == 17000
    run(body())


def test_token_mode_strict_fifo_no_overtaking():
    async def body():
        r = troute(backends=["http://a:1"])
        s0 = Slot(9500)
        await r.acquire(True, s0)
        big = asyncio.create_task(r.acquire(True, Slot(5000)))    # 안 들어감
        await asyncio.sleep(0.01)
        small = asyncio.create_task(r.acquire(True, Slot(400)))   # 들어갈 자리는 있지만 앞(big)을 추월하지 않는다
        await asyncio.sleep(0.01)
        assert not big.done() and not small.done()
        r.release(s0)
        assert await big is None and await small is None
    run(body())


def test_reservation_keeps_each_backend_within_its_kv():
    async def body():
        r = troute()
        a, b = Slot(8000), Slot(1000)
        await r.acquire(True, a)
        await r.acquire(True, b)
        assert a.backend is not b.backend              # 예약이 적은 쪽으로 간다
        c = Slot(8000)
        await r.acquire(True, c)                       # 건수가 같아도 토큰이 적은 쪽(b의 백엔드)을 고른다
        assert c.backend is b.backend
        assert all(x.tokens <= r.backend_budget for x in r.backends.values())
    run(body())


def test_pooled_room_does_not_admit_when_no_single_backend_fits():
    async def body():
        r = troute()
        await r.acquire(True, Slot(7000))
        await r.acquire(True, Slot(7000))              # 합계 여유는 6천이지만 백엔드마다 3천뿐
        w = asyncio.create_task(r.acquire(True, Slot(4000)))
        await asyncio.sleep(0.01)
        assert not w.done()
        assert await w == "queue_timeout"
    run(body())


def test_request_is_sent_to_reserved_backend_not_least_inflight():
    async def body():
        r = troute()
        s1, s2 = Slot(9000), Slot(100)
        await r.acquire(True, s1)
        await r.acquire(True, s2)
        s1.backend.inflight = 1
        s3 = Slot(9000)
        await r.acquire(True, s3)                      # s2의 백엔드만 자리가 있다
        s2.backend.inflight = 5                        # 건수는 더 많아도
        assert r.pick(slot=s3) is s3.backend is s2.backend
    run(body())


def test_reservation_moves_when_backend_leaves_before_send():
    async def body():
        r = troute()
        sl = Slot(3000)
        await r.acquire(True, sl)
        first = sl.backend
        first.healthy = False
        moved = r.pick(slot=sl)
        assert moved is not first and moved is sl.backend
        assert first.tokens == 0 and moved.tokens == 3000
        r.release(sl)
        assert moved.tokens == 0 and r.inflight_tokens == 0
    run(body())


def test_reservation_dropped_when_no_other_backend_has_room():
    async def body():
        r = troute()
        s1, s2 = Slot(9000), Slot(9000)
        await r.acquire(True, s1)
        await r.acquire(True, s2)
        s1.backend.healthy = False
        assert r.pick(slot=s1) is None                 # 살아 있는 백엔드에 자리가 없으면 옮기지 않는다
        r.release(s1)
        assert r.inflight == 1 and r.inflight_tokens == 9000
    run(body())


def test_small_request_bypasses_blocked_head_only_when_configured():
    async def body():
        r = troute(backends=["http://a:1"], bypass_tokens=500, bypass_age_s=5)
        s0 = Slot(9000)
        await r.acquire(True, s0)
        big = asyncio.create_task(r.acquire(True, Slot(5000)))
        await asyncio.sleep(0.01)
        assert await r.acquire(True, Slot(400)) is None    # 자리가 있는 작은 요청은 막힌 맨 앞을 앞지른다
        mid = asyncio.create_task(r.acquire(True, Slot(600)))  # 기준보다 크면 줄을 선다
        await asyncio.sleep(0.01)
        assert not big.done() and not mid.done()
        r.release(s0)
        assert await big is None and await mid is None
    run(body())


def test_bypass_stops_when_head_waited_too_long():
    async def body():
        r = troute(backends=["http://a:1"], bypass_tokens=500, bypass_age_s=0.01, queue_timeout_s=0.3)
        await r.acquire(True, Slot(9000))
        big = asyncio.create_task(r.acquire(True, Slot(5000)))
        await asyncio.sleep(0.05)                          # 맨 앞이 시한을 넘겼다
        small = asyncio.create_task(r.acquire(True, Slot(400)))
        await asyncio.sleep(0.01)
        assert not small.done()                            # 더 앞지르지 않아 긴 요청의 기아를 막는다
        assert await big == "queue_timeout" and await small == "queue_timeout"
    run(body())


def test_oversized_request_is_never_admitted():
    async def body():
        r = troute(backends=["http://a:1"])
        assert not r.fits(10001)                       # 비어 있어도 들어가지 않는다(게이트웨이가 400으로 먼저 거른다)
        assert await r.acquire(True, Slot(10001)) == "queue_timeout"
    run(body())


def test_backend_loss_does_not_overfill_survivor():
    async def body():
        r = troute()
        s = [Slot(4000) for _ in range(4)]
        for x in s:
            await r.acquire(True, x)                   # 백엔드마다 8천
        dead = s[0].backend
        dead.healthy = False
        w = asyncio.create_task(r.acquire(True, Slot(4000)))
        await asyncio.sleep(0.01)
        assert not w.done()                            # 죽은 쪽 여유가 살아 있는 쪽으로 넘어오지 않는다
        survivor = next(b for b in r.backends.values() if b is not dead)
        assert survivor.tokens == 8000
        for x in s:
            if x.backend is survivor:
                r.release(x)
                break
        assert await w is None
        assert survivor.tokens == 8000
    run(body())


def test_token_mode_uses_max_seqs_and_count_mode_uses_per_backend():
    import app
    r = troute(max_inflight_per_backend=5, max_seqs_per_backend=32)
    assert r.max_inflight == 64                      # 토큰 모드: 32 × 2
    app.ADMISSION["mode"] = "count"
    try:
        assert r.token_budget is None and r.max_inflight == 10   # 건수 모드: 5 × 2
    finally:
        app.ADMISSION["mode"] = "tokens"


def test_token_budget_shrinks_with_backend_failure():
    r = troute()
    assert r.token_budget == 20000 and r.backend_budget == 10000
    r.backends["http://a:1"].healthy = False
    assert r.token_budget == 10000 and r.backend_budget == 10000


def test_removed_backend_metric_series_is_deleted():
    import app
    r = route()
    app.M_BACKEND_UP.labels("m", "http://a:1").set(0)
    r.cfg["backends"] = ["http://b:1"]
    r.sync_backends()
    samples = [s for m in app.M_BACKEND_UP.collect() for s in m.samples if s.labels.get("backend") == "http://a:1"]
    assert samples == []


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))


# ---------------------------------------------------------------- 사용자별 사용량 (포털 '사용량' 화면)
from app import UsageBook, kst_day  # noqa: E402


def test_usage_book_counts_and_recent_order():
    u = UsageBook(recent=3)
    u.record("s1", "qwen3-4b", 200, "ok", 100, 20, 0.3, 1.2)
    u.record("s1", "qwen3-4b", 429, "user_rpm")
    u.record("s2", "gpt-oss-20b", 200, "ok", 50, 10)
    s = u.summary("s1")
    assert (s["requests"], s["prompt_tokens"], s["completion_tokens"]) == (2, 100, 20)
    assert s["codes"] == {"200": 1, "429": 1}
    assert [x["code"] for x in u.get("s1")["recent"]] == ["429", "200"]   # 최근 것이 먼저
    for i in range(5):
        u.record("s1", "m", 200, "ok")
    assert len(u.get("s1")["recent"]) == 3                               # 최근 기록은 상한만큼만


def test_usage_book_resets_at_kst_midnight(monkeypatch):
    u = UsageBook()
    u.record("s1", "m", 200, "ok", 10, 1)
    monkeypatch.setattr("app.kst_day", lambda ts=None: "2099-01-01")
    assert u.summary("s1")["requests"] == 0 and u.day == "2099-01-01"


def test_kst_day_is_utc_plus_nine():
    assert kst_day(0) == "1970-01-01" and kst_day(15 * 3600) == "1970-01-02"


def test_discovered_backend_starts_unhealthy():
    r = route()
    r.discovered = ["http://c:1"]
    r.sync_backends()
    assert r.backends["http://a:1"].healthy        # 정적 백엔드는 그대로
    assert not r.backends["http://c:1"].healthy    # 새 Pod 주소는 헬스체크 통과 전까지 제외
    assert "http://c:1" not in [b.url for b in r.healthy()]
    r.backends["http://a:1"].healthy = False
    r.sync_backends()
    assert not r.backends["http://a:1"].healthy    # 기존 백엔드의 헬스 상태는 다시 맞춰도 유지
