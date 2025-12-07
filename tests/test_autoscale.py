"""탄력 배치 판단(rental/autoscale.py) 단위 테스트 — 시간을 직접 넣어 결정만 검증한다.
실행: .venv/bin/python -m pytest -q tests/
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "rental"))
from autoscale import Config, Policy, UnitView  # noqa: E402

S, G, Q = "serving-small", "serving-gptoss", "serving-qwen30b"


def views(**kw):
    base = {S: UnitView(0.5, 0), G: UnitView(0.5, 0), Q: UnitView(0.5, 0)}
    base.update(kw)
    return base


def test_expands_only_after_hold():
    p = Policy(Config(up_hold_s=30))
    assert p.decide(0, views(**{S: UnitView(0.95, 3)}), free=2) == []
    assert p.decide(20, views(**{S: UnitView(0.95, 3)}), free=2) == []
    assert p.decide(31, views(**{S: UnitView(0.95, 3)}), free=2) == [("expand", S)]


def test_short_spike_does_not_expand():
    p = Policy(Config(up_hold_s=30))
    p.decide(0, views(**{S: UnitView(0.95, 3)}), free=2)
    p.decide(20, views(**{S: UnitView(0.4, 0)}), free=2)      # 몰림이 풀림 → 타이머 초기화
    assert p.decide(40, views(**{S: UnitView(0.95, 1)}), free=2) == []


def test_no_second_expand_while_starting():
    p = Policy(Config(up_hold_s=0, cooldown_s=0))
    assert p.decide(0, views(**{S: UnitView(1.0, 5)}), free=3) == [("expand", S)]
    assert p.decide(10, views(**{S: UnitView(1.0, 5, starting=1)}), free=2) == []


def test_hottest_unit_first_when_one_gpu_left():
    p = Policy(Config(up_hold_s=0))
    acts = p.decide(0, views(**{S: UnitView(0.95, 1), G: UnitView(1.0, 9)}), free=1)
    assert acts == [("expand", G)]


def test_steals_idle_expansion_when_pool_full():
    p = Policy(Config(up_hold_s=0, min_life_s=300))
    acts = p.decide(0, views(**{G: UnitView(1.0, 8), Q: UnitView(0.1, 0, ready_ages=[400])}), free=0)
    assert acts == [("steal", Q, G)]


def test_does_not_steal_young_or_busy_expansion():
    p = Policy(Config(up_hold_s=0, min_life_s=300, steal_max_util=0.5))
    assert p.decide(0, views(**{G: UnitView(1.0, 8), Q: UnitView(0.1, 0, ready_ages=[100])}), free=0) == []
    assert p.decide(1, views(**{G: UnitView(1.0, 8), Q: UnitView(0.7, 0, ready_ages=[900])}), free=0) == []


def test_reclaims_after_long_idle_only():
    p = Policy(Config(down_hold_s=600, min_life_s=300))
    v = views(**{Q: UnitView(0.1, 0, ready_ages=[1000])})
    assert p.decide(0, v, free=0) == []
    assert p.decide(599, v, free=0) == []
    assert p.decide(601, v, free=0) == [("reclaim", Q)]


def test_schedule_floor_prewarms_before_class_and_blocks_reclaim():
    cfg = Config(lead_s=300, down_hold_s=0, min_life_s=0,
                 schedule=[{"unit": S, "extra": 2, "from_s": 1000, "to_s": 2000}])
    p = Policy(cfg, t0=0)
    assert p.decide(600, views(), free=4) == []                       # 시작 400초 전: 아직
    assert p.decide(700, views(), free=4) == [("expand", S)]          # 시작 300초 전부터 하한 2
    idle = views(**{S: UnitView(0.0, 0, ready_ages=[500, 500])})
    assert p.decide(1500, idle, free=2) == []                         # 수업 중엔 한가해도 하한 유지
    assert p.decide(2100, idle, free=2) == [("reclaim", S)]           # 끝나면 줄인다


def test_wall_clock_schedule_kst():
    from datetime import datetime
    from autoscale import KST
    cfg = Config(lead_s=300, schedule=[{"unit": S, "extra": 3, "days": ["tue"], "start": "13:00", "end": "15:50"}])
    p = Policy(cfg)
    tue_1258 = datetime(2026, 10, 27, 12, 58, tzinfo=KST).timestamp()   # 2026-10-27은 화요일
    wed_1258 = datetime(2026, 10, 28, 12, 58, tzinfo=KST).timestamp()
    assert p.floor(S, tue_1258) == 3 and p.floor(S, wed_1258) == 0
