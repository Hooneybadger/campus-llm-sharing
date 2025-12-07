"""탄력 배치 판단 — 서빙 묶음(unit)별 수요를 보고 확장 풀 GPU에 복제를 늘리거나 줄인다.

k8s도 게이트웨이도 모르는 순수 로직이다. 실제 클러스터(rental/manager.py)와 8노드 에뮬레이션(sim/l3_dataplane.py
--elastic)이 같은 판단을 쓰도록 분리했다. 입력은 묶음별 관측값, 출력은 실행할 동작 목록이다.

  바닥     : 묶음마다 전용 노드 1개(배치 밖)는 항상 떠 있다. 여기서는 확장 복제만 다룬다.
  늘리기   : 대기열이 있거나 예산 사용률이 up_util 이상인 상태가 up_hold_s 이어지면 확장 1개.
             빈 풀 GPU가 없으면 한가한 다른 묶음의 확장을 회수해 넘긴다(steal).
  줄이기   : 사용률이 down_util 미만, 대기열 0인 상태가 down_hold_s 이어지면 확장 1개 회수.
  출렁임 방지: 기동 중인 복제가 있으면 같은 묶음은 더 늘리지 않는다. 동작 뒤 cooldown_s,
             확장은 준비된 뒤 min_life_s 동안 회수하지 않는다(기동 비용 20~115초를 치렀으므로).
  시간표   : 수업처럼 미리 아는 수요는 시작 lead_s 전부터 확장 수 하한(extra)을 둔다.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))
DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


@dataclass
class UnitView:
    util: float                       # 묶음 안 모델들의 예산 사용률 최댓값(0~1, 넘을 수 있음)
    queue: int                        # 묶음 안 모델들의 게이트웨이 대기열 합
    ready_ages: list[float] = field(default_factory=list)   # 준비된 확장 복제마다 준비 후 지난 초
    starting: int = 0                 # 기동 중인 확장 복제 수

    @property
    def extras(self) -> int:
        return len(self.ready_ages) + self.starting


@dataclass
class Config:
    up_util: float = 0.9
    up_hold_s: float = 30
    down_util: float = 0.3
    down_hold_s: float = 600
    min_life_s: float = 300
    cooldown_s: float = 60
    steal_max_util: float = 0.5       # 이보다 바쁜 묶음의 확장은 뺏지 않는다
    lead_s: float = 300
    schedule: list[dict] = field(default_factory=list)
    # schedule 항목: {"unit", "extra", "days": ["tue"], "start": "13:00", "end": "15:50"}  (KST 벽시계)
    #           또는 {"unit", "extra", "from_s": 0, "to_s": 1800}                   (판단기 시작 기준 초, 에뮬레이션용)


def unit_load(state: dict, models: list[str]) -> tuple[float, int]:
    """게이트웨이 /admin/state에서 묶음의 (사용률, 대기열). 사용률 = 처리 중 토큰 / 토큰 예산(예산이 없는 모델은 건수 / 건수 한도)."""
    ms = [state["models"][m] for m in models if m in state.get("models", {})]
    util = max((m["inflight_tokens"] / m["token_budget"] if m.get("token_budget") else m["inflight"] / max(1, m["max_inflight"])
                for m in ms), default=0.0)
    return util, sum(m["queue"] for m in ms)


class Policy:
    def __init__(self, cfg: Config | None = None, t0: float = 0.0):
        self.cfg = cfg or Config()
        self.t0 = t0
        self.hot_since: dict[str, float] = {}
        self.cold_since: dict[str, float] = {}
        self.last_action: dict[str, float] = {}

    # ------------------------------------------------------------------ 시간표
    def floor(self, unit: str, now: float) -> int:
        c, need = self.cfg, 0
        for e in c.schedule:
            if e["unit"] != unit:
                continue
            if "from_s" in e:
                t = now - self.t0
                on = e["from_s"] - c.lead_s <= t < e["to_s"]
            else:
                dt = datetime.fromtimestamp(now, KST)
                sh, sm = map(int, e["start"].split(":"))
                eh, em = map(int, e["end"].split(":"))
                start = dt.replace(hour=sh, minute=sm, second=0, microsecond=0)
                end = dt.replace(hour=eh, minute=em, second=0, microsecond=0)
                on = DAYS[dt.weekday()] in e.get("days", DAYS) and start - timedelta(seconds=c.lead_s) <= dt < end
            if on:
                need = max(need, int(e["extra"]))
        return need

    # ------------------------------------------------------------------ 판단
    def decide(self, now: float, units: dict[str, UnitView], free: int) -> list[tuple]:
        """동작 목록: ("expand", unit) | ("reclaim", unit) | ("steal", victim, unit).
        steal은 victim의 확장 하나를 회수한 GPU에 unit 확장을 띄우라는 뜻이다."""
        c = self.cfg
        for u, v in units.items():
            hot = v.queue > 0 or v.util >= c.up_util
            cold = v.queue == 0 and v.util < c.down_util
            self.hot_since.setdefault(u, now) if hot else self.hot_since.pop(u, None)
            self.cold_since.setdefault(u, now) if cold else self.cold_since.pop(u, None)

        acts: list[tuple] = []
        touched: set[str] = set()

        def cooled(u: str) -> bool:
            return now - self.last_action.get(u, -1e18) >= c.cooldown_s

        def victim_for(u: str) -> str | None:
            cands = []
            for w, v in units.items():
                if w == u or w in touched or w in self.hot_since:
                    continue
                old = [a for a in v.ready_ages if a >= c.min_life_s]
                if old and len(v.ready_ages) > self.floor(w, now) and v.util < c.steal_max_util and v.queue == 0:
                    cands.append((v.util, w))
            return min(cands)[1] if cands else None

        def grow(u: str, why_floor: bool) -> bool:
            nonlocal free
            if free > 0:
                acts.append(("expand", u))
                free -= 1
            else:
                w = victim_for(u)
                if w is None:
                    return False
                acts.append(("steal", w, u))
                touched.add(w)
                self.last_action[w] = now
            touched.add(u)
            self.last_action[u] = now
            if not why_floor:
                self.hot_since[u] = now          # 다음 확장은 다시 up_hold_s를 채운 뒤에
            return True

        # 1) 시간표 하한
        for u, v in units.items():
            if v.extras < self.floor(u, now) and u not in touched:
                grow(u, True)

        # 2) 붐비는 묶음부터 늘리기
        hot = sorted((u for u in units if u in self.hot_since and u not in touched),
                     key=lambda u: (units[u].queue, units[u].util), reverse=True)
        for u in hot:
            v = units[u]
            if now - self.hot_since[u] >= c.up_hold_s and v.starting == 0 and cooled(u):
                grow(u, False)

        # 3) 오래 한가한 묶음 줄이기
        for u, v in units.items():
            if u in touched or u not in self.cold_since:
                continue
            if (now - self.cold_since[u] >= c.down_hold_s and len(v.ready_ages) > self.floor(u, now)
                    and any(a >= c.min_life_s for a in v.ready_ages) and cooled(u)):
                acts.append(("reclaim", u))
                touched.add(u)
                self.last_action[u] = now
                self.cold_since[u] = now
        return acts
