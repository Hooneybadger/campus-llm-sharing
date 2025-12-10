"""실습 포털 대화 기록 저장소(ChatStore) 테스트 — 임시 SQLite 파일로 검증한다(네트워크 없음)."""
import importlib.util
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location("portal_app", Path(__file__).resolve().parent.parent / "portal/app.py")
portal = importlib.util.module_from_spec(spec)
spec.loader.exec_module(portal)


def body(text="리스트와 튜플 차이?", **kw):
    return {"model": "Qwen3-4B-Instruct-2507", "system": "", "messages": [
        {"role": "user", "content": text},
        {"role": "assistant", "content": "리스트는 바꿀 수 있고…", "model": "Qwen3-4B-Instruct-2507", "streaming": True,
         "meta": {"ttft": 0.1}}], **kw}


def test_save_list_get_delete_per_user(tmp_path):
    s = portal.ChatStore(tmp_path / "c.sqlite3")
    s.save("u1", "chat-0001", body())
    s.save("u2", "chat-0002", body("다른 사용자 질문"))
    assert [c["id"] for c in s.list("u1")] == ["chat-0001"]          # 다른 사용자의 대화는 보이지 않는다
    got = s.get("u1", "chat-0001")
    assert got["title"] == "리스트와 튜플 차이?"
    assert "streaming" not in got["messages"][1] and got["messages"][1]["meta"] == {"ttft": 0.1}
    assert s.get("u2", "chat-0001") is None
    assert s.delete("u2", "chat-0001") is False and s.delete("u1", "chat-0001") is True
    assert s.list("u1") == []


def test_latest_first_and_old_chats_trimmed(tmp_path, monkeypatch):
    s = portal.ChatStore(tmp_path / "c.sqlite3")
    monkeypatch.setattr(portal.ChatStore, "MAX_CHATS", 3)
    for i in range(5):
        s.save("u1", f"chat-000{i}", body(f"질문 {i}"))
    assert [c["title"] for c in s.list("u1")] == ["질문 4", "질문 3", "질문 2"]
    s.save("u1", "chat-0002", body("질문 2 이어서"))             # 같은 대화를 다시 저장하면 맨 위로
    assert s.list("u1")[0]["id"] == "chat-0002"


@pytest.mark.parametrize("bad", [
    {"messages": []},
    {"messages": [{"role": "system", "content": "x"}]},
    {"messages": [{"role": "user", "content": 3}]},
    {"messages": [{"role": "user", "content": "x" * (600 * 1024)}]},
])
def test_rejects_malformed_or_oversized(tmp_path, bad):
    s = portal.ChatStore(tmp_path / "c.sqlite3")
    with pytest.raises(ValueError):
        s.save("u1", "chat-0001", bad)
    assert s.list("u1") == []
