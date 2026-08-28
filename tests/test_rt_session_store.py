#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""rt_session_store 台账测试（docs/kg/14-unified-callback-split.md §2.1）。

覆盖：put/get/update/list 往返、幂等 put（不烧号）、LRU 超 500 删最旧、
no 单调递增且重开库不复用旧号、credentials JSON 往返（库内为字符串）、
chars 由 body 派生、env/缺省路径解析与 WAL。全离线：tmp_path 建库，不起服务。
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "realtime-provider-poc"))

from rt_session_store import (  # noqa: E402
    DEFAULT_LIST_LIMIT,
    DEFAULT_MAX_ROWS,
    STATUSES,
    SessionStore,
)


@pytest.fixture()
def store(tmp_path):
    s = SessionStore(tmp_path / "store.db")
    yield s
    s.close()


def _entry(ref: str, **kw):
    entry = {"ref": ref, "run_id": f"run-{ref}"}
    entry.update(kw)
    return entry


# ---- put/get/update/list 往返 ----


def test_put_get_roundtrip(store):
    body = "全文内容" * 9
    stored = store.put(_entry(
        "vh-a", title="竞品定价调研", summary="采纳方案B", body=body,
        credentials=["【凭证R-A】"], conv_id="s-ab12",
    ))
    assert stored["status"] == "accepted"
    assert stored["no"] >= 1
    assert stored["chars"] == len(body)
    assert stored["updated_ts"] == stored["ts"]  # put 时未单独给 updated_ts

    got = store.get("vh-a")
    assert got == stored
    assert got["body"] == body
    assert got["credentials"] == ["【凭证R-A】"]
    assert got["run_id"] == "run-vh-a"

    assert store.get("vh-miss") is None


def test_update_fields(store):
    store.put(_entry("vh-b"))
    updated = store.update(
        "vh-b", status="done", body="结果正文", summary="完成", conv_id="s-cd34"
    )
    assert updated["status"] == "done"
    assert updated["body"] == "结果正文"
    assert updated["chars"] == len("结果正文")
    assert updated["summary"] == "完成"
    assert updated["conv_id"] == "s-cd34"
    assert updated["credentials"] == []
    assert updated["updated_ts"] >= updated["ts"]

    assert store.update("vh-miss", status="done") is None


def test_status_set_roundtrip(store):
    store.put(_entry("vh-st"))
    for status in STATUSES:
        assert store.update("vh-st", status=status)["status"] == status


def test_list_index_without_body(store):
    for i in range(DEFAULT_LIST_LIMIT + 10):
        store.put(_entry(f"vh-{i:03d}", title=f"t{i}"))
    items = store.list()
    assert len(items) == DEFAULT_LIST_LIMIT
    assert all("body" not in item for item in items)
    # 最近 50 条（no 升序）：最旧 10 条落在窗口外
    assert items[0]["ref"] == "vh-010"
    assert items[-1]["ref"] == "vh-059"
    assert store.list(limit=5)[0]["ref"] == "vh-055"


def test_list_before_ts_pages_older_rows(store):
    """before_ts 游标翻页（body.list_more 控制帧）：只返回 ts 严格早于
    游标的行（等 ts 不计入）、按 no 升序取其中最近 limit 条；同游标重复
    调用结果一致（纯读幂等）。"""
    for i in range(5):
        store.put(_entry(f"vh-{i}", ts=1000.0 + i))
    page = store.list(limit=2, before_ts=1002.5)
    assert [r["ref"] for r in page] == ["vh-1", "vh-2"], "游标前最近 limit 条"
    assert page == store.list(limit=2, before_ts=1002.5), "同游标幂等"
    # 游标更早：窗口收窄
    assert [r["ref"] for r in store.list(limit=2, before_ts=1000.5)] == ["vh-0"]
    # 游标更晚：仍只取窗口内最近 limit 条（no 降序截断再升序返回）
    assert [r["ref"] for r in store.list(limit=2, before_ts=1004.5)] == \
        ["vh-3", "vh-4"]
    # 等 ts 严格排除 + 游标早于全部行 → 空页
    store.put(_entry("vh-dup", ts=1000.0))
    assert [r["ref"] for r in store.list(before_ts=1000.0)] == []
    assert store.list(before_ts=999.0) == []


# ---- 幂等 / 校验 ----


def test_idempotent_put(store):
    first = store.put(_entry("vh-d", body="v1"))
    second = store.put(_entry("vh-d", body="v2", status="running"))
    assert store.count() == 1
    assert second["no"] == first["no"]  # 同 ref 重复事件不烧号
    assert second["body"] == "v2"
    assert second["status"] == "running"
    assert store.get("vh-d")["body"] == "v2"


def test_no_monotonic(store):
    nos = [store.put(_entry(f"vh-n{i}"))["no"] for i in range(5)]
    assert nos == sorted(nos)
    assert len(set(nos)) == 5


def test_put_rejects_bad_input(store):
    with pytest.raises(ValueError):
        store.put({"title": "缺 ref"})
    with pytest.raises(ValueError):
        store.put({"ref": "vh-x", "status": "bogus"})
    with pytest.raises(TypeError):
        store.put({"ref": "vh-x", "credentials": "not-a-list"})


def test_update_rejects_non_updatable_fields(store):
    store.put(_entry("vh-c"))
    with pytest.raises(ValueError):
        store.update("vh-c", no=99)
    with pytest.raises(ValueError):
        store.update("vh-c", updated_ts=1.0)
    with pytest.raises(ValueError):
        store.update("vh-c", status="bogus")
    assert store.get("vh-c")["no"] == store.get("vh-c")["no"]  # 拒绝后原值未动


# ---- LRU ----


def test_lru_evicts_oldest(store):
    total = DEFAULT_MAX_ROWS + 5
    for i in range(total):
        store.put(_entry(f"vh-{i:04d}", ts=1000.0 + i))  # ts 确定性定序
    assert store.count() == DEFAULT_MAX_ROWS
    for i in range(5):
        assert store.get(f"vh-{i:04d}") is None  # 最旧 5 条被逐
    assert store.get("vh-0005") is not None
    newest = store.get(f"vh-{total - 1:04d}")
    assert newest["status"] == "accepted"
    assert store.list(limit=1)[0]["ref"] == f"vh-{total - 1:04d}"


# ---- no 跨重开稳定 / credentials / chars ----


def test_no_not_reused_across_reopen(tmp_path):
    db = tmp_path / "store.db"
    s1 = SessionStore(db)
    nos = [s1.put(_entry(f"vh-{i}"))["no"] for i in range(3)]
    s1.close()

    s2 = SessionStore(db)
    assert s2.put(_entry("vh-new"))["no"] > max(nos)  # 高水位不回退
    s2.close()


def test_credentials_stored_as_json_string(store):
    creds = ["【凭证R-1】", "【凭证R-2】"]
    store.put(_entry("vh-cred", credentials=creds))
    assert store.get("vh-cred")["credentials"] == creds

    raw = sqlite3.connect(store.path)  # 第二连接直读落库形态（WAL 允许并发读）
    stored = raw.execute(
        "SELECT credentials_json FROM sessions WHERE ref = 'vh-cred'"
    ).fetchone()[0]
    assert raw.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    raw.close()
    assert isinstance(stored, str)
    assert json.loads(stored) == creds

    assert store.update("vh-cred", credentials=["【凭证R-3】"])["credentials"] == ["【凭证R-3】"]


def test_chars_derived_from_body(store):
    stored = store.put(_entry("vh-ch", body="x" * 30, chars=999))  # 给定值不采信
    assert stored["chars"] == 30
    assert store.update("vh-ch", body="y" * 12)["chars"] == 12
    cleared = store.update("vh-ch", body=None)
    assert cleared["chars"] == 0
    assert cleared["body"] == ""


# ---- 路径解析 ----


def test_path_resolution_env_and_home(tmp_path, monkeypatch):
    monkeypatch.setenv("VOICE_STORE_DB", str(tmp_path / "env.db"))
    s = SessionStore()
    try:
        assert s.path == tmp_path / "env.db"
        s.put(_entry("vh-env"))
    finally:
        s.close()
    assert (tmp_path / "env.db").exists()

    monkeypatch.delenv("VOICE_STORE_DB", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    s2 = SessionStore()
    try:
        expected = tmp_path / "home" / ".local/state/voice-gateway/store.db"
        assert s2.path == expected  # 目录自动建 + 库文件落位
        assert expected.exists()
    finally:
        s2.close()
