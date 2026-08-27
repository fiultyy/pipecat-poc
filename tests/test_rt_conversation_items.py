#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""DoctrineSource + conversation-item mirror tests (offline, no services)."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "realtime-provider-poc"))

from rt_conversation_items import ConversationCompactor, ConversationLog  # noqa: E402
from rt_head_tools import DSH_TOOLS_DOCTRINE, DoctrineSource  # noqa: E402


def _msg(item_id, role, text):
    from types import SimpleNamespace
    return SimpleNamespace(type="message", role=role, id=item_id,
                           content=[SimpleNamespace(type="text", text=text)])


def _call(item_id, name, call_id, args):
    from types import SimpleNamespace
    return SimpleNamespace(type="function_call", id=item_id, name=name,
                           call_id=call_id, arguments=args)


def _output(item_id, call_id, out):
    from types import SimpleNamespace
    return SimpleNamespace(type="function_call_output", id=item_id,
                           call_id=call_id, output=out)


# ---- DoctrineSource ----


def test_doctrine_env_unset_falls_back():
    assert DoctrineSource(env={}).load() == DSH_TOOLS_DOCTRINE


def test_doctrine_env_file_loaded(tmp_path):
    f = tmp_path / "doctrine.md"
    f.write_text("自定义教义：只许说文言文。", encoding="utf-8")
    src = DoctrineSource(env={"VOICE_HEAD_DOCTRINE": str(f)})
    assert src.load() == "自定义教义：只许说文言文。"


def test_doctrine_missing_file_warns_and_falls_back(tmp_path):
    warnings = []
    src = DoctrineSource(env={"VOICE_HEAD_DOCTRINE": str(tmp_path / "nope.md")},
                         warn=warnings.append)
    assert src.load() == DSH_TOOLS_DOCTRINE
    assert warnings and "unreadable" in warnings[0]


def test_doctrine_blank_file_warns_and_falls_back(tmp_path):
    f = tmp_path / "blank.md"
    f.write_text("   \n", encoding="utf-8")
    warnings = []
    src = DoctrineSource(env={"VOICE_HEAD_DOCTRINE": str(f)}, warn=warnings.append)
    assert src.load() == DSH_TOOLS_DOCTRINE
    assert warnings and "blank" in warnings[0]


# ---- ConversationLog ----


def test_log_mirror_order_and_update_in_place():
    log = ConversationLog()
    log.add("i1", _msg("i1", "user", "帮我查黄金"))
    log.add("i2", _msg("i2", "assistant", "已受理"))
    log.add("i2", _msg("i2", "assistant", "已受理，凭证R-7734"))  # server update
    assert [i.item_id for i in log.items()] == ["i1", "i2"]
    assert log.items()[1].text == "已受理，凭证R-7734"


def test_log_summary_prompt_renders_tool_pairs():
    log = ConversationLog()
    log.add("i1", _msg("i1", "user", "查一下状态"))
    log.add("i2", _call("i2", "query_status", "call_9", "{}"))
    log.add("i3", _output("i3", "call_9", '{"runs": 1}'))
    prompt = log.to_summary_prompt()
    assert "[user] 查一下状态" in prompt
    assert "[tool_call query_status(call_9)] {}" in prompt
    assert "[tool_output call_9] {\"runs\": 1}" in prompt


def test_log_open_tool_pairs():
    log = ConversationLog()
    log.add("i2", _call("i2", "dispatch_intent", "call_1", '{"raw_intent":"X"}'))
    assert len(log.open_tool_pairs()) == 1, "unclosed call must be open"
    log.add("i3", _output("i3", "call_1", '{"status":"accepted"}'))
    assert log.open_tool_pairs() == [], "closed pair must not be open"


def test_compactor_trigger_threshold():
    log = ConversationLog()
    log.add("i1", _msg("i1", "user", "x" * 100))
    c = ConversationCompactor(log, trigger_chars=50)
    assert c.should_compact() is True
    c2 = ConversationCompactor(log, trigger_chars=200)
    assert c2.should_compact() is False


def test_compactor_zero_threshold_disables():
    log = ConversationLog()
    log.add("i1", _msg("i1", "user", "x" * 5000))
    assert ConversationCompactor(log, trigger_chars=0).should_compact() is False


# ---- ConversationCompactor：compact plan / state.snapshot（KG 14 §2.5，PR5）----


def test_compact_plan_deletes_all_but_open_pairs():
    log = ConversationLog()
    log.add("i1", _msg("i1", "user", "查任务"))
    log.add("i2", _call("i2", "query_status", "call_1", "{}"))
    log.add("i3", _output("i3", "call_1", '{"runs": 1}'))
    log.add("i4", _call("i4", "dispatch_intent", "call_2", '{"raw_intent":"X"}'))
    plan = ConversationCompactor(log).compact_plan()
    assert plan["delete_ids"] == ["i1", "i2", "i3"], "闭合并的历史项（含整对工具往返）全删"
    assert plan["pinned"] == 1, "在飞 call_2 钉住"
    assert plan["before_chars"] == log.text_chars()


def test_state_snapshot_store_union_running():
    log = ConversationLog()
    c = ConversationCompactor(log)
    now = 1759300123.4
    rows = [{"no": 1, "ref": "vh-1a", "status": "done", "summary": "采纳方案B…",
             "chars": 1834}]
    running = [
        {"ref": "vh-1a", "ts": now - 999},    # store 已有 → 沿台账终态，不重复
        {"ref": "vh-9f", "ts": now - 412.9},  # store 无 → running + elapsed_s
        {"ref": "", "ts": now},               # 空 ref 弃
    ]
    snap = c.state_snapshot(rows, running, now=now)
    assert snap["t"] == "state.snapshot" and snap["ts"] == now
    assert snap["tasks"] == [
        {"no": 1, "ref": "vh-1a", "status": "done",
         "summary": "采纳方案B…", "chars": 1834},
        {"ref": "vh-9f", "status": "running", "elapsed_s": 412},
    ]
    assert snap["counts"] == {"done": 1, "running": 1}


def test_state_snapshot_counts_aggregate():
    log = ConversationLog()
    c = ConversationCompactor(log)
    rows = [
        {"no": 1, "ref": "vh-a", "status": "done", "summary": "s1", "chars": 10},
        {"no": 2, "ref": "vh-b", "status": "failed", "summary": "s2", "chars": 0},
        {"no": 3, "ref": "vh-c", "status": "cancelled", "summary": "s3", "chars": 2},
    ]
    snap = c.state_snapshot(rows, [], now=1.0)
    assert snap["counts"] == {"done": 1, "failed": 1, "cancelled": 1}
    assert snap["tasks"] == [dict(r) for r in rows]


def test_log_add_accepts_mirror_sink_dict_and_drop():
    log = ConversationLog()
    log.add("m1", {"item_id": "m1", "type": "message", "role": "user",
                   "text": "镜像直喂", "name": None, "call_id": None})
    log.add("m2", {"type": "message", "role": "assistant",
                   "content": [{"type": "text", "text": "content 提取"}]})
    assert log.items()[0].text == "镜像直喂"
    assert log.items()[1].text == "content 提取"
    assert log.drop("m1").item_id == "m1"
    assert log.drop("m1") is None
    assert [i.item_id for i in log.items()] == ["m2"]
