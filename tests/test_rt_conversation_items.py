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
