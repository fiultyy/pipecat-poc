#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Unit tests for OrcaLane (mocked CLI --json output, no real orca-ide
calls; envelope shapes live-probed 2026-08-23, orca app 1.4.185)."""

import asyncio
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "realtime-provider-poc"))

from rt_orca_lane import (  # noqa: E402
    OrcaLane,
    OrcaLaneError,
    OrcaLaneTimeout,
)


def env(result=None, *, ok=True, code=None, message=None) -> str:
    """Envelope builder: {id, ok, result | error{code, message}} (probed)."""
    body = {"id": "req-1", "ok": ok}
    if ok:
        body["result"] = result if result is not None else {}
    else:
        body["error"] = {"code": code or "error", "message": message or code or "error"}
    return json.dumps(body)


def make_lane(responses: list[str]):
    """Runner mock: pops one --json stdout payload per invocation."""
    queue = list(responses)

    async def runner(argv):
        stdout = queue.pop(0) if queue else ""
        return stdout, ""

    return OrcaLane(runner=runner), queue


# ---- R1: binary discipline ----


def test_bin_hardcoded_and_bare_orca_rejected():
    assert OrcaLane.BIN == "orca-ide"
    assert OrcaLane().binary == "orca-ide"
    with pytest.raises(ValueError, match="GNOME screen reader"):
        OrcaLane(binary="orca")


# ---- 探活 ----


def test_status_parses_envelope_result():
    lane, _ = make_lane([env({"app": {"running": True},
                              "runtime": {"state": "ready", "reachable": True}})])
    status = asyncio.run(lane.status())
    assert status["app"]["running"] is True
    assert status["runtime"]["state"] == "ready"
    assert lane._call_log[-1] == ["orca-ide", "status", "--json"]


# ---- spawn ----


CREATE_NEW = env({
    "worktree": {"id": "repo-123::/home/u/orca/wt-task", "path": "/home/u/orca/wt-task"},
    "agentTerminalHandle": "term_new_1",
})


def test_spawn_worktree_argv_and_new_handle():
    lane, _ = make_lane([CREATE_NEW])
    out = asyncio.run(lane.spawn_worktree(
        "wt-task", "path:/home/u/repo", "codex", "do the thing"))
    assert out == {"worktreeId": "repo-123::/home/u/orca/wt-task",
                   "terminalHandle": "term_new_1"}
    argv = lane._call_log[-1]
    assert argv[0] == "orca-ide" and argv[1:3] == ["worktree", "create"]
    for flag, val in [("--name", "wt-task"), ("--repo", "path:/home/u/repo"),
                      ("--agent", "codex"), ("--prompt", "do the thing"),
                      ("--setup", "run")]:
        i = argv.index(flag)
        assert argv[i + 1] == val
    assert argv[-1] == "--json"


def test_spawn_worktree_legacy_startup_terminal_handle():
    lane, _ = make_lane([env({"worktree": {"id": "r::/w"},
                              "startupTerminal": {"handle": "term_old_1"}})])
    out = asyncio.run(lane.spawn_worktree("n", "id:r", "claude", "p"))
    assert out["terminalHandle"] == "term_old_1"


def test_spawn_worktree_no_handle_returns_none():
    lane, _ = make_lane([env({"worktree": {"id": "r::/w"}})])
    out = asyncio.run(lane.spawn_worktree("n", "id:r", "codex", "p"))
    assert out["terminalHandle"] is None


def test_spawn_worktree_base_branch_and_setup_flag():
    lane, _ = make_lane([env({"worktree": {"id": "r::/w"}})])
    asyncio.run(lane.spawn_worktree("n", "id:r", "codex", "p",
                                    base_branch="origin/main", setup="skip"))
    argv = lane._call_log[-1]
    i = argv.index("--base-branch")
    assert argv[i + 1] == "origin/main"
    assert argv[argv.index("--setup") + 1] == "skip"


def test_spawn_worktree_invalid_setup_rejected_before_cli():
    lane, _ = make_lane([])
    with pytest.raises(ValueError, match="setup"):
        asyncio.run(lane.spawn_worktree("n", "id:r", "codex", "p", setup="boom"))
    assert lane._call_log == []


def test_spawn_worktree_missing_worktree_id_raises():
    lane, _ = make_lane([env({"startupTerminal": {"handle": "t"}})])
    with pytest.raises(OrcaLaneError, match="no worktree id"):
        asyncio.run(lane.spawn_worktree("n", "id:r", "codex", "p"))


# ---- 监控 ----


def test_terminal_list_returns_rows_and_worktree_selector():
    rows = [{"handle": "term_a", "worktreeId": "r::/w", "title": "codex"}]
    lane, _ = make_lane([env({"terminals": rows}), env({"terminals": rows})])
    assert asyncio.run(lane.terminal_list("id:r::/w")) == rows
    assert "--worktree" in lane._call_log[-1]
    assert asyncio.run(lane.terminal_list()) == rows
    assert "--worktree" not in lane._call_log[-1]


def test_read_tail_and_string_cursor_roundtrip():
    # live shape: result.terminal.tail[] + STRING cursors (nextCursor...)
    first = env({"terminal": {"handle": "term_a", "status": "running",
                              "tail": ["line 1", "line 2"], "truncated": False,
                              "limited": True, "oldestCursor": "0",
                              "nextCursor": "42", "latestCursor": "57",
                              "returnedLineCount": 2}})
    second = env({"terminal": {"handle": "term_a", "tail": ["line 3"],
                               "limited": False, "nextCursor": "57"}})
    lane, _ = make_lane([first, second])
    text, cursor = asyncio.run(lane.read("term_a"))
    assert text == "line 1\nline 2"
    assert cursor == "42"
    # incremental: feed nextCursor back; only new output returned
    text2, cursor2 = asyncio.run(lane.read("term_a", cursor=cursor))
    assert text2 == "line 3"
    assert cursor2 == "57"
    argv = lane._call_log[-1]
    assert argv[argv.index("--cursor") + 1] == "42"
    assert argv[argv.index("--limit") + 1] == "40"


def test_read_empty_tail():
    lane, _ = make_lane([env({"terminal": {"tail": [], "nextCursor": "0"}})])
    text, cursor = asyncio.run(lane.read("term_a"))
    assert text == "" and cursor == "0"


# ---- 有界等待（绝不裸等）----


def test_wait_argv_always_bounded():
    lane, _ = make_lane([env({"condition": "exit", "satisfied": True})] * 2)
    out = asyncio.run(lane.wait("term_a", what="tui-idle", timeout_ms=5000))
    assert out["condition"] == "exit"
    argv = lane._call_log[-1]
    assert argv[argv.index("--for") + 1] == "tui-idle"
    assert argv[argv.index("--timeout-ms") + 1] == "5000"
    # default path: timeout_ms always materialized — no naked wait ever
    asyncio.run(lane.wait("term_a"))
    argv = lane._call_log[-1]
    assert argv[argv.index("--timeout-ms") + 1] == "30000"
    assert argv[-1] == "--json"


def test_wait_cli_timeout_raises_orcalanetimeout():
    lane, _ = make_lane([env(ok=False, code="timeout", message="timeout")])
    with pytest.raises(OrcaLaneTimeout, match="timeout"):
        asyncio.run(lane.wait("term_a", timeout_ms=100))


def test_wait_timeout_is_lane_error_subtype():
    assert issubclass(OrcaLaneTimeout, OrcaLaneError)


def test_wait_subprocess_bound_exceeds_cli_bound():
    lane, _ = make_lane([env({"satisfied": True})])
    lane.recorded_timeout_s = None

    async def run():
        original = lane._run.__func__

        async def spy(*args, timeout_s=None):
            lane.recorded_timeout_s = timeout_s
            return await original(lane, *args, timeout_s=timeout_s)

        lane._run = spy
        await lane.wait("term_a", timeout_ms=90000)

    asyncio.run(run())
    assert lane.recorded_timeout_s >= 90000 / 1000 + 10 - 1e-9


def test_wait_invalid_args_rejected_before_cli():
    lane, _ = make_lane([])
    with pytest.raises(ValueError, match="what"):
        asyncio.run(lane.wait("term_a", what="forever"))
    with pytest.raises(ValueError, match="timeout_ms"):
        asyncio.run(lane.wait("term_a", timeout_ms=0))
    assert lane._call_log == []


# ---- 干预 ----


def test_send_enter_default_and_disabled():
    lane, _ = make_lane([env({"sent": True}), env({"sent": True})])
    asyncio.run(lane.send("term_a", "continue"))
    argv = lane._call_log[-1]
    assert argv[argv.index("--text") + 1] == "continue"
    assert "--enter" in argv and "--interrupt" not in argv
    asyncio.run(lane.send("term_a", "y", enter=False))
    assert "--enter" not in lane._call_log[-1]


def test_interrupt_sends_interrupt_flag_without_text():
    lane, _ = make_lane([env({"sent": True})])
    asyncio.run(lane.interrupt("term_a"))
    argv = lane._call_log[-1]
    assert argv[1:3] == ["terminal", "send"]
    assert "--interrupt" in argv
    assert "--text" not in argv and "--enter" not in argv


def test_stop_targets_worktree_selector():
    lane, _ = make_lane([env({"stopped": ["term_a", "term_b"]})])
    asyncio.run(lane.stop("id:repo-123::/home/u/orca/wt-task"))
    assert lane._call_log[-1] == [
        "orca-ide", "terminal", "stop",
        "--worktree", "id:repo-123::/home/u/orca/wt-task", "--json",
    ]


# ---- 汇总 ----


def test_worktree_ps_returns_summary():
    summary = {"worktrees": [{"worktreeId": "r::/w", "displayName": "task",
                              "workspaceStatus": "in-progress"}]}
    lane, _ = make_lane([env(summary)])
    out = asyncio.run(lane.worktree_ps())
    assert out["worktrees"][0]["worktreeId"] == "r::/w"
    assert lane._call_log[-1] == ["orca-ide", "worktree", "ps", "--json"]


# ---- 错误面（形制对齐 DaisLaneError）----


def test_ok_false_envelope_raises_with_cli_error_code():
    lane, _ = make_lane([env(ok=False, code="terminal_handle_stale",
                             message="terminal_handle_stale")])
    with pytest.raises(OrcaLaneError, match="terminal_handle_stale"):
        asyncio.run(lane.read("term_bogus"))


def test_non_json_output_raises():
    lane, _ = make_lane(["not json at all\n"])
    with pytest.raises(OrcaLaneError, match="non-json"):
        asyncio.run(lane.status())


def test_empty_output_raises():
    lane, _ = make_lane([""])
    with pytest.raises(OrcaLaneError):
        asyncio.run(lane.status())


def test_error_message_carries_command_prefix_and_detail():
    lane, _ = make_lane([env(ok=False, code="no_active_terminal")])
    with pytest.raises(OrcaLaneError, match=r"terminal read.*no_active_terminal"):
        asyncio.run(lane.read("term_x"))


def test_every_invocation_carries_json_flag():
    lane, _ = make_lane([env(), CREATE_NEW, env(),
                         env({"terminal": {"tail": [], "nextCursor": "0"}}),
                         env(), env(), env(), env()])
    asyncio.run(lane.status())
    asyncio.run(lane.spawn_worktree("n", "id:r", "codex", "p"))
    asyncio.run(lane.terminal_list())
    asyncio.run(lane.read("term_a"))
    asyncio.run(lane.wait("term_a"))
    asyncio.run(lane.send("term_a", "hi"))
    asyncio.run(lane.interrupt("term_a"))
    asyncio.run(lane.worktree_ps())
    assert len(lane._call_log) == 8
    assert all(argv[-1] == "--json" for argv in lane._call_log)
    assert all(argv[0] == "orca-ide" for argv in lane._call_log)
