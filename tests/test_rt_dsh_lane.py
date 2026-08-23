#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Unit tests for DaisLane (mocked CLI) + one live smoke against real dais."""

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "realtime-provider-poc"))

from rt_dsh_lane import DaisLane, DaisLaneError, _parse_status  # noqa: E402


def make_lane(responses: list[tuple[str, str]]):
    """Runner mock: pops (stdout, stderr) per invocation."""
    queue = list(responses)

    async def runner(argv):
        assert argv[1] == "orchestration", f"expected orchestration subcommand, got {argv}"
        if not queue:
            raise AssertionError(f"unexpected extra call: {' '.join(argv)}")
        return queue.pop(0)

    return DaisLane(runner=runner), queue


def test_create_run_parses_id():
    lane, q = make_lane([("run_<redacted>\n", "")])
    assert asyncio.run(lane.create_run("目标")) == "run_<redacted>"
    assert lane._call_log[-1][2] == "create-run"


def test_create_task_with_deps():
    lane, _ = make_lane([("task_69c2a7e34dda\n", "")])
    tid = asyncio.run(lane.create_task("run_x", "spec", deps=["task_a", "task_b"]))
    assert tid == "task_69c2a7e34dda"
    argv = lane._call_log[-1]
    assert argv.count("--dep") == 2


def test_start_worker_ctx_handle():
    lane, _ = make_lane([("ctx_ab12cd34\n", "")])
    assert asyncio.run(lane.start_worker("task_x", command="echo hi")) == "ctx_ab12cd34"


def test_send_intent_body_carries_ref_prefix():
    lane, _ = make_lane([("enqueued seq=7\n", "")])
    seq = asyncio.run(lane.send_intent("run_x", "session_abc", "调研 X", ref="vh-1"))
    assert seq == 7
    argv = lane._call_log[-1]
    body = argv[argv.index("--body") + 1]
    assert body == "[ref:vh-1] 调研 X"


def test_check_messages_plain_and_json_rows():
    lane, _ = make_lane([(
        'seq=3 from=orch to=voice-head type=status body=[ref:vh-1] done ok\n'
        '{"seq":4,"from":"orch","to":"voice-head","type":"status","body":"plain row"}\n',
        "",
    )])
    rows = asyncio.run(lane.check_messages("session_abc"))
    assert len(rows) == 2
    assert rows[0]["seq"] == 3
    assert rows[1]["seq"] == 4


def test_await_done_matches_ref_and_strips_prefix():
    reply_hit = ['seq=9 from=orch to=voice-head type=status body=[ref:vh-2] "Agent Final Message": 结果 【凭证R-1】']
    calls = {"n": 0}

    async def runner(argv):
        calls["n"] += 1
        if calls["n"] == 1:
            return ("(no messages)\n", "")
        return (reply_hit[0] + "\n", "")

    lane = DaisLane(runner=runner)

    async def run():
        return await lane.await_done("session_abc", "vh-2", timeout_s=2, poll_s=0.01)

    body = asyncio.run(run())
    assert body.startswith('"Agent Final Message"')
    assert "【凭证R-1】" in body


def test_await_done_timeout_not_error_until_deadline():
    async def runner(argv):
        return ("(no messages)\n", "")

    lane = DaisLane(runner=runner)

    async def run():
        await lane.await_done("session_abc", "vh-3", timeout_s=0.05, poll_s=0.02)

    with pytest.raises(TimeoutError):
        asyncio.run(run())


def test_read_worker_cursor_from_stderr():
    lane, _ = make_lane([("line1\nline2\n", "cursor: 42\n")])
    tail, cursor = asyncio.run(lane.read_worker("ctx_x", after=40))
    assert "line1" in tail and cursor == 42


# ---- live two-line flavor + await_done guards (probed 2026-08-23) ----

LIVE_MAILBOX = (
    "--- seq 3 from session_orch [status] done ---\n"
    "[ref:vh-live1] done 【凭证R-LIVE-9】 结论 41%\n"
    "\n"
    "--- seq 4 from session_orch [status] done ---\n"
    "[ref:vh-live2] 第二段\n多行 body\n"
    "\n"
    "no unread messages for voice-head\n"
)


def test_parse_live_two_line_flavor():
    from rt_dsh_lane import _parse_message_rows

    rows = _parse_message_rows(LIVE_MAILBOX)
    assert len(rows) == 2
    assert rows[0] == {"seq": 3, "from": "session_orch", "to": "",
                       "type": "status", "subject": "done",
                       "body": "[ref:vh-live1] done 【凭证R-LIVE-9】 结论 41%"}
    assert rows[1]["seq"] == 4
    assert rows[1]["body"] == "[ref:vh-live2] 第二段\n多行 body"
    assert _parse_message_rows("no unread messages for voice-head\n") == []


def test_await_done_after_seq_and_from_filters_reject_own_intent():
    own_intent = ("--- seq 10 from voice-head [status] intent ---\n"
                  "[ref:vh-f1] 调研 X（这是我发出的 intent，不是回信）\n")
    reply = ("--- seq 11 from session_orch [status] done ---\n"
             "[ref:vh-f1] done 【凭证R-F1】\n")
    calls = {"n": 0}

    async def runner(argv):
        calls["n"] += 1
        return (own_intent if calls["n"] == 1 else reply, "")

    lane = DaisLane(runner=runner)

    async def run():
        return await lane.await_done("voice-head", "vh-f1", timeout_s=2,
                                     poll_s=0.01, after_seq=10,
                                     from_filter="session_orch")

    body = asyncio.run(run())
    assert body == "done 【凭证R-F1】"


def test_await_done_inbox_buffers_other_refs_for_concurrent_waiters():
    both = ("--- seq 21 from session_orch [status] done ---\n"
            "[ref:vh-c1] done C1 【凭证R-C1】\n"
            "\n"
            "--- seq 22 from session_orch [status] done ---\n"
            "[ref:vh-c2] done C2 【凭证R-C2】\n")
    calls = {"n": 0}

    async def runner(argv):
        calls["n"] += 1
        return (both if calls["n"] == 1 else "no unread messages for voice-head\n", "")

    lane = DaisLane(runner=runner)

    async def run():
        first = await lane.await_done("voice-head", "vh-c1", timeout_s=2, poll_s=0.01)
        # second waiter must find its reply in the buffer without a new poll
        second = await lane.await_done("voice-head", "vh-c2", timeout_s=2, poll_s=0.01)
        return first, second, calls["n"]

    first, second, n = asyncio.run(run())
    assert (first, second) == ("done C1 【凭证R-C1】", "done C2 【凭证R-C2】")
    assert n == 1  # one drain served both waiters


def test_fail_dispatch_and_exit_nonzero_raises():
    lane, q = make_lane([("", "boom")])

    # non-zero exit needs the subprocess path; simulate via runner raising DaisLaneError
    async def runner(argv):
        raise DaisLaneError("exit=1 fail-dispatch: boom")

    lane.runner = runner
    with pytest.raises(DaisLaneError):
        asyncio.run(lane.fail_dispatch("ctx_x", "reason"))


def test_parse_status_plain_flavors():
    status = _parse_status("1 runs\n  run_<redacted> dais-orchestration skill smoke 2026-08-22\n")
    assert status["runs"] == 1
    assert status["entries"][0]["id"] == "run_<redacted>"
    status2 = _parse_status("Run run_<redacted>: 3 tasks")
    assert status2["entries"][0]["tasks"] == 3


# ---- live smoke against the real dais binary (skip when absent) ----

@pytest.mark.parametrize("missing", [False])
def test_live_dais_roundtrip(missing):
    import shutil

    if not Path("~/.local/bin/dais").expanduser().exists():
        pytest.skip("dais binary absent")
    lane = DaisLane(default_timeout_s=30)

    # bus-health pre-probe (same idiom as test_rt_conformance): the orchestration
    # plane lives in the resident dais app; when it is down the CLI reports
    # "orchestration is not enabled in this build" and every call would fail.
    def healthy() -> bool:
        try:
            asyncio.run(lane.check_status())
            return True
        except Exception:
            return False

    if not healthy():
        pytest.skip("dais orchestration plane down (resident app not running)")

    async def run():
        run_id = await lane.create_run("vh-lane smoke: create/status roundtrip")
        status = await lane.check_status(run_id)
        assert any(e.get("id") == run_id for e in status["entries"]), status
        task_id = await lane.create_task(run_id, "vh-lane smoke task (no worker)")
        return run_id, task_id

    run_id, task_id = asyncio.run(run())
    assert run_id.startswith("run_") and task_id.startswith("task_")

    # reading a dispatch with no registered terminal view is a CLI error
    # (daemon behavior settled by 2026-08-23: exit 1, "no terminal view")
    async def read_missing():
        await lane.read_worker("ctx_nonexistent_probe", after=0)

    with pytest.raises(DaisLaneError):
        asyncio.run(read_missing())
