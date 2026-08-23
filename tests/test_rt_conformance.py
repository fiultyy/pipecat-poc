#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Lane A/B conformance (W1.5; docs/kg/01-ws1-head-dsh.md §7 W1.5).

- Offline: the plugin's dais executor driven by an injected CLI mock —
  state machine + mailbox parsing without dais.
- Live: one identical intent pair through both lanes against the REAL
  dais bus; the test plays the orchestrator (drains the orchestrator
  mailbox, replies per ref). Lane A runs its own mailbox identity
  (``voice-head-a2a``) so the two consumers never contend for reads —
  check-messages is consume-on-read.

VO-005 dual-delivery conformance: the SAME {from,to,ref,type,body} envelope
delivered via push (session-send DSHMSG] injection — lands as the recipient
turn's first line) and via pull (dais mailbox send-message → check-messages)
must parse to the identical five-tuple at the recipient. Live-probed facts
baked in (2026-08-23): dais send-message accepts only ``--message-type
status``; session.history user/message events carry the injected line
verbatim; push needs a fleet-registered session (session-spawn, no GUI).
"""

import asyncio
import json
import re
import shutil
import subprocess
import sys
import time
import urllib.request
import uuid
from pathlib import Path

import pytest
import pytest_asyncio

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "realtime-provider-poc"))

from rt_a2a_client import A2aClient  # noqa: E402
from rt_dsh_backend import DshBackend  # noqa: E402
from rt_dsh_lane import DaisLane  # noqa: E402
import rt_dsh_lane as rt_dsh_lane_module  # noqa: E402
from rt_event_bus import EventBus  # noqa: E402
from rt_orchestrator import FINAL_PREFIX  # noqa: E402

PLUGIN_DIR = Path("~/.dsh/plugins/a2a-profile-server").expanduser()
DAIS_BIN = Path("~/.local/bin/dais").expanduser()
ORCH = "session_vhconf"
HEAD_A = "voice-head-a2a"
HEAD_B = "voice-head"
PORT = 8799

# ---- VO-005 push/pull dual-delivery conformance ----

FLEET_PATH = Path("~/.dsh/maestro/fleet.json").expanduser()
SESSION_SPAWN = Path("~/.dsh/maestro/bin/session-spawn").expanduser()
SESSION_SEND = Path("~/.dsh/maestro/bin/session-send").expanduser()
SESSION_PURGE = Path("~/.dsh/maestro/bin/session-purge").expanduser()
DSHMSG = "DSHMSG]"


def _boot(port: int, state_dir: Path, profile_root: Path, executor_expr: str) -> subprocess.Popen:
    """Boot the plugin HTTP server in node with a custom executor."""
    script = (
        "(async () => {"
        "const { createHttpServer } = await import('./http-server.js');"
        "const { createTaskStore } = await import('./task-store.js');"
        "const { createDaisExecutor } = await import('./executors/dais.js');"
        "const { mkdirSync } = await import('node:fs');"
        f"mkdirSync('{state_dir}', {{ recursive: true }});"
        f"mkdirSync('{profile_root}', {{ recursive: true }});"
        f"const tasks = createTaskStore('{state_dir}/tasks.jsonl');"
        "const http = createHttpServer({ tasks, profiles: null, token: 'conf-tok',"
        f" executor: {executor_expr} }});"
        f"console.log('READY ' + await http.start({port}));"
        "})().catch(e => { console.error('BOOTFAIL ' + (e?.stack ?? e)); process.exit(1); })"
    )
    proc = subprocess.Popen(
        ["node", "--input-type=module", "-e", script],
        cwd=PLUGIN_DIR, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    deadline = time.time() + 15
    while time.time() < deadline:
        line = proc.stdout.readline().decode()
        if "READY" in line:
            return proc
        if "BOOTFAIL" in line or proc.poll() is not None:
            break
    proc.kill()
    pytest.fail("plugin harness did not become ready")


def _env_ok() -> bool:
    return bool(shutil.which("node")) and PLUGIN_DIR.joinpath("index.js").exists()


# ---- offline: executor state machine over an injected CLI mock ----

@pytest.mark.asyncio
async def test_dais_executor_offline_state_machine(tmp_path):
    if not _env_ok():
        pytest.skip("node or a2a-profile-server plugin absent")
    executor_expr = (
        "(() => { let polls = 0;"
        " const run = async (argv) => {"
        "  const sub = argv[0];"
        "  if (sub === 'create-run') return { stdout: 'run_dais0001\\n', stderr: '' };"
        "  if (sub === 'send-message') return { stdout: 'enqueued seq=41\\n', stderr: '' };"
        "  if (sub === 'check-messages') { polls += 1;"
        "   if (polls === 1) return { stdout: 'no unread messages for voice-head-a2a\\n', stderr: '' };"
        "   return { stdout: '--- seq 42 from session_vhconf [status] done ---\\n"
        "[ref:vh-off1] 调研完成 【凭证R-OFF-1】 结论 41%\\n', stderr: '' };"
        "  }"
        "  return { stdout: '', stderr: '' };"
        " };"
        " return createDaisExecutor(tasks, { orchestratorHandle: 'session_vhconf',"
        "  headHandle: 'voice-head-a2a', pollMs: 50, run });"
        "})()"
    )
    proc = _boot(PORT, tmp_path / "state", tmp_path / "profiles", executor_expr)
    try:
        client = A2aClient(f"http://127.0.0.1:{PORT}", token="conf-tok",
                           poll_interval_s=0.05, timeout_s=10)
        task_id = await client.send("离线执行桥冒烟", ref="vh-off1")
        assert task_id.startswith("t_")
        body = await client.await_done(task_id, timeout_s=10)
        assert body == "调研完成 【凭证R-OFF-1】 结论 41%"
        assert not body.startswith(FINAL_PREFIX)  # 终稿前缀由 head 侧统一拼接
    finally:
        proc.kill()


# ---- live: A/B lanes against the real dais bus, test plays orchestrator ----

async def _bus_healthy(lane: DaisLane) -> bool:
    """The resident dais daemon occasionally wedges a dispatch under test
    churn (a send-message hangs on a futex holding the bus lock). Probe
    before committing to the live flow; callers skip with a reason."""
    try:
        await lane.check_status()
        return True
    except Exception:
        return False


@pytest.mark.asyncio
async def test_live_lane_a_b_conformance(tmp_path):
    if not _env_ok() or not DAIS_BIN.exists():
        pytest.skip("node or dais absent")
    executor_expr = (
        "createDaisExecutor(tasks, { orchestratorHandle: 'session_vhconf',"
        " headHandle: 'voice-head-a2a', pollMs: 500, timeoutMs: 60000 })"
    )

    async def attempt(token_suffix: str):
        proc = _boot(PORT, tmp_path / f"state{token_suffix}", tmp_path / f"p{token_suffix}",
                     executor_expr)
        try:
            lane = DaisLane(default_timeout_s=20)
            if not await _bus_healthy(lane):
                pytest.skip("dais bus unresponsive (daemon wedge; restart resident dais)")
            client = A2aClient(f"http://127.0.0.1:{PORT}", token="conf-tok",
                               poll_interval_s=0.1, timeout_s=10)
            finals_a, finals_b = [], []

            async def on_final_a(ref, message):
                finals_a.append((ref, message))

            async def on_final_b(ref, message):
                finals_b.append((ref, message))

            backend_a = DshBackend(lane=lane, lane_a=client, lane_mode="a",
                                   orchestrator_handle=ORCH, head_handle=HEAD_A,
                                   on_final=on_final_a, await_timeout_s=30)
            backend_b = DshBackend(lane=lane, orchestrator_handle=ORCH, head_handle=HEAD_B,
                                   on_final=on_final_b, await_timeout_s=30, poll_s=0.5)

            import json

            token_a, token_b = f"TOKEN-A2A-77{token_suffix}", f"TOKEN-B-88{token_suffix}"
            receipt_a = json.loads(await backend_a.dispatch(f"车道A一致性意图 {token_a}"))
            receipt_b = json.loads(await backend_b.dispatch(f"车道B一致性意图 {token_b}"))

            status = await lane.check_status()
            summaries = {e.get("id", ""): e.get("summary", "") for e in status["entries"]}

            seen: set[str] = set()
            deadline = time.monotonic() + 25
            while len(seen) < 2 and time.monotonic() < deadline:
                rows = await lane.check_messages(ORCH)
                for row in rows:
                    body = row.get("body", "")
                    m = re.search(r"\[ref:(vh-[0-9a-f]+)\]", body)
                    if not m or m.group(1) in seen:
                        continue
                    ref = m.group(1)
                    seen.add(ref)
                    to = row.get("from") or HEAD_B
                    run_id = next(
                        (rid for rid, summary in summaries.items()
                         if (token_a if to == HEAD_A else token_b) in summary),
                        receipt_b["run_id"],
                    )
                    await lane.send_reply(
                        run_id, ORCH, to,
                        "调研完成 【凭证R-CONF-A2A-8899】 结论 41%", ref,
                    )
                await asyncio.sleep(0.3)

            for _ in range(150):
                if finals_a and finals_b:
                    break
                await asyncio.sleep(0.2)

            assert finals_a and finals_b, (
                f"finals missing: a={finals_a!r} b={finals_b!r} seen={seen!r}"
            )
            expected = FINAL_PREFIX + "调研完成 【凭证R-CONF-A2A-8899】 结论 41%"
            assert finals_a[0][1] == expected
            assert finals_b[0][1] == expected
            assert receipt_a["run_id"].startswith("t_")   # A2A task handle
            assert receipt_b["run_id"].startswith("run_")  # dais run handle
            for receipt in (receipt_a, receipt_b):
                assert receipt["credentials"][0].startswith("【凭证")
        finally:
            proc.kill()

    try:
        await attempt("")
    except rt_dsh_lane_module.DaisLaneError:
        # one retry absorbs transient bus contention from the resident daemon
        await attempt("-r2")


# ---- live: VO-005 dual-delivery parity (push session-send vs pull mailbox) ----

def _loopback(port: str, method: str, payload: dict, timeout_s: float = 10.0) -> dict:
    """One maestro loopback RPC (session-send wire format); returns result.value."""
    wire = json.dumps({
        "type": "client-request", "rpcId": str(uuid.uuid4()), "method": method, "payload": payload,
    }).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/api/{method}",
        data=wire, headers={"content-type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        result = json.loads(resp.read()).get("result", {})
    if not result.get("ok"):
        raise RuntimeError(f"loopback {method} failed: {result.get('error')}")
    return result.get("value", {})


def _push_plane_up() -> bool:
    """Push plane = maestro loopback session.list answering."""
    try:
        fleet = json.loads(FLEET_PATH.read_text())
        return bool(_loopback(str(fleet.get("port", 3080)), "session.list", {}))
    except Exception:
        return False


def _dshmsg_lines(events: list) -> list[str]:
    """Machine-parseable DSHMSG] first-line texts from recipient turn events."""
    texts = []
    for e in events:
        ev = e.get("event", e)
        if ev.get("type") not in ("user/message", "agent/inbox/spliced"):
            continue
        data = ev.get("data", {})
        for msg in data.get("inserted") or [data]:
            for part in msg.get("content") or []:
                text = part.get("text", "")
                if text.startswith(DSHMSG):
                    texts.append(text)
    return texts


@pytest.mark.asyncio
async def test_dual_delivery_parity(tmp_path):
    """VO-005: same envelope via DSHMSG push and mailbox pull → identical
    five-tuple at the recipient; push lands as the turn's first (machine-
    parseable) line; mailbox reads consume."""
    if not _env_ok() or not DAIS_BIN.exists():
        pytest.skip("node or dais absent")
    if not SESSION_SEND.exists():
        pytest.skip("maestro session-send absent")
    lane = DaisLane(default_timeout_s=20)
    if not _push_plane_up():
        pytest.skip("dsh loopback plane down (push unreachable)")
    if not await _bus_healthy(lane):
        pytest.skip("dais bus unresponsive (daemon wedge; restart resident dais)")

    suffix = uuid.uuid4().hex[:6]
    ref = f"vh-dual-{suffix}"
    body = f"双投递对拍 TOKEN-DUAL-{suffix} 【凭证R-DUAL-{suffix}】"

    # recipient for push: fresh fleet session (session-spawn; no GUI dependency)
    spawned = subprocess.run(
        [SESSION_SPAWN, "standard", f"vh-dual-{suffix}",
         f"VO-005 dual-delivery parity probe {ref}"],
        capture_output=True, text=True, timeout=30,
    )
    codes = re.findall(r"\b([0-9a-f]{4})\b", spawned.stdout)
    assert codes, f"session-spawn produced no code: {spawned.stdout!r} {spawned.stderr!r}"
    code = codes[-1]
    envelope = {"from": HEAD_A, "to": code, "type": "steer", "ref": ref, "body": body}
    # byte-identical construction to session-send's json.dumps default separators
    line = DSHMSG + json.dumps(envelope, ensure_ascii=False)

    try:
        # ---- push: session-send → DSHMSG] as recipient turn first line ----
        await asyncio.sleep(0.5)  # bus-lock gap: spawn → send
        push = subprocess.run(
            [SESSION_SEND, HEAD_A, code, "steer", ref, body],
            capture_output=True, text=True, timeout=30,
        )
        assert push.returncode == 0 and "accepted=True" in push.stdout, (
            f"session-send failed: {push.stdout!r} {push.stderr!r}"
        )
        session_id = json.loads(FLEET_PATH.read_text())["fleet"][code]["sessionId"]
        push_line = None
        deadline = time.monotonic() + 20
        while push_line is None and time.monotonic() < deadline:
            await asyncio.sleep(0.7)  # bus-lock gap between loopback calls
            events = _loopback(str(json.loads(FLEET_PATH.read_text()).get("port", 3080)),
                               "session.history",
                               {"sessionId": session_id, "maxMessages": 10}).get("events", [])
            push_line = next((t for t in _dshmsg_lines(events) if f'"{ref}"' in t), None)
        assert push_line is not None, "pushed DSHMSG never reached recipient history"
        assert push_line == line, f"push wire line drifted:\n{push_line!r}\n{line!r}"
        push_tuple = json.loads(push_line[len(DSHMSG):])  # first line machine-parseable

        # ---- pull: same line via dais mailbox (send-message → check-messages) ----
        await asyncio.sleep(0.5)  # bus-lock gap: cross-plane
        run_id = await lane.create_run(f"vo005 dual-delivery parity {ref}")
        await asyncio.sleep(0.5)
        # live fact: only --message-type status is accepted by dais send-message
        await lane._run("send-message", run_id, HEAD_A, ORCH,
                        "--message-type", "status", "--subject", "route", "--body", line)
        pull_line = None
        deadline = time.monotonic() + 20
        while pull_line is None and time.monotonic() < deadline:
            await asyncio.sleep(0.7)
            for row in await lane.check_messages(ORCH, wait_s=2.0):
                text = row.get("body", "")
                if text.startswith(DSHMSG) and f'"{ref}"' in text:
                    pull_line = text
        assert pull_line is not None, "mailed envelope never reached ORCH mailbox"
        assert pull_line == line, f"pull wire line drifted:\n{pull_line!r}\n{line!r}"
        pull_tuple = json.loads(pull_line[len(DSHMSG):])

        # ---- parity: identical five-tuple, credentials verbatim ----
        assert push_tuple == pull_tuple == envelope
        assert push_tuple["body"] == body  # 凭证逐字（TOKEN 与【凭证…】未改写）

        # ---- read-consume: the mailbox snapshot consumed our row ----
        await asyncio.sleep(0.7)
        again = await lane.check_messages(ORCH, wait_s=2.0)
        assert not any(f'"{ref}"' in r.get("body", "") for r in again), (
            "mailbox row survived a read (consume-on-read broken)"
        )
    finally:
        # best-effort cleanup: purge refuses sessions written <5min ago (busy gate)
        subprocess.run([SESSION_PURGE, code], capture_output=True, timeout=30)
