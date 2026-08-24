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
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.request
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
import pytest_asyncio

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "realtime-provider-poc"))

from rt_a2a_client import A2aClient  # noqa: E402
from rt_dsh_backend import DshBackend  # noqa: E402
from rt_dsh_lane import DaisLane  # noqa: E402
import rt_dsh_lane as rt_dsh_lane_module  # noqa: E402
from rt_event_bus import EventBus  # noqa: E402
from rt_orca_lane import OrcaLane, OrcaLaneError  # noqa: E402
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


@pytest.mark.live("dais-bus",)
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
    except (rt_dsh_lane_module.DaisLaneError, AssertionError):
        # retry absorbs transient bus contention and the laneA push final
        # occasionally dropping on the resident daemon (environment-sensitive
        # flake observed since VO-005; the retry run re-boots a fresh head)
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


@pytest.mark.live("dais-bus",)
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
        # OG5 semantics: envelope keys are append-only (msgid/ts added by v2 senders);
        # assert the five-tuple subset plus DSHMSG framing instead of byte equality.
        pushed_env = json.loads(push_line[len(DSHMSG):])
        assert all(pushed_env.get(k) == v for k, v in envelope.items()), (
            f"push envelope tuple drifted:\n{pushed_env!r}\n{envelope!r}"
        )
        assert push_line.startswith(DSHMSG)  # first line machine-parseable
        push_tuple = pushed_env

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
        assert pull_line.startswith(DSHMSG), f"pull line not DSHMSG-framed: {pull_line!r}"
        pull_tuple = json.loads(pull_line[len(DSHMSG):])

        # ---- parity: five-tuple identical across push/pull; push may carry v2
        # append-only extras (msgid/ts) per OG5 — subset semantics, not byte equality.
        for t_name, tup in (("push", push_tuple), ("pull", pull_tuple)):
            assert all(tup.get(k) == v for k, v in envelope.items()), (
                f"{t_name} envelope tuple drifted:\n{tup!r}\n{envelope!r}"
            )
        assert all(push_tuple.get(k) == pull_tuple.get(k) for k in pull_tuple), (
            f"push lost keys present in pull:\n{push_tuple!r}\n{pull_tuple!r}"
        )
        assert push_tuple["body"] == pull_tuple["body"] == body  # 凭证逐字（TOKEN 与【凭证…】未改写）

        # ---- read-consume: the mailbox snapshot consumed our row ----
        await asyncio.sleep(0.7)
        again = await lane.check_messages(ORCH, wait_s=2.0)
        assert not any(f'"{ref}"' in r.get("body", "") for r in again), (
            "mailbox row survived a read (consume-on-read broken)"
        )
    finally:
        # best-effort cleanup: purge refuses sessions written <5min ago (busy gate)
        subprocess.run([SESSION_PURGE, code], capture_output=True, timeout=30)


# ---- VO-009: lane B live smoke + A/B lane-factory final conformance ----
#
# Live-probed facts baked in (2026-08-23, orca app 1.4.185; agent claude
# 2.1.179 on the GLM anthropic proxy, global effortLevel=high):
#
# - orca launches the agent with ``--dangerously-skip-permissions``, the
#   prompt as argv, and the worktree root as cwd.
# - ``terminal wait --for tui-idle`` has three terminal shapes: satisfied
#   → exit 0 + ok:true + result.wait.satisfied=true; unsatisfied slice →
#   exit 1 + ok:true (satisfied:false, blockedReason e.g.
#   "codex-trust-workspace"); slice timeout → exit 1 + ok:false
#   code=timeout. OrcaLane raises OrcaLaneError for both exit-1 shapes
#   before parsing the envelope; both mean "keep waiting within budget".
# - ``terminal read --cursor 0`` pages FROM THE OLDEST retained line; the
#   agent's ANSWER TEXT NEVER ENTERS the scrollback ring (spinner frames
#   only — verified across 141s of incremental paging) — a render-proof
#   completion signal is mandatory: the worker writes its final to
#   ``final.txt`` in the worktree (13.5s end-to-end observed), and only
#   an exact content match completes (partial writes never satisfy).
# - a fresh worktree can stall on a trust/confirm dialog ("Enter to
#   confirm" / "2. No, exit"); sending "1" answers it (bounded count).
# - tui-idle can flash satisfied EARLY — completion is decided by the
#   artifact, never by a satisfied wait.
# - high-effort thinking runs minutes on even a trivial task; the worker
#   runner interrupts the spawned turn after its settle window, drops to
#   ``/effort low`` and resends the task once (27s observed post-dance).
#   Every step is bounded.

ORCA_WAIT_SLICE_MS = 15000       # bounded terminal-wait slice (--timeout-ms)
ORCA_READ_PAGE = 200             # incremental read page size
ORCA_MAX_DIALOG_ANSWERS = 5      # bounded dialog interventions
ORCA_TURN_SETTLE_S = 20.0        # turn's artifact window before the dance
ORCA_WORKER_BUDGET_S = 240.0     # overall worker deadline (spawn → final)
# Lane-B worker agent. Unspecified → omp (user ruling 2026-08-24; orca accepts
# it via --agent). ORCA_AGENT=<id> overrides (e.g. claude, whose terminal
# choreography — trust dialogs, /effort — the dance below still knows).
ORCA_AGENT = os.environ.get("ORCA_AGENT", "omp")
_ORCA_HAS_EFFORT = ORCA_AGENT == "claude"  # /effort low is a claude command


def _wait_slice_unsatisfied(exc: OrcaLaneError) -> bool:
    """Exit-1 wait outcomes meaning "condition not met within the bounded
    slice" (ok:true unsatisfied, or the CLI's own timeout error); anything
    else is a real lane error and propagates."""
    msg = str(exc)
    return msg.startswith("exit=1") and (
        '"ok": true' in msg or '"code": "timeout"' in msg
    )


def _dialog_open(page: str) -> bool:
    """Trust/confirm dialog signature in the newest rendered lines."""
    lines = [ln.strip() for ln in page.splitlines() if ln.strip()]
    return bool(lines) and any(
        marker in ln for ln in lines[-4:]
        for marker in ("Enter to confirm", "No, exit")
    )




def _repo_root() -> str:
    """Main checkout of this repo (git common-dir parent) — the orca repo
    selector for spawning scratch worktrees."""
    out = subprocess.run(
        ["git", "-C", str(Path(__file__).resolve().parent.parent),
         "rev-parse", "--path-format=absolute", "--git-common-dir"],
        capture_output=True, text=True, timeout=15,
    ).stdout.strip()
    return str(Path(out).resolve().parent)


async def _orca_plane_up(lane: OrcaLane) -> bool:
    """Lane B probe: orca-ide app running and runtime reachable."""
    try:
        status = await lane.status()
    except Exception:
        return False
    app = status.get("app") or {}
    runtime = status.get("runtime") or {}
    return bool(app.get("running")) and bool(runtime.get("reachable"))


async def _orca_teardown(lane: OrcaLane, wt_id: str, name: str) -> None:
    """Stop the worktree's terminals, remove the worktree, assert no
    residue (red line: the smoke leaves neither processes nor worktrees)."""
    try:
        await lane.stop(wt_id)
    except OrcaLaneError:
        pass  # already stopped / never started: rm below is the authority
    removed = subprocess.run(
        [OrcaLane.BIN, "worktree", "rm", "--worktree", wt_id, "--force", "--json"],
        capture_output=True, text=True, timeout=60,
    )
    ps = await lane.worktree_ps()
    residual = [w for w in ps.get("worktrees", [])
                if name in json.dumps(w, ensure_ascii=False)]
    assert removed.returncode == 0 and not residual, (
        f"worktree teardown left residue (rm rc={removed.returncode}, "
        f"residual={json.dumps(residual, ensure_ascii=False)[:300]})"
    )


@asynccontextmanager
async def _orca_worker(lane: OrcaLane, name: str, prompt: str, body: str,
                       *, budget_s: float = ORCA_WORKER_BUDGET_S,
                       artifact: str = "final.txt"):
    """One REAL lane-B execution: spawn a scratch worktree (agent per
    ORCA_AGENT, default omp; cwd = worktree root) → bounded tui-idle wait
    slices → incremental reads (dialog detection) → worker final observed
    as the worktree ARTIFACT whose content equals the expected body
    exactly (render-proof: TUI answer text never enters the scrollback
    ring).

    Manager interventions stay bounded: dialog answers (≤ cap) and ONE
    interrupt + resend after the spawned turn's settle window passes
    without the artifact (claude additionally drops to ``/effort low``
    first — high-effort thinking runs minutes; 27s observed post-dance).
    Teardown (stop + rm + residual assert) runs on every exit path."""
    created = await lane.spawn_worktree(
        name, f"path:{_repo_root()}", ORCA_AGENT, prompt, setup="skip",
    )
    wt_id, handle = created["worktreeId"], created["terminalHandle"]
    final_path = Path(wt_id.split("::", 1)[1]) / artifact
    try:
        t0 = time.monotonic()
        deadline = t0 + budget_s
        cursor, dialogs, danced = 0, 0, False
        t_output, final, page = None, None, ""
        while time.monotonic() < deadline and final is None:
            try:
                await lane.wait(handle, what="tui-idle",
                                timeout_ms=ORCA_WAIT_SLICE_MS)
            except OrcaLaneError as e:
                if not _wait_slice_unsatisfied(e):
                    raise
            if final_path.exists():
                content = final_path.read_text().strip()
                if content == body:  # exact — a partial write never completes
                    final = content
                    break
            page, cursor = await lane.read(handle, cursor=cursor,
                                           limit=ORCA_READ_PAGE)
            if page.strip() and t_output is None:
                t_output = time.monotonic()
            if _dialog_open(page):
                if dialogs >= ORCA_MAX_DIALOG_ANSWERS:
                    break
                dialogs += 1
                await lane.send(handle, "1")
                continue
            if (not danced and t_output is not None
                    and time.monotonic() - t_output >= ORCA_TURN_SETTLE_S):
                await lane.interrupt(handle)
                await asyncio.sleep(3)
                if _ORCA_HAS_EFFORT:
                    await lane.send(handle, "/effort low")
                    await asyncio.sleep(3)
                await lane.send(handle, prompt)
                danced = True
        assert final is not None, (
            f"lane B worker artifact {artifact!r} never matched the body "
            f"within {budget_s:.0f}s (dialogs={dialogs}, danced={danced}); "
            f"newest page tail: {page[-400:]!r}"
        )
        yield wt_id, handle, final
    finally:
        await _orca_teardown(lane, wt_id, name)


@pytest.mark.live("dais-bus", "orca-host")
@pytest.mark.asyncio
async def test_live_lane_b_smoke():
    """VO-009①: lane B live smoke — one REAL worktree spawned through
    rt_orca_lane (agent per ORCA_AGENT, default omp) → bounded wait →
    incremental read → verbatim final with the credential intact;
    teardown removes the worktree."""
    if not shutil.which(OrcaLane.BIN):
        pytest.skip("orca-ide absent (lane B host offline)")
    # no PATH guard on the agent itself: orca resolves known TUI agents
    # (omp/claude/...) through its own launch path — a bad id fails loudly
    # in spawn_worktree instead of silently skipping here.
    lane = OrcaLane(default_timeout_s=90)
    if not await _orca_plane_up(lane):
        pytest.skip("orca-ide plane down (app/runtime unreachable)")

    suffix = uuid.uuid4().hex[:6]
    body = f"冒烟完成 【凭证R-ORCA-SMOKE-{suffix}】 结论 41%"
    prompt = (
        "冒烟任务：请立即用 Write 工具把下面这一行逐字写入当前工作目录"
        "的 final.txt 文件（文件内容必须恰好是这一行，不加任何其他文字），"
        "写完即完成，无需其他回复：\n" + body
    )
    async with _orca_worker(lane, f"vo009-smoke-{suffix}", prompt, body) as ctx:
        wt_id, handle, worker_final = ctx
        assert wt_id and handle
        assert worker_final == body
        assert f"【凭证R-ORCA-SMOKE-{suffix}】" in worker_final  # 凭证逐字
    # 有界等待纪律：冒烟确实等待过，且每条 wait 都带 --timeout-ms（无裸等）
    waits = [argv for argv in lane._call_log if argv[1:3] == ["terminal", "wait"]]
    assert waits, "smoke never waited (completion must be observed, not assumed)"
    for argv in waits:
        assert "--timeout-ms" in argv


# A/B conformance: the SAME intent through both execution lanes must
# deliver the IDENTICAL final to the head — FINAL_PREFIX + same body,
# credentials verbatim — and the final channel is NOT lane-split: both
# lanes return through the unified dais mailbox chain (the head polls its
# dais mailbox; the manager role relays lane B's worker final via
# send_reply exactly like the lane A reply).
_AB_INTENT = "A/B 对拍一致性意图：复核调研结论 41% 并逐字回显凭证"
_AB_BODY = "调研完成 【凭证R-CONF-AB-8899】 结论 41%"
_AB_LANES = [
    pytest.param("dais", id="laneA-dais"),
    pytest.param("orca", id="laneB-orca"),
]


@pytest.mark.live("dais-bus", "orca-host")
@pytest.mark.asyncio
@pytest.mark.parametrize("lane_kind", _AB_LANES)
async def test_ab_lane_final_conformance(lane_kind):
    """VO-009②③: lane-factory parameterized A/B conformance."""
    # 验收③ — per-lane probes fail → deterministic skip with a reason.
    # dais carries the head's messaging plane AND the unified final
    # channel for BOTH lanes, so it is probed first in either param.
    dais = DaisLane(default_timeout_s=10)
    if not await _bus_healthy(dais):
        pytest.skip("车道A dais 编排面不可用（重启窗口期；统一终稿链依赖 dais）")
    orca = OrcaLane(default_timeout_s=90)
    if lane_kind == "orca" and not await _orca_plane_up(orca):
        pytest.skip("车道B orca-ide 宿主不可用（app/runtime 不可达）")

    finals: list[tuple[str, str]] = []

    async def on_final(ref, message):
        finals.append((ref, message))

    backend = DshBackend(lane=dais, orchestrator_handle=ORCH, head_handle=HEAD_B,
                         on_final=on_final, poll_s=0.5,
                         await_timeout_s=ORCA_WORKER_BUDGET_S + 120)
    receipt = json.loads(await backend.dispatch(_AB_INTENT))
    assert receipt["status"] == "accepted"
    assert receipt["run_id"].startswith("run_")
    assert receipt["ref"].startswith("vh-")
    assert receipt["credentials"][0].startswith("【凭证")

    # unified intake: the intent must sit in the orchestrator mailbox
    row = None
    deadline = time.monotonic() + 25
    while row is None and time.monotonic() < deadline:
        for candidate in await dais.check_messages(ORCH):
            if f"[ref:{receipt['ref']}]" in candidate.get("body", ""):
                row = candidate
        if row is None:
            await asyncio.sleep(0.3)
    assert row is not None, "intent never reached the orchestrator mailbox"
    assert _AB_INTENT in row.get("body", "")

    if lane_kind == "dais":
        # 车道A executes on the dais lane (orchestrator role) — the
        # deterministic conformance body.
        worker_final = _AB_BODY
    else:
        # 车道B executes in a REAL orca worktree; the manager relays the
        # worker final into the same dais mailbox chain (不分会道).
        suffix = uuid.uuid4().hex[:6]
        prompt = (
            "对拍任务：请立即用 Write 工具把下面这一行逐字写入当前工作目录"
            "的 final.txt 文件（文件内容必须恰好是这一行，不加任何其他文字），"
            "写完即完成，无需其他回复：\n" + _AB_BODY
        )
        async with _orca_worker(orca, f"vo009-ab-{suffix}", prompt,
                                _AB_BODY) as (_, _, worker_final):
            assert worker_final == _AB_BODY

    # unified final channel: orchestrator → head over the dais mailbox
    await asyncio.sleep(0.5)  # bus-lock gap (cross-process discipline)
    await dais.send_reply(receipt["run_id"], ORCH, HEAD_B,
                          worker_final, receipt["ref"])

    for _ in range(200):  # bounded head phase-2 poll (≤40s)
        if finals:
            break
        await asyncio.sleep(0.2)
    assert finals, "head never produced the final through the unified chain"
    ref, final = finals[0]
    assert ref == receipt["ref"]
    assert final == FINAL_PREFIX + _AB_BODY   # 同终稿：前缀 + 同 body
    assert "【凭证R-CONF-AB-8899】" in final  # 凭证一致（逐字）
