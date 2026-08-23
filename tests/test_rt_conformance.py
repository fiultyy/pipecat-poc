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
"""

import asyncio
import re
import shutil
import subprocess
import sys
import time
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
