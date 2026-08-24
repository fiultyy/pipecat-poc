#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""M0 verification: orchestrator probes + fake A2A contract (offline).

Writes the evidence table to docs/kg/evidence/m0-probe.md on success so
the probe latencies stay recorded next to the KG.
"""

import asyncio
import json
import sys
from pathlib import Path

import pytest
import pytest_asyncio

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "realtime-provider-poc"))

from fake_a2a_server import FakeTaskStore, build_app  # noqa: E402
from rt_env import ZHIPU_ENV_PATH, glm_credentials  # noqa: E402
from rt_probe_m0 import run_matrix, render_evidence  # noqa: E402

EVIDENCE = Path(__file__).parent.parent / "docs" / "kg" / "evidence" / "m0-probe.md"


# ---- probes: real orchestrator CLIs (skip when a binary/runtime is absent) ----

def _dais_plane_up() -> bool:
    """The dais orchestration plane lives in the resident dais app. When it is
    down the CLI exits 1 ("orchestration is not enabled in this build"); that is
    an environment state, not a probe regression — skip the dais entries."""
    import subprocess

    dais = Path("~/.local/bin/dais").expanduser()
    if not dais.exists():
        return False
    try:
        return subprocess.run(
            [str(dais), "orchestration", "check-status"],
            capture_output=True, timeout=30,
        ).returncode == 0
    except Exception:
        return False


@pytest.mark.live("dais-bus", "orca-host")
@pytest.mark.parametrize("name", ["dais runtime json", "dais check-status", "orca-ide status"])
def test_probe_matrix(name: str):
    if name.startswith("dais") and not _dais_plane_up():
        pytest.skip("dais orchestration plane down (resident app not running)")
    results = {r.name: r for r in asyncio.run(run_matrix())}
    res = results[name]
    assert res.ok, f"{name} failed: {res.detail}"
    assert res.latency_ms < 5000, f"{name} too slow: {res.latency_ms:.0f} ms"


@pytest.mark.live("dais-bus", "orca-host")
def test_evidence_written():
    results = asyncio.run(run_matrix())
    if not all(r.ok for r in results):
        pytest.skip(f"probes not all green, evidence not written: {[r.name for r in results if not r.ok]}")
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE.write_text(render_evidence(results), encoding="utf-8")
    assert "dais check-status | PASS" in EVIDENCE.read_text(encoding="utf-8")


# ---- env wiring (N5§4 credential chain) ----

def test_zhipu_env_chain():
    if not ZHIPU_ENV_PATH.exists():
        pytest.skip("~/.dsh/zhipu.env absent on this box")
    key, base_url = glm_credentials()
    assert key, "ZHIPU_CODING_PLAN_API_KEY must resolve via rt_env chain"
    assert base_url.startswith("https://")


# ---- fake A2A server contract (aiohttp in-process) ----

@pytest_asyncio.fixture
async def client():
    from aiohttp.test_utils import TestClient, TestServer

    server = TestServer(build_app(FakeTaskStore(complete_s=0.05)))
    test_client = TestClient(server)
    await test_client.start_server()
    yield test_client
    await test_client.close()


@pytest.mark.asyncio
async def test_agent_card(client):
    resp = await client.get("/.well-known/agent-card.json")
    assert resp.status == 200
    card = await resp.json()
    assert card["name"] == "voice-head-orchestrator"
    assert card["skills"][0]["id"] == "dispatch"


async def _rpc(client, method: str, params: dict) -> dict:
    resp = await client.post("/", json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    assert resp.status == 200
    return await resp.json()


@pytest.mark.asyncio
async def test_message_send_lifecycle(client):
    result = await _rpc(client, "message/send", {
        "message": {"role": "user", "parts": [{"type": "text", "text": "调研 WebGPU 现状"}]},
        "context": {"source": "voice-head", "ref": "vh-1"},
    })
    task = result["result"]["task"]
    assert task["state"] == "submitted"

    for _ in range(40):  # ≤2s: submitted→working→completed at 0.05s steps
        got = await _rpc(client, "tasks/get", {"taskId": task["id"]})
        state = got["result"]["task"]["state"]
        if state == "completed":
            artifact = got["result"]["task"]["artifacts"][0]["content"]
            assert artifact.startswith('"Agent Final Message":')
            assert "【凭证FAKE-" in artifact  # credential convention relayed verbatim
            assert "调研 WebGPU 现状" in artifact
            return
        await asyncio.sleep(0.05)
    pytest.fail(f"task never completed; last state={state}")


@pytest.mark.asyncio
async def test_cancel_and_errors(client):
    sent = await _rpc(client, "message/send", {
        "message": {"role": "user", "parts": [{"type": "text", "text": "可取消任务"}]},
    })
    task_id = sent["result"]["task"]["id"]
    canceled = await _rpc(client, "tasks/cancel", {"taskId": task_id})
    assert canceled["result"]["task"]["state"] == "canceled"

    unknown = await _rpc(client, "tasks/get", {"taskId": "t_nope"})
    assert unknown["error"]["code"] == -32602

    bad = await _rpc(client, "no/such-method", {})
    assert bad["error"]["code"] == -32601


@pytest.mark.asyncio
async def test_empty_intent_rejected(client):
    got = await _rpc(client, "message/send", {
        "message": {"role": "user", "parts": [{"type": "text", "text": ""}]},
    })
    assert got["error"]["code"] == -32602
