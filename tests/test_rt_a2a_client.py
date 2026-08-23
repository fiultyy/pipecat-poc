#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""A2A client conformance: against the python fake server (offline) and the
real node plugin (cross-language; skipped when node is absent)."""

import asyncio
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest
import pytest_asyncio

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "realtime-provider-poc"))

from fake_a2a_server import FakeTaskStore, build_app  # noqa: E402
from rt_a2a_client import A2aClient, A2aError  # noqa: E402

PLUGIN_DIR = Path("~/.dsh/plugins/a2a-profile-server").expanduser()

GOOD_MD = """# AGENTS.md

> 探索型助手。

## Agent Behavior
快。

### Mission
1. 做

### How you work
- 步

### MUST
- 证据

### MUST NOT
- 禁止删除生产数据或泄露敏感凭据

### Style
简。

### Output
- 物
"""


@pytest_asyncio.fixture
async def fake_client():
    from aiohttp.test_utils import TestClient, TestServer

    server = TestServer(build_app(FakeTaskStore(complete_s=0.05)))
    client = TestClient(server)
    await client.start_server()
    yield A2aClient(str(client.make_url("")))
    await client.close()


# ---- against the python fake server ----

@pytest.mark.asyncio
async def test_send_and_await_done(fake_client):
    card = await fake_client.agent_card()
    assert card["version"] == "internal-1"
    task_id = await fake_client.send("调研 WebGPU", ref="vh-t1")
    artifact = await fake_client.await_done(task_id, timeout_s=5)
    assert artifact.startswith('"Agent Final Message":')
    assert "【凭证FAKE-" in artifact


@pytest.mark.asyncio
async def test_cancel_and_error(fake_client):
    task_id = await fake_client.send("取消我")
    assert await fake_client.cancel(task_id) == "canceled"
    with pytest.raises(A2aError) as ei:
        await fake_client.get("t_nope")
    assert ei.value.code == -32602


# ---- against the real node plugin (cross-language conformance) ----

def _node_available() -> bool:
    return bool(shutil.which("node")) and PLUGIN_DIR.joinpath("index.js").exists()


@pytest_asyncio.fixture
async def plugin_server(tmp_path):
    """Boot the real plugin on a fixed port in a node subprocess."""
    if not _node_available():
        pytest.skip("node or a2a-profile-server plugin absent")
    port = 8797
    state_dir = tmp_path / "state"
    profile_root = tmp_path / "profiles"
    script = (
        "import('./index.js').then(async (m) => {"
        f"const h = await m.activate({{port: {port}, "
        f"profileRoot: '{profile_root}', stateDir: '{state_dir}', token: 'conf-tok'}});"
        "console.log('READY ' + h.port);})"
    )
    proc = subprocess.Popen(
        ["node", "--input-type=module", "-e", script],
        cwd=PLUGIN_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    deadline = time.time() + 10
    ready = False
    while time.time() < deadline:
        line = proc.stdout.readline().decode()
        if "READY" in line:
            ready = True
            break
    if not ready:
        proc.kill()
        pytest.fail("plugin did not become ready in 10s")
    yield A2aClient(f"http://127.0.0.1:{port}", token="conf-tok"), profile_root
    proc.kill()


@pytest.mark.asyncio
async def test_node_plugin_conformance(plugin_server):
    client, _ = plugin_server
    card = await client.agent_card()
    assert card["name"] == "voice-head-orchestrator"

    task_id = await client.send("跨语言一致性检查", ref="conf-1")
    artifact = await client.await_done(task_id, timeout_s=10)
    assert artifact.startswith('"Agent Final Message":') and "【凭证A2A-" in artifact


@pytest.mark.asyncio
async def test_node_plugin_incubate(plugin_server):
    client, profile_root = plugin_server
    result = await client.incubate(
        name="conformance-probe",
        agents_md=GOOD_MD,
        targets=["dry"],
        profile_json={"scenario": "一致性探针"},
    )
    assert result["profile"]["version"] == 1
    assert result["receipts"][0]["target"] == "dry"
    saved = (profile_root / "conformance-probe" / "AGENTS.md").read_text(encoding="utf-8")
    assert saved.startswith("# AGENTS.md")


@pytest.mark.asyncio
async def test_node_plugin_auth_enforced(plugin_server):
    client, _ = plugin_server
    bad = A2aClient(client.base_url)  # no token
    with pytest.raises(A2aError):
        await bad.send("不应通过")
