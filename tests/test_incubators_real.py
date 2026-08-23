#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""W2.3 real-incubator smoke: one true agent per target (dsh/omp/claude).

Runs against the real plugin (node subprocess, temp profile root) with the
REAL incubators — dsh spawns a live session via session-spawn, omp edits
the real oh-my-opencode.json (with timestamped backup), claude writes a
real ~/.claude/agents/<name>.md. Cleanup restores/removes what it created.
"""

import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "realtime-provider-poc"))

from rt_a2a_client import A2aClient  # noqa: E402
from rt_projection_gates import run_gates  # noqa: E402
from rt_projector import Projector  # noqa: E402

PLUGIN_DIR = Path("~/.dsh/plugins/a2a-profile-server").expanduser()
HOME = Path.home()
PROBE = "vh-smoke-probe"

GOOD_MD = """# AGENTS.md

> 语音编排监督探针 agent。

## Agent Behavior
监控 fan-out 并汇报。

### Mission
1. 观察 2. 汇报

### How you work
- 周期巡检

### MUST
- 汇报带证据

### MUST NOT
- 禁止删除生产数据或泄露敏感凭据；不可逆动作必须先确认

### Style
简洁。

### Output
- 状态行
"""

pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="module")
def plugin(tmp_path_factory):
    if not PLUGIN_DIR.joinpath("incubators/real.js").exists():
        pytest.skip("a2a-profile-server plugin absent")
    tmp = tmp_path_factory.mktemp("w23-smoke")
    port = 8798
    script = (
        "import('./index.js').then(async (m) => {"
        f"const h = await m.activate({{port: {port}, profileRoot: '{tmp}/profiles', "
        f"stateDir: '{tmp}/state'}});"
        "console.log('READY ' + h.port);})"
    )
    proc = subprocess.Popen(
        ["node", "--input-type=module", "-e", script],
        cwd=PLUGIN_DIR, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    deadline = time.time() + 10
    while time.time() < deadline:
        if b"READY" in proc.stdout.readline():
            break
    else:
        proc.kill()
        pytest.fail("plugin not ready")
    yield {"client": A2aClient(f"http://127.0.0.1:{port}"), "tmp": tmp}
    proc.kill()


async def _cleanup_before():
    # claude: remove previous probe file if any
    agent_file = HOME / ".claude/agents" / f"{PROBE}.md"
    agent_file.unlink(missing_ok=True)


async def test_omp_incubation_real(plugin, tmp_path):
    await _cleanup_before()
    project = tmp_path / "omp-project"
    result = await plugin["client"].incubate(
        name=PROBE,
        agents_md=GOOD_MD,
        targets=["omp"],
        profile_json={"scenario": "冒烟"},
    )
    receipt = result["receipts"][0]
    assert receipt["target"] == "omp", receipt
    # 真实验证 1：oh-my-opencode.json 注册了 agent
    omp = json.loads((HOME / ".config/opencode/oh-my-opencode.json").read_text())
    assert PROBE in omp.get("agents", {}), "omp agent not registered"
    assert Path(receipt["configBackup"]).exists(), "backup not written"
    # 真实验证 2：三门必须先过（AGENTS.md 内容 = 投影产物 + 反向索引尾注）
    saved = (project.parent / "omp-project")  # projectRoot default = plugin cwd; check receipt path instead
    agents_path = Path(receipt["project"]) / "AGENTS.md"
    assert agents_path.exists()
    content = agents_path.read_text(encoding="utf-8")
    assert content.startswith("# AGENTS.md")
    assert f"x-profile-ref: {PROBE}@v1" in content
    # 还原 omp 配置（用备份）
    shutil.copy(receipt["configBackup"], HOME / ".config/opencode/oh-my-opencode.json")


async def test_claude_incubation_real(plugin):
    result = await plugin["client"].incubate(
        name=PROBE,
        agents_md=GOOD_MD,
        targets=["claude"],
        profile_json={"scenario": "冒烟"},
        description="语音编排冒烟探针：验证孵化链路",
    )
    receipt = result["receipts"][0]
    assert receipt["target"] == "claude", receipt
    path = Path(receipt["path"])
    assert path.exists()
    text = path.read_text(encoding="utf-8")
    # frontmatter 形制对齐 research-analyst.md
    assert text.startswith("---\n")
    assert f"name: {PROBE}" in text
    assert "description:" in text and "model:" in text and "color:" in text
    assert "语音编排冒烟探针" in text
    assert "x-profile-ref:" in text
    # 三门对正文（去 frontmatter 与尾注）也应通过
    body = text.split("---\n", 2)[2].rsplit("<!-- x-profile-ref", 1)[0]
    assert run_gates(body).passed, "incubated body must pass gates"
    path.unlink()  # cleanup


async def test_dsh_incubation_real(plugin):
    result = await plugin["client"].incubate(
        name=PROBE,
        agents_md=GOOD_MD,
        targets=["dsh"],
        profile_json={"scenario": "冒烟"},
    )
    receipt = result["receipts"][0]
    assert receipt["target"] == "dsh", receipt
    assert receipt["code"] and len(receipt["code"]) == 4, receipt
    # 真实验证：fleet.json 里有该 code 的登记（session-spawn 的原子写）
    fleet = json.loads((HOME / ".dsh/maestro/fleet.json").read_text())
    fleet_codes = {k: v for k, v in fleet.get("fleet", {}).items()}
    assert any(
        code == receipt["code"] or (ent.get("sessionId", "").startswith("session-" + receipt["code"]))
        for code, ent in fleet_codes.items()
    ), f"code {receipt['code']} not in fleet.json"
