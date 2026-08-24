#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Live V5/V6 wrapper (W1.6 / VO-006): runs live_v5_v6_dsh.py as a
subprocess and asserts both verdicts — now against the REAL dsh-liaison
(incubated through the plugin; orchestrator_handle is a config value, head
logic untouched). Requires GLM credentials + the real dais binary + the
a2a-profile-server plugin; skips otherwise. GLM text mode per the Q2
decision.

VO-006 additions:

- ``test_live_v5_v6`` also asserts the incubation receipt trail (code /
  mailbox / role / project / fleet five-key registration) parsed from the
  live output and cross-checked against fleet.json.
- ``test_zero_diff_head_side`` pins acceptance ③: none of the head-side
  logic files (backend / lane / tools / orchestrator / src/pipecat) may
  carry a working-tree diff — the stand-in → real swap must be a pure
  configuration change (KG 06 §3.1 four-row checklist).
"""

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).parent.parent / "examples" / "realtime-provider-poc"
REPO = Path(__file__).parent.parent
DAIS_BIN = Path("~/.local/bin/dais").expanduser()
PLUGIN_DIR = Path("~/.dsh/plugins/a2a-profile-server").expanduser()
FLEET_PATH = Path("~/.dsh/maestro/fleet.json").expanduser()

# head-side logic files: acceptance ③ — zero diff (config-only swap)
HEAD_SIDE_FILES = [
    str(REPO / "src/pipecat"),
    str(HERE / "rt_dsh_backend.py"),
    str(HERE / "rt_dsh_lane.py"),
    str(HERE / "rt_head_tools.py"),
    str(HERE / "rt_orchestrator.py"),
]


def _ready() -> bool:
    if not DAIS_BIN.exists():
        return False
    sys.path.insert(0, str(HERE))
    from rt_env import glm_credentials

    try:
        key, _ = glm_credentials()
        return bool(key)
    except Exception:
        return False


def _plugin_ok() -> bool:
    """Real incubation needs node + the plugin entry (incubate RPC)."""
    return bool(shutil.which("node")) and PLUGIN_DIR.joinpath("index.js").exists()


def _dais_plane_up() -> bool:
    """The orchestration plane lives in the resident dais app; without it the
    CLI exits 1 ("orchestration is not enabled in this build") and the live
    script would only emit [SKIP]. Probe once up front so the suite treats a
    down plane as an environment skip, not a failure."""
    try:
        return subprocess.run(
            [str(DAIS_BIN), "orchestration", "check-status"],
            capture_output=True, timeout=30,
        ).returncode == 0
    except Exception:
        return False


@pytest.mark.skipif(not _ready(), reason="GLM creds or dais absent")
@pytest.mark.skipif(not _plugin_ok(), reason="node or a2a-profile-server plugin absent")
@pytest.mark.skipif(not _dais_plane_up(), reason="dais orchestration plane down (resident app not running)")
@pytest.mark.live("dais-bus")
def test_live_v5_v6():
    proc = subprocess.run(
        [sys.executable, str(HERE / "live_v5_v6_dsh.py")],
        capture_output=True, text=True, timeout=900,
    )
    out = proc.stdout + proc.stderr
    assert "V5 PASS" in out, f"V5 failed:\n{out[-2000:]}"
    assert "V6 PASS" in out, f"V6 failed:\n{out[-2000:]}"
    assert "[SKIP]" not in out, f"bus wedged:\n{out[-500:]}"

    # ---- VO-006: real incubation trail (mailbox=agent_liaison, fleet keys) ----
    m = re.search(r"liaison code=([0-9a-f]{4}) mailbox=agent_liaison ", out)
    assert m, f"no real-liaison registration line in live output:\n{out[-800:]}"
    code = m.group(1)
    fleet = json.loads(FLEET_PATH.read_text())["fleet"][code]
    assert fleet["sessionId"].startswith(f"session-{code}")
    assert fleet["mailbox"] == "agent_liaison"
    assert fleet["role"] == "liaison"
    assert fleet["project"] == "voice-head"
    assert "profile_version" in fleet

    # doctrine behavioral verdict emitted by the live script (first action
    # of the wakeup turn = mailbox snapshot drain)
    assert "[PASS] liaison 回合首动作" in out, f"doctrine first-action check failed:\n{out[-800:]}"
    # incubation + fleet verdicts
    assert "[PASS] 孵化回执（真身 dsh-liaison）" in out
    assert "[PASS] fleet 五键登记" in out
    # two-phase timing verdict
    assert "[PASS] phase-1 阶段终稿未到（两阶段时序）" in out


def test_zero_diff_head_side():
    """Acceptance ③ (KG 06 §3.1): swapping the stand-in for the real liaison
    must leave every head-side logic file untouched — orchestrator_handle is
    a configuration value, nothing else. Controlled-workspace assumption:
    the tree carries no unrelated in-progress edits to these files."""
    r = subprocess.run(
        ["git", "diff", "--name-only", "HEAD", "--", *HEAD_SIDE_FILES],
        cwd=REPO, capture_output=True, text=True, timeout=30,
    )
    assert r.returncode == 0, r.stderr
    drifted = r.stdout.strip()
    assert not drifted, f"head-side logic drifted (must be config-only):\n{drifted}"
