#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Live V5/V6 wrapper (W1.6): runs live_v5_v6_dsh.py as a subprocess and
asserts both verdicts. Requires GLM credentials + the real dais binary;
skips otherwise. GLM text mode per the Q2 decision."""

import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

HERE = Path(__file__).parent.parent / "examples" / "realtime-provider-poc"
DAIS_BIN = Path("~/.local/bin/dais").expanduser()


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
@pytest.mark.skipif(not _dais_plane_up(), reason="dais orchestration plane down (resident app not running)")
def test_live_v5_v6():
    proc = subprocess.run(
        [sys.executable, str(HERE / "live_v5_v6_dsh.py")],
        capture_output=True, text=True, timeout=600,
    )
    out = proc.stdout + proc.stderr
    assert "V5 PASS" in out, f"V5 failed:\n{out[-2000:]}"
    assert "V6 PASS" in out, f"V6 failed:\n{out[-2000:]}"
    assert "[SKIP]" not in out, f"bus wedged:\n{out[-500:]}"
