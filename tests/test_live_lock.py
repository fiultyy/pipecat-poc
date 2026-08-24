"""Unit + two-process drills for the live lease (tests/live_lock.py).

Covers: cross-process mutual exclusion, skip-mode downgrade, loud wait
warning, and a synthetic two-pytest drill proving marked cases serialize on
a shared domain (the D-13 failure mode: two concurrent runs trampling the
dais bus / a mailbox).
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from live_lock import LiveLockUnavailable, live_lease

DOMAIN = "drill-domain"
REPO = Path(__file__).resolve().parent.parent


def _holder(domain: str, hold_s: float) -> subprocess.Popen:
    """Start a helper process that holds the lease for ``hold_s`` seconds."""
    code = textwrap.dedent(
        f"""
        import sys, time
        sys.path.insert(0, {str(REPO / "tests")!r})
        from live_lock import live_lease
        with live_lease({domain!r}):
            print("held", flush=True)
            time.sleep({hold_s})
        """
    )
    proc = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, text=True)
    assert proc.stdout is not None
    assert proc.stdout.readline().strip() == "held"  # lease really acquired
    return proc


def test_lease_serializes_two_processes():
    holder = _holder(DOMAIN, 1.5)
    try:
        started = time.monotonic()
        with live_lease(DOMAIN, warn_after_s=30.0):
            waited = time.monotonic() - started
        assert waited >= 0.6, f"second acquirer did not wait for the holder ({waited:.2f}s)"
    finally:
        holder.wait(timeout=10)


def test_skip_mode_raises_immediately_when_held():
    holder = _holder(DOMAIN, 1.5)
    try:
        started = time.monotonic()
        with pytest.raises(LiveLockUnavailable, match=DOMAIN):
            with live_lease(DOMAIN, mode="skip"):
                pass
        waited = time.monotonic() - started
        assert waited < 0.8, f"skip mode waited {waited:.2f}s instead of bailing out"
    finally:
        holder.wait(timeout=10)


def test_blocking_wait_warns_loudly(capsys):
    holder = _holder(DOMAIN, 1.2)
    try:
        with live_lease(DOMAIN, warn_after_s=0.3, poll_s=0.1):
            err = capsys.readouterr().err
        assert DOMAIN in err and "WARNING" in err, f"no loud warning emitted: {err!r}"
    finally:
        holder.wait(timeout=10)


def test_release_allows_immediate_reacquire():
    with live_lease(DOMAIN):
        pass
    started = time.monotonic()
    with live_lease(DOMAIN):
        pass
    assert time.monotonic() - started < 1.0


def test_unknown_mode_rejected():
    with pytest.raises(ValueError, match="block\\|skip"):
        with live_lease(DOMAIN, mode="yolo"):
            pass


# ---- synthetic two-pytest drill: marked cases serialize on one domain ----

_DRILL_CASE = textwrap.dedent(
    """
    import sys, time
    sys.path.insert(0, {tests!r})
    import pytest

    @pytest.mark.live({domain!r})
    def test_occupied():
        time.sleep(1.2)
    """
)


def _drill_case_path(tmp_path: Path, name: str) -> Path:
    """Write the drill case inside tests/ so the repo conftest hooks apply.

    The live marker is only honored for cases collected under ``tests/``
    (that is where ``pytest_runtest_setup`` lives), so the drill writes a
    throwaway file next to it and removes it afterwards.
    """
    case = REPO / "tests" / name
    case.write_text(_DRILL_CASE.format(tests=str(REPO / "tests"), domain=DOMAIN), encoding="utf-8")
    return case


def test_two_pytest_processes_serialize(tmp_path):
    """Two concurrent pytest runs on one domain must not overlap execution."""
    case = _drill_case_path(tmp_path, "test_drill_tmp.py")
    try:
        cmd = [sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "-q", "tests/test_drill_tmp.py"]
        started = time.monotonic()
        procs = [subprocess.Popen(cmd, cwd=REPO, stdout=subprocess.DEVNULL) for _ in range(2)]
        for proc in procs:
            assert proc.wait(timeout=60) == 0
        elapsed = time.monotonic() - started
    finally:
        case.unlink(missing_ok=True)
    assert elapsed >= 2.0, (
        f"two 1.2s cases finished in {elapsed:.2f}s — they overlapped, the lease "
        "did not serialize them"
    )


def test_skip_env_downgrades_to_skip(tmp_path):
    """DSH_LIVE_LOCK=skip makes a marked case skip (not wait) when held."""
    case = _drill_case_path(tmp_path, "test_drill_skip_tmp.py")
    holder = _holder(DOMAIN, 1.5)
    try:
        import os

        env = {**os.environ, "DSH_LIVE_LOCK": "skip"}
        proc = subprocess.run(
            [sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "-q", "-rs",
             "tests/test_drill_skip_tmp.py"],
            cwd=REPO, env=env, capture_output=True, text=True, timeout=60,
        )
    finally:
        holder.wait(timeout=10)
        case.unlink(missing_ok=True)
    assert proc.returncode == 0
    assert "1 skipped" in proc.stdout, proc.stdout
    assert "live lease unavailable" in proc.stdout, proc.stdout
