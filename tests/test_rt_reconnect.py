#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Unit tests for the reconnect backoff state machine."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "realtime-provider-poc"))

from rt_reconnect import ReconnectPolicy, ReconnectState, run_with_reconnect


def test_backoff_growth_and_cap():
    st = ReconnectState(policy=ReconnectPolicy(base_delay=0.2, max_delay=5.0))
    d1 = st.on_failure()
    d2 = st.on_failure()
    d3 = st.on_failure()
    d6 = None
    for _ in range(3):
        d6 = st.on_failure()
    assert (d1, d2, d3) == (0.2, 0.4, 0.8)
    assert d6 == 5.0, f"cap not applied: {d6}"


def test_gives_up_after_max_attempts():
    st = ReconnectState(policy=ReconnectPolicy(max_attempts=3))
    st.on_failure()
    st.on_failure()
    st.on_failure()
    assert st.on_failure() is None


def test_stable_window_resets_counter():
    st = ReconnectState(policy=ReconnectPolicy(base_delay=0.2, stable_after=0.0))
    st.on_failure()
    st.on_failure()
    st.on_connected()
    # connection held >= stable_after (0.0 here), so counter resets
    d = st.on_failure()
    assert d == 0.2, f"counter not reset: {d}"


def test_unstable_connection_keeps_counter():
    st = ReconnectState(policy=ReconnectPolicy(base_delay=0.2, stable_after=30.0))
    st.on_failure()
    st.on_connected()
    # simulate instant death: held time ~0 < 30s -> counter preserved
    st._connected_at = st._connected_at - 0.0
    d = st.on_failure()
    assert d == 0.4, f"counter incorrectly reset: {d}"


def test_run_with_reconnect_reseeds_and_stops():
    events: list[str] = []
    attempts = {"n": 0}
    alive = {"v": False}

    async def connect():
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("boom")  # first attempt fails
        alive["v"] = True
        events.append(f"connected#{attempts['n']}")

    async def on_reconnect():
        events.append("reseed")

    def is_connected():
        return alive["v"]

    async def fast_sleep(t):
        events.append(f"sleep:{t}")

    async def main():
        # stop after the reseed of the second successful connect
        async def stop():
            return "reseed" in events

        await run_with_reconnect(
            connect,
            is_connected,
            on_reconnect=on_reconnect,
            policy=ReconnectPolicy(base_delay=0.01, max_delay=0.02, stable_after=1.0),
            sleep=fast_sleep,
            stop=lambda: len([e for e in events if e == "reseed"]) >= 1,
        )

    asyncio.run(main())
    assert "connected#2" in events
    assert "reseed" in events
    assert "sleep:0.01" in events  # backoff sleep happened after failure


if __name__ == "__main__":
    fns = [v for k, v in list(globals().items()) if k.startswith("test_")]
    failed = 0
    for fn in fns:
        try:
            fn()
            print(f"  [PASS] {fn.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"  [FAIL] {fn.__name__}: {e}")
    sys.exit(1 if failed else 0)
