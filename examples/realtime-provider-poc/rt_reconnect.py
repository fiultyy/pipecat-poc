#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Reconnect backoff state machine for realtime sessions.

Port of Codex's sideband reconnect loop (codex-rs/core/src/realtime_
conversation/sideband.rs): exponential backoff with jitter-free base
growth (200ms -> 5s cap), a stability window that resets the failure
counter after the connection has held for 30s, and a hook for
re-seeding conversation context on every fresh connection — the only
way to restore context on DashScope, whose session history cannot be
read back or edited.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable

RECONNECT_BASE_DELAY = 0.2  # seconds
RECONNECT_MAX_DELAY = 5.0
STABLE_CONNECTION_DURATION = 30.0  # failures before this window reset the counter


@dataclass
class ReconnectPolicy:
    """Tunable backoff parameters (tests shrink these)."""

    base_delay: float = RECONNECT_BASE_DELAY
    max_delay: float = RECONNECT_MAX_DELAY
    stable_after: float = STABLE_CONNECTION_DURATION
    max_attempts: int = 0  # 0 = unlimited


@dataclass
class ReconnectState:
    """Tracks consecutive failures and computes delays.

    Feed ``on_connected()`` once a connection is established and
    ``on_failure()`` on each loss; ``next_delay()`` gives the wait before
    the next attempt (None when the policy gives up).
    """

    policy: ReconnectPolicy = field(default_factory=ReconnectPolicy)
    consecutive_failures: int = 0
    _connected_at: float | None = None

    def on_connected(self) -> None:
        self._connected_at = time.monotonic()

    def on_failure(self) -> float | None:
        """Record a failure; return the delay before the next attempt."""
        if self._connected_at is not None:
            held = time.monotonic() - self._connected_at
            if held >= self.policy.stable_after and self.consecutive_failures > 0:
                self.consecutive_failures = 0  # stable window: forgive history
        self._connected_at = None
        self.consecutive_failures += 1
        return self.next_delay()

    def next_delay(self) -> float | None:
        if self.policy.max_attempts and self.consecutive_failures > self.policy.max_attempts:
            return None
        delay = self.policy.base_delay * (2 ** (self.consecutive_failures - 1))
        return min(delay, self.policy.max_delay)


async def run_with_reconnect(
    connect: Callable[[], Awaitable],
    is_connected: Callable[[], bool],
    on_reconnect: Callable[[], Awaitable[None]] | None = None,
    policy: ReconnectPolicy | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    stop: Callable[[], bool] | None = None,
) -> None:
    """Keep a realtime connection alive, reconnecting with backoff.

    Args:
        connect: coroutine establishing the connection; raises on failure.
        is_connected: cheap liveness probe checked between attempts.
        on_reconnect: called after every successful re-connect (e.g. re-seed
            the conversation transcript into the fresh session).
        policy: backoff tuning; defaults to production values.
        sleep: injectable sleeper (tests pass a no-op).
        stop: optional predicate; when true the loop exits cleanly.
    """
    state = ReconnectState(policy or ReconnectPolicy())
    while not (stop and stop()):
        try:
            await connect()
            state.on_connected()
            if on_reconnect is not None:
                await on_reconnect()
            # Hold this connection until it dies (probe returns False).
            while is_connected() and not (stop and stop()):
                await sleep(0.5)
        except Exception:
            pass
        if stop and stop():
            break
        delay = state.on_failure()
        if delay is None:
            break
        await sleep(delay)
