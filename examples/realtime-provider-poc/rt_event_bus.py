#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""EventBus: in-process pub/sub for orchestration events (WS1 §6; WS4 §2).

The gateway (WS4) subscribes and forwards every event as a ws ``event``
frame — this is the "orchestrator information also flows over ws" channel.
"""

from __future__ import annotations

import time
from collections import defaultdict
from typing import Awaitable, Callable

Listener = Callable[[str, dict], Awaitable[None]]


class EventBus:
    """Minimal async fan-out bus; subscribe() returns an unsubscribe fn."""

    def __init__(self) -> None:
        self._listeners: dict[str, list[Listener]] = defaultdict(list)

    def subscribe(self, callback: Listener, *kinds: str) -> Callable[[], None]:
        """Register a listener; no kinds = all events."""
        entry = (callback, kinds)
        self._listeners["*"].append(entry) if not kinds else None
        for kind in kinds or ["*"]:
            self._listeners[kind].append(entry)

        def unsubscribe() -> None:
            for kind in kinds or ["*"]:
                try:
                    self._listeners[kind].remove(entry)
                except ValueError:
                    pass

        return unsubscribe

    async def emit(self, kind: str, payload: dict) -> None:
        """Fire-and-forget fan-out; listener errors are swallowed (log-only)."""
        event = dict(payload)
        event.setdefault("ts", time.time())
        for entry in [*self._listeners.get(kind, []), *self._listeners.get("*", [])]:
            callback = entry[0]
            try:
                await callback(kind, event)
            except Exception as e:  # noqa: BLE001 — one bad sink must not kill the bus
                import logging

                logging.getLogger(__name__).warning("event listener failed: %s", e)
