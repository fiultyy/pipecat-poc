#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""DshBackend: head tool semantics over the two lanes (WS1; docs/kg/
01-ws1-head-dsh.md §1/§5).

Two-phase response contract (docs/kg/05-contracts.md §2):

- phase 1 (immediate): ``{"status":"accepted", run_id, tickets, credentials}``
- phase 2 (on done):   ``"Agent Final Message":\n\n<done body>`` re-injected
  into the head context via the pending-result callback.

Interrupt semantics: local playback interruption never cancels the remote
run; explicit ``cancel()`` does.
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from typing import Awaitable, Callable

from rt_a2a_client import A2aClient
from rt_dsh_lane import DaisLane
from rt_event_bus import EventBus
from rt_orchestrator import FINAL_PREFIX, BackendResult, make_credential

CLARIFY_NOTE = '{"status": "clarify", "note": "无法拆解该意图，请补充信息"}'


@dataclass
class DshDispatch:
    """Phase-1 acceptance receipt."""

    run_id: str | None
    task_id: str | None
    ref: str
    credentials: list[str] = field(default_factory=list)
    intent_seq: int = -1


@dataclass
class DshBackend:
    """Implements the head tool surface: dispatch / query_status / cancel.

    Args:
        lane: dais CLI lane (lane B) — required for status/cancel even
            in lane-A mode.
        lane_a: optional A2A client; with ``lane_mode="a"`` dispatch goes
            through it (message/send) instead of the dais CLI.
        lane_mode: "b" (dais CLI, default) or "a" (A2A plugin lane).
        bus: event bus for WS4 forwarding.
        orchestrator_handle: dais handle of the orchestrator session
            (``session_<sid>``); the intent is addressed to it and done
            replies are expected from it.
        head_handle: the head's own mailbox handle (default
            ``voice-head``); phase-2 polls HERE — replies are addressed
            to the intent's sender.
        on_final: callback invoked with the phase-2 final message (the
            head pipeline wires this to context re-injection).
        await_timeout_s: phase-2 blocking budget per dispatch.
    """

    lane: DaisLane
    lane_a: A2aClient | None = None
    lane_mode: str = "b"
    bus: EventBus = field(default_factory=EventBus)
    orchestrator_handle: str = ""
    head_handle: str = "voice-head"
    on_final: Callable[[str, str], Awaitable[None]] | None = None
    await_timeout_s: float = 1800.0
    poll_s: float = 2.0
    _runs: dict[str, DshDispatch] = field(default_factory=dict)
    _pending: dict[str, asyncio.Task] = field(default_factory=dict)

    # ---- head tool 1: dispatch_intent ----

    async def dispatch(self, raw_intent: str) -> str:
        """Phase 1 acceptance now; phase 2 final re-injected on completion."""
        ref = "vh-" + uuid.uuid4().hex[:8]
        dispatch = await self._fanout(raw_intent, ref)

        if dispatch is None:
            return CLARIFY_NOTE

        receipt = {
            "status": "accepted",
            "run_id": dispatch.run_id,
            "ref": dispatch.ref,
            "credentials": dispatch.credentials,
            "note": "已受理，完成后播报",
        }
        self._pending[ref] = asyncio.create_task(self._phase2(ref, dispatch))
        import json

        return json.dumps(receipt, ensure_ascii=False)

    async def _fanout(self, raw_intent: str, ref: str) -> DshDispatch | None:
        """Create run + intent message; returns None when unsplittable."""
        if self.lane_mode == "a" and self.lane_a is not None:
            task_id = await self.lane_a.send(raw_intent, ref)
            credential = make_credential(ref.upper())
            dispatch = DshDispatch(run_id=task_id, task_id=None, ref=ref,
                                   credentials=[credential])
            self._runs[ref] = dispatch
            await self.bus.emit("orch.dispatch", {
                "run_id": None, "task_id": task_id, "ref": ref,
                "credentials": dispatch.credentials, "lane": "a",
            })
            return dispatch
        run_id = await self.lane.create_run(f"[voice-head] {raw_intent[:200]}")
        intent_seq = -1
        if not self.orchestrator_handle:
            # no orchestrator session yet — the run itself carries the intent
            task_id = await self.lane.create_task(run_id, raw_intent[:2000])
        else:
            intent_seq = await self.lane.send_intent(
                run_id, self.orchestrator_handle, raw_intent, ref,
                from_id=self.head_handle,
            )
            task_id = None
        credential = make_credential(ref.upper())
        dispatch = DshDispatch(run_id=run_id, task_id=task_id, ref=ref,
                               credentials=[credential], intent_seq=intent_seq)
        self._runs[ref] = dispatch
        await self.bus.emit("orch.dispatch", {
            "run_id": run_id, "task_id": task_id, "ref": ref,
            "credentials": dispatch.credentials, "lane": "b",
        })
        return dispatch

    async def _phase2(self, ref: str, dispatch: DshDispatch) -> None:
        """Await the done body and re-inject the final message.

        Transient lane errors (``database is locked`` under cross-process
        contention with the resident daemon) are retried until the
        overall budget expires; only a clean TimeoutError surfaces as
        "still running".
        """
        from rt_dsh_lane import DaisLaneError

        deadline = asyncio.get_event_loop().time() + self.await_timeout_s
        body: str | None = None
        while body is None:
            budget = max(1.0, deadline - asyncio.get_event_loop().time())
            try:
                if (self.lane_a is not None and dispatch.run_id
                        and dispatch.run_id.startswith("t_")):
                    body = await self.lane_a.await_done(dispatch.run_id, timeout_s=budget)
                    continue
                # Poll the HEAD's own mailbox: replies are addressed to the
                # intent's sender; polling the orchestrator handle would
                # self-match the intent we sent (live-probed 2026-08-23).
                body = await self.lane.await_done(
                    self.head_handle, ref, timeout_s=budget,
                    poll_s=self.poll_s,
                    after_seq=dispatch.intent_seq if dispatch.intent_seq >= 0 else None,
                    from_filter=self.orchestrator_handle or None,
                )
            except TimeoutError:
                await self.bus.emit("orch.progress", {"ref": ref, "note": "still running"})
                return
            except DaisLaneError:
                if asyncio.get_event_loop().time() >= deadline:
                    await self.bus.emit("orch.progress", {"ref": ref, "note": "lane errors until deadline"})
                    return
                await asyncio.sleep(min(self.poll_s, 1.0))
        if body.startswith(FINAL_PREFIX):
            body = body[len(FINAL_PREFIX):]
        final = f"{FINAL_PREFIX}{body}"
        await self.bus.emit("orch.done", {"ref": ref, "run_id": dispatch.run_id, "artifact": body})
        if self.on_final:
            await self.on_final(ref, final)

    # ---- head tool 2: query_status ----

    async def query_status(self, run_id: str | None = None) -> str:
        """Spoken-friendly aggregation of dais status (+ pending refs)."""
        status = await self.lane.check_status(run_id)
        lines = []
        for entry in status.get("entries", []):
            if "tasks" in entry:
                lines.append(f"{entry['id']}：{entry['tasks']} 个任务")
            else:
                lines.append(f"{entry['id']}：{entry.get('summary', '')}")
        pending = [d.ref for d in self._runs.values() if d.ref in self._pending
                   and not self._pending[d.ref].done()]
        if pending:
            lines.append(f"语音头待收终稿 {len(pending)} 项")
        import json

        return json.dumps({"runs": lines, "pending": pending}, ensure_ascii=False)

    # ---- head tool 3: cancel ----

    async def cancel(self, ref_or_run: str) -> str:
        """Cancel by ref (vh-…) or run_id; kills pending phase-2 task."""
        dispatch = self._runs.get(ref_or_run)
        run_id = dispatch.run_id if dispatch else ref_or_run
        task = self._pending.get(ref_or_run) or self._pending.get(run_id)
        if task and not task.done():
            task.cancel()
        if self.lane_a is not None and run_id.startswith("t_"):
            state = await self.lane_a.cancel(run_id)
        else:
            # fail-dispatch expects a ctx_ dispatch handle and rejects
            # run/task ids (exit 1 "not found", live-probed) — the local
            # pending-task kill above is the authoritative cancellation
            if run_id.startswith("ctx_"):
                await self.lane.fail_dispatch(run_id, "cancelled by voice user")
            state = "canceled"
        await self.bus.emit("orch.done", {"ref": ref_or_run, "run_id": run_id, "artifact": "(已取消)"})
        import json

        return json.dumps({"status": state, "run_id": run_id}, ensure_ascii=False)

    # ---- PoC BackendFn adapter (keeps Orchestrator unit-test compatible) ----

    def as_backend_fn(self):
        """Wrap as ``(agent, goal) -> BackendResult`` for Orchestrator."""

        async def backend_fn(agent: str, goal: str) -> BackendResult:
            receipt = await self.dispatch(f"[{agent}] {goal}")
            import json

            data = json.loads(receipt)
            return BackendResult(agent=agent, finding=receipt,
                                 canary=(data.get("credentials") or [None])[0])

        return backend_fn
