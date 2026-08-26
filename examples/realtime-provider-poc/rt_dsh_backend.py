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
import json
import os
import sys
import time
import urllib.request
import uuid
from dataclasses import dataclass, field
from typing import Awaitable, Callable

from rt_a2a_client import A2aClient, A2aError
from rt_dsh_lane import DaisLane
from rt_event_bus import EventBus
from rt_orchestrator import FINAL_PREFIX, BackendResult, make_credential

CLARIFY_NOTE = '{"status": "clarify", "note": "无法拆解该意图，请补充信息"}'

# lane B-orca worker defaults (live-probed 2026-08-24, orca app 1.4.185 +
# omp on GLM-5.3): the artifact-exactness choreography follows the
# conformance _orca_worker dance (render-proof completion signal).
ORCA_AGENT_DEFAULT = "omp"
ORCA_WORKER_BUDGET_S = 300.0
ORCA_WAIT_SLICE_MS = 15000
ORCA_TURN_SETTLE_S = 20.0        # output window before the one interrupt+resend


def _report_phase2_death(task: asyncio.Task) -> None:
    """Tripwire: a dead phase-2 means the final is lost with no signal —
    surface it instead of an unretrieved-exception warning."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        print(f"[dsh-backend] phase-2 aborted: {exc!r}",
              file=sys.stderr, flush=True)


def _parse_dag_subtasks(raw: str) -> list:
    """Tolerantly parse a head-authored subtask JSON list.

    Accepts a bare array or a ``{"subtasks"|"tasks"|"items": [...]}``
    wrapper; per item the spec text, dep indices, settlement command,
    and worker session are read from any of their known spellings
    (same tolerance family as ``rt_orchestrator.SplitPlan.subtasks``).
    Returns ``[]`` on any structural failure; items without usable
    spec text are dropped.
    """
    import json

    if isinstance(raw, (list, dict)):
        data = raw
    else:
        try:
            data = json.loads(raw)
        except (ValueError, TypeError):
            return []
    if isinstance(data, dict):
        items = next((data[k] for k in ("subtasks", "tasks", "items")
                      if isinstance(data.get(k), list)), None)
    elif isinstance(data, list):
        items = data
    else:
        return []
    out: list[DagTaskSpec] = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        spec = str(item.get("spec") or item.get("goal") or item.get("task")
                   or item.get("description") or "").strip()
        if not spec:
            continue
        deps_raw = item.get("deps") or []
        if not isinstance(deps_raw, list):
            deps_raw = []
        deps = []
        for d in deps_raw:
            try:
                deps.append(int(d))
            except (ValueError, TypeError):
                continue
        command = item.get("command") or item.get("cmd") or None
        session = item.get("session") or item.get("worker") or None
        out.append(DagTaskSpec(spec=spec, deps=deps,
                               command=str(command) if command else None,
                               session=str(session) if session else None))
    return out


@dataclass
class DshDispatch:
    """Phase-1 acceptance receipt."""

    run_id: str | None
    task_id: str | None
    ref: str
    credentials: list[str] = field(default_factory=list)
    intent_seq: int = -1


@dataclass
class DagTaskSpec:
    """One subtask in a dependency-ordered voice dispatch (lane B DAG).

    Args:
        spec: self-contained subtask description.
        deps: indices (into the same dispatch's task list) of prerequisite
            subtasks; a worker starts only after all deps settle
            ``succeeded`` — a failed dep skips the dependent task.
        command: shell block for daemon-driven settlement (exit 0 =
            worker_done succeeded); mutually informative with ``session``.
        session: bind the dispatch to a long-lived worker session's pane
            (``start-worker --session``, dais be8d9cf3) instead of a
            command block.
    """

    spec: str
    deps: list[int] = field(default_factory=list)
    command: str | None = None
    session: str | None = None


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
        poll_max_s: ceiling of the phase-2 poll backoff (consumption
            polls are write transactions on the daemon store; a flat
            cadence starves in-flight senders — see DaisLane.await_done).
        dag_workers: provisioned worker-session mailbox keys
            (``session_<sid>``); DAG tasks that carry no explicit
            ``session`` bind round-robin onto this pool, so the head
            splits semantically while execution binding stays with the
            plane.
        bind_session_id: in-flight dsh session (full ``session_<uuid>``)
            that a dispatch-carried profile is bound to via pool/spawn
            binding-mode (G4 dressing; dsh sessions only).
    """

    lane: DaisLane
    lane_a: A2aClient | None = None
    lane_mode: str = "b"
    lane_orca: object | None = None     # OrcaLane (typed loose: optional dep)
    orca_agent: str = ORCA_AGENT_DEFAULT
    orca_repo: str = ""                 # verbatim selector (path:<p>/id:/name:)
    bus: EventBus = field(default_factory=EventBus)
    orchestrator_handle: str = ""
    head_handle: str = "voice-head"
    on_final: Callable[[str, str], Awaitable[None]] | None = None
    await_timeout_s: float = 1800.0
    poll_s: float = 2.0
    poll_max_s: float = 8.0
    dag_workers: list[str] | None = None
    bind_session_id: str = ""
    # Dedicated liaison session (fleet 4-code or full sessionId): when set,
    # every head tool call is delivered into that session's turn — steer if
    # a turn is in flight (immediate activation), queue otherwise (drives a
    # fresh turn). Finals still return through the voice-head dais mailbox.
    liaison_session: str = ""
    _runs: dict[str, DshDispatch] = field(default_factory=dict)
    _pending: dict[str, asyncio.Task] = field(default_factory=dict)
    _bound: dict[str, dict] = field(default_factory=dict)

    # ---- liaison-session delivery (all head tools → one agent's turn) ----

    async def _dsh_api(self, method: str, payload: dict) -> dict:
        """POST one RPC to the dsh web host loopback API."""

        def _call() -> dict:
            wire = {"type": "client-request", "rpcId": str(uuid.uuid4()),
                    "method": method, "payload": payload}
            req = urllib.request.Request(
                f"http://127.0.0.1:{os.environ.get('DSH_PORT', '3080')}/api/{method}",
                data=json.dumps(wire).encode(),
                headers={"content-type": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                result = json.loads(resp.read())["result"]
            if not result.get("ok"):
                raise RuntimeError(f"{method}: {result.get('error')}")
            return result["value"]

        return await asyncio.to_thread(_call)

    def _liaison_sid(self) -> str:
        """Resolve the configured liaison target to a full sessionId."""
        key = self.liaison_session
        if key.startswith("session-"):
            return key
        with open(os.path.expanduser(
                os.environ.get("MAESTRO_FLEET", "~/.dsh/maestro/fleet.json"))) as fh:
            fleet = json.load(fh)
        entry = fleet.get("fleet", {}).get(key)
        if not entry:
            raise RuntimeError(f"liaison {key!r} not in fleet.json")
        return entry["sessionId"]

    async def _deliver_liaison(self, ref: str, body: str) -> str:
        """Drop one DSHMSG envelope into the liaison session's turn.

        Returns the delivery mode used: ``steer`` when a turn was in flight
        (the message joins the running turn immediately), ``queue`` when the
        session was idle (the message drives a fresh turn on it).
        """
        sid = self._liaison_sid()
        sessions = await self._dsh_api("session.list", {})
        running = any(s.get("sessionId") == sid and s.get("running")
                      for s in sessions.get("items", []))
        mode = "steer" if running else "queue"
        line = "DSHMSG]" + json.dumps({
            "from": self.head_handle, "to": self.liaison_session, "type": "ask",
            "ref": ref, "body": body[:8000],
            "msgid": str(uuid.uuid4()), "ts": int(time.time() * 1000),
        }, ensure_ascii=False)
        await self._dsh_api("session.prompt", {
            "sessionId": sid, "mode": mode,
            "content": [{"type": "text", "text": line}],
        })
        return mode

    async def _liaison_roundtrip(self, body: str, run_id: str | None,
                                 extra_receipt: dict | None = None) -> str:
        """Deliver to the liaison turn, arm the phase-2 wait, return receipt."""
        ref = "vh-" + uuid.uuid4().hex[:8]
        dispatch = DshDispatch(run_id=run_id, task_id=None, ref=ref,
                               credentials=[make_credential(ref.upper())])
        self._runs[ref] = dispatch
        mode = await self._deliver_liaison(ref, body)
        receipt = {
            "status": "accepted", "run_id": run_id, "ref": ref,
            "credentials": dispatch.credentials,
            "note": f"已转对接人（{mode}），完成后播报",
        }
        if extra_receipt:
            receipt.update(extra_receipt)
        self._pending[ref] = asyncio.create_task(self._phase2(ref, dispatch))
        self._pending[ref].add_done_callback(_report_phase2_death)
        await self.bus.emit("orch.dispatch", {
            "run_id": run_id, "task_id": None, "ref": ref,
            "credentials": dispatch.credentials, "lane": "liaison", "mode": mode,
        })
        return json.dumps(receipt, ensure_ascii=False)

    # ---- head tool 1: dispatch_intent ----

    async def dispatch_plan(self, objective: str, subtasks_json: str) -> str:
        """Head-facing DAG entry: tolerant-parse a subtask JSON list.

        Accepts a bare JSON array or ``{"subtasks": [...]}`` (also
        ``tasks``/``items``); per item the spec text may sit under
        ``spec``/``goal``/``task``/``description``, deps under ``deps``
        (indices into the same list), the settlement command under
        ``command``/``cmd``, and an explicit worker session under
        ``session``/``worker``. Unparseable or spec-less input returns
        the clarify note.
        """
        import json

        tasks = _parse_dag_subtasks(subtasks_json)
        if not tasks:
            return CLARIFY_NOTE
        if self.liaison_session:
            run_id = await self.lane.create_run(f"[voice-head-plan] {objective[:200]}")
            body = (f"PLAN {objective} run={run_id} || "
                    + json.dumps(tasks, ensure_ascii=False))
            return await self._liaison_roundtrip(body, run_id, {"tasks": len(tasks)})
        return await self.dispatch_dag(objective, tasks)

    async def dispatch(self, raw_intent: str, profile: str | None = None) -> str:
        """Phase 1 acceptance now; phase 2 final re-injected on completion.

        ``profile`` (G4 dressing): bind the stored profile onto the
        configured in-flight dsh session (``bind_session_id``) via
        pool/spawn binding-mode BEFORE the intent goes out — the dressed
        session's final then carries the profile's persona traces.
        Binding is idempotent per (profile, session).
        """
        ref = "vh-" + uuid.uuid4().hex[:8]
        bound = await self._bind_profile(profile)
        if self.liaison_session:
            run_id = await self.lane.create_run(f"[voice-head] {raw_intent[:200]}")
            body = f"INTENT {raw_intent} run={run_id}"
            if profile:
                body += f" profile={profile}"
            return await self._liaison_roundtrip(body, run_id)
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
        if bound:
            receipt["profile"] = bound
        self._pending[ref] = asyncio.create_task(self._phase2(ref, dispatch))
        self._pending[ref].add_done_callback(_report_phase2_death)
        import json

        return json.dumps(receipt, ensure_ascii=False)

    async def _bind_profile(self, profile: str | None) -> dict | None:
        """Bind ``profile`` onto ``bind_session_id`` (pool/spawn
        binding-mode); returns the bind receipt summary or None when not
        applicable (no profile / no lane-a / no session / already bound).

        Failures are NOT swallowed silently — a dressing request that
        cannot be honored raises, so the head hears it instead of
        dispatching an undressed session under a dressed expectation.
        """
        if not profile or not self.bind_session_id or self.lane_a is None:
            return None
        key = f"{profile}@{self.bind_session_id}"
        if key in self._bound:
            return self._bound[key]
        receipt = await self.lane_a.pool_spawn(
            profile, strategy="binding-mode",
            binding_session_id=self.bind_session_id)
        summary = {"name": receipt.get("name", profile),
                   "version": receipt.get("version", ""),
                   "sessionId": receipt.get("sessionId", self.bind_session_id),
                   "injected": bool(receipt.get("injected"))}
        self._bound[key] = summary
        return summary

    async def dispatch_dag(self, objective: str, tasks: list[DagTaskSpec]) -> str:
        """Head tool: split one intent into a dependent task DAG (lane B).

        Creates the run + tasks (``--dep`` wired from spec indices), then
        a phase-2 walker starts workers in dependency waves — a worker
        starts only once its deps settled ``succeeded`` (dais-side state
        kept in step via promote-tasks) — matches each dispatch's
        worker_done settlement, and aggregates per-task outcomes plus
        terminal tails into ONE final message for the voice chain.
        """
        import json

        if not tasks:
            return CLARIFY_NOTE
        ref = "vh-" + uuid.uuid4().hex[:8]
        run_id = await self.lane.create_run(f"[voice-head] {objective[:200]}")
        task_ids: list[str] = []
        for t in tasks:
            deps = [task_ids[i] for i in t.deps if i < len(task_ids)]
            task_ids.append(await self.lane.create_task(run_id, t.spec[:2000],
                                                        deps=deps))
        credential = make_credential(ref.upper())
        dispatch = DshDispatch(run_id=run_id, task_id=None, ref=ref,
                               credentials=[credential])
        dispatch.dag = [
            {"task_id": tid, "deps": t.deps, "command": t.command,
             "session": t.session
             or (self.dag_workers[i % len(self.dag_workers)]
                 if self.dag_workers else None),
             "spec": t.spec}
            for i, (tid, t) in enumerate(zip(task_ids, tasks))
        ]  # type: ignore[attr-defined]
        dispatch.dag_ctx = {}                                  # type: ignore[attr-defined]
        self._runs[ref] = dispatch
        await self.bus.emit("orch.dispatch", {
            "run_id": run_id, "task_id": None, "ref": ref,
            "credentials": dispatch.credentials, "lane": "b-dag",
            "extra": json.dumps({"tasks": len(tasks)}, ensure_ascii=False),
        })
        self._pending[ref] = asyncio.create_task(self._phase2_dag(ref, dispatch))
        self._pending[ref].add_done_callback(_report_phase2_death)
        return json.dumps({
            "status": "accepted", "run_id": run_id, "ref": ref,
            "credentials": dispatch.credentials, "tasks": len(tasks),
            "note": "已按依赖拆分受理，完成后播报",
        }, ensure_ascii=False)

    async def _fanout(self, raw_intent: str, ref: str) -> DshDispatch | None:
        """Create run + intent message; returns None when unsplittable."""
        if self.lane_mode == "b-orca":
            return await self._fanout_orca(raw_intent, ref)
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

    async def _fanout_orca(self, raw_intent: str, ref: str) -> DshDispatch:
        """Lane B-orca fan-out (F7 routing: worktree/terminal deliveries).

        Spawns a scratch worktree with the configured agent (default omp)
        and a prompt that asks for one verbatim final line plus an artifact
        write — the artifact is the render-proof completion signal (the
        agent's answer text never enters the scrollback ring; live-probed
        2026-08-23). The worktree id doubles as the run handle.
        """
        if self.lane_orca is None:
            raise ValueError("lane_mode='b-orca' requires lane_orca=OrcaLane(...)")
        if not self.orca_repo:
            raise ValueError("lane_mode='b-orca' requires orca_repo selector")
        import json as _json

        credential = make_credential(ref.upper())
        body = f"{raw_intent} {credential}"
        artifact = f"final-{ref}.txt"
        prompt = (
            f"任务：{raw_intent}\n"
            f"完成后：①把下面这一行逐字写入当前工作目录的 {artifact} 文件（不要加任何其他内容）：\n"
            f"调研完成 {credential} 结论 41%\n"
            f"②同时用一行回复确认。"
        )
        created = await self.lane_orca.spawn_worktree(
            f"vh-{ref}", self.orca_repo, self.orca_agent, prompt, setup="skip")
        wt_id = created["worktreeId"]
        dispatch = DshDispatch(run_id=wt_id, task_id=created.get("terminalHandle"),
                               ref=ref, credentials=[credential])
        dispatch.artifact = artifact      # type: ignore[attr-defined]
        dispatch.prompt = prompt          # type: ignore[attr-defined]  (dance resend)
        self._runs[ref] = dispatch
        await self.bus.emit("orch.dispatch", {
            "run_id": wt_id, "task_id": None, "ref": ref,
            "credentials": dispatch.credentials, "lane": "b-orca",
            "extra": _json.dumps({"artifact": artifact, "agent": self.orca_agent}),
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
            if (self.lane_mode == "b-orca"
                    and dispatch.run_id and "::" in dispatch.run_id):
                body = await self._await_orca(dispatch, budget)
                continue
            try:
                if (self.lane_a is not None and dispatch.run_id
                        and dispatch.run_id.startswith("t_")):
                    try:
                        body = await self.lane_a.await_done(
                            dispatch.run_id, timeout_s=budget)
                    except A2aError as exc:
                        # server no longer knows the task (restart/roll):
                        # its final is unknowable — surface and stop polling
                        await self.bus.emit("orch.progress",
                                            {"ref": ref,
                                             "note": f"lane-a task gone: {exc}"})
                        return
                    continue
                # Poll the HEAD's own mailbox: replies are addressed to the
                # intent's sender; polling the orchestrator handle would
                # self-match the intent we sent (live-probed 2026-08-23).
                body = await self.lane.await_done(
                    self.head_handle, ref, timeout_s=budget,
                    poll_s=self.poll_s, poll_max_s=self.poll_max_s,
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

    async def _phase2_dag(self, ref: str, dispatch: DshDispatch) -> None:
        """Walk the DAG in dependency waves; aggregate ONE final message.

        A worker starts only after its deps settled ``succeeded``; a
        failed dep skips the dependents (recorded, not hidden). Each
        settlement is matched via the dispatch's worker_done row, the
        terminal tail is kept as the task's contribution, and
        promote-tasks keeps dais-side readiness in step. Settlement
        observation shares the escalating-backoff discipline (D-17).
        """
        from rt_dsh_lane import DaisLaneError

        dag: list[dict] = dispatch.dag          # type: ignore[attr-defined]
        ctx: dict[int, str] = dispatch.dag_ctx  # type: ignore[attr-defined]
        outcomes: dict[int, dict] = {}
        tails: dict[int, str] = {}
        deadline = asyncio.get_event_loop().time() + self.await_timeout_s
        delay = self.poll_s

        def _deps_done(i: int) -> bool:
            return all(d in outcomes for d in dag[i]["deps"])

        def _deps_ok(i: int) -> bool:
            return all(outcomes[d].get("outcome") == "succeeded"
                       for d in dag[i]["deps"])

        while len(outcomes) < len(dag):
            left = deadline - asyncio.get_event_loop().time()
            if left <= 0:
                await self.bus.emit("orch.progress",
                                    {"ref": ref, "note": "DAG 仍在途"})
                return
            # wave start: ready tasks (deps settled succeeded) get workers
            for i, t in enumerate(dag):
                if i in outcomes or i in ctx:
                    continue
                if not _deps_done(i):
                    continue
                if _deps_ok(i):
                    try:
                        ctx[i] = await self.lane.start_worker(
                            t["task_id"], command=t["command"],
                            session=t["session"])
                        await self.bus.emit("orch.progress", {
                            "ref": ref,
                            "note": f"task {i + 1}/{len(dag)} worker "
                                    f"{ctx[i]} started"})
                        if t["command"]:
                            # block settlement needs the command to RUN in
                            # the bound terminal — inject it there. A failed
                            # injection strands the task (no command block
                            # → no settlement), so surface it on the
                            # progress plane instead of swallowing it.
                            try:
                                await self.lane.inject_prompt(ctx[i], t["command"])
                            except DaisLaneError as e:
                                await self.bus.emit("orch.progress", {
                                    "ref": ref,
                                    "note": f"task {i + 1} inject failed: "
                                            f"{str(e)[:120]}"})
                    except DaisLaneError:
                        await asyncio.sleep(min(delay, 1.0))
                else:
                    outcomes[i] = {"outcome": "skipped",
                                   "reason": "dependency failed"}
            if not ctx:
                break
            # observe settlements round-robin with small slices
            settled_any = False
            for i, handle in list(ctx.items()):
                slice_s = min(2.0, max(0.1, deadline - asyncio.get_event_loop().time()))
                try:
                    row = await self.lane.await_worker_done(
                        handle, timeout_s=slice_s, poll_s=0.5,
                        poll_max_s=2.0, run_id=dispatch.run_id,
                        task_id=dag[i]["task_id"])
                except TimeoutError:
                    continue
                except DaisLaneError:
                    continue  # transient lane errors retry next cycle
                outcomes[i] = row
                settled_any = True
                del ctx[i]
                try:
                    tail, _ = await self.lane.read_worker(handle, lines=6)
                    tails[i] = tail.strip()[-400:]
                except DaisLaneError:
                    tails[i] = ""
                try:
                    await self.lane.promote_tasks(dispatch.run_id or "")
                except DaisLaneError:
                    pass  # dais-side readiness is best-effort bookkeeping
                await self.bus.emit("orch.progress", {
                    "ref": ref,
                    "note": f"task {i + 1}/{len(dag)} settled "
                            f"{row.get('outcome', '?')}"})
            if not settled_any:
                await asyncio.sleep(min(delay, max(0.05, left)))
                delay = min(delay * 1.5, self.poll_max_s)

        parts = []
        for i, t in enumerate(dag):
            outcome = outcomes.get(i, {}).get("outcome", "unsettled")
            tail = tails.get(i, "")
            line = f"子任务{i + 1}：{t['spec'][:60]} → {outcome}"
            if tail:
                line += f"｜{tail}"
            parts.append(line)
        credential = (dispatch.credentials or [""])[0]
        body = "\n".join(parts) + f"\n【凭证{credential}】"
        final = f"{FINAL_PREFIX}{body}"
        await self.bus.emit("orch.done", {"ref": ref, "run_id": dispatch.run_id,
                                          "artifact": body})
        if self.on_final:
            await self.on_final(ref, final)

    async def _await_orca(self, dispatch: DshDispatch, budget_s: float) -> str | None:
        """Await the worktree artifact (render-proof) within one budget slice.

        Follows the conformance ``_orca_worker`` choreography in miniature:
        bounded tui-idle wait slices; completion decided ONLY by the
        artifact's exact content match (partial writes never satisfy);
        dialog flashes answered with ``1`` (bounded); ONE interrupt +
        prompt resend after the settle window passes with output but no
        artifact (a worker stuck thinking never completes otherwise).
        Teardown is owned by cancel()/the caller — this coroutine never
        removes the worktree. Returns the artifact body, or None when
        the slice budget expired (phase-2 loops back for another slice
        until await_timeout_s).
        """
        from pathlib import Path
        from rt_orca_lane import OrcaLaneError, OrcaLaneTimeout

        assert self.lane_orca is not None
        handle = dispatch.task_id
        wt_id = dispatch.run_id or ""
        artifact = getattr(dispatch, "artifact", None) or f"final-{dispatch.ref}.txt"
        prompt = getattr(dispatch, "prompt", "")
        final_path = Path(wt_id.split("::", 1)[1]) / artifact if "::" in wt_id else None
        expected = f"调研完成 {dispatch.credentials[0]} 结论 41%"
        now = asyncio.get_event_loop().time
        deadline = now() + budget_s
        dialogs = 0
        cursor = 0
        t_output = None
        danced = False

        async def lane_retry(note: str) -> None:
            # transient lane failure (CLI hang killed at subprocess bound,
            # ok:false busy, …): observable pause, retried until budget
            await self.bus.emit("orch.progress",
                                {"ref": dispatch.ref, "note": note[:160]})
            await asyncio.sleep(2.0)

        while now() < deadline:
            try:
                await self.lane_orca.wait(handle, what="tui-idle",
                                          timeout_ms=ORCA_WAIT_SLICE_MS)
            except OrcaLaneTimeout:
                pass  # bounded slice consumed — keep polling within budget
            except OrcaLaneError as exc:
                msg = str(exc)
                if not (msg.startswith("exit=1") and '"ok": true' in msg):
                    await lane_retry(f"lane retry (wait): {msg}")
                    continue
            if final_path is not None and final_path.exists():
                try:
                    content = final_path.read_text(encoding="utf-8").strip()
                except OSError:
                    content = ""  # mid-write/mid-remove race — next pass re-reads
                if content == expected:  # exact — partial writes never satisfy
                    return content
            try:
                page, cursor = await self.lane_orca.read(handle, cursor=cursor,
                                                         limit=200)
                if ("Enter to confirm" in page or "2. No, exit" in page) \
                        and dialogs < 5:
                    dialogs += 1
                    await self.lane_orca.send(handle, "1")
                    continue
                if page.strip() and t_output is None:
                    t_output = now()
                if (not danced and prompt and t_output is not None
                        and now() - t_output >= ORCA_TURN_SETTLE_S):
                    await self.lane_orca.interrupt(handle)
                    await asyncio.sleep(3)
                    await self.lane_orca.send(handle, prompt)
                    danced = True
            except OrcaLaneError as exc:
                await lane_retry(f"lane retry (read): {exc}")
        return None

    # ---- head tool 2: query_status ----

    async def query_status(self, run_id: str | None = None) -> str:
        """Spoken-friendly aggregation of dais status (+ pending refs).

        Pending dais dispatches (ctx_ handles) are additionally classified
        via ``scan-wait-blocked`` so the user hears WHERE a run is stuck,
        not just that it is."""
        from rt_dsh_lane import DaisLaneError

        if self.liaison_session:
            body = f"STATUS run={run_id}" if run_id else "STATUS"
            return await self._liaison_roundtrip(body, run_id)
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
        for d in self._runs.values():
            if d.ref not in pending:
                continue
            handles = []
            tid = getattr(d, "task_id", None)
            if tid and str(tid).startswith("ctx_"):
                handles.append(str(tid))
            handles += [c for c in getattr(d, "dag_ctx", {}).values()]
            for handle in handles:
                try:
                    label = (await self.lane.scan_wait_blocked(handle)).strip()
                except DaisLaneError:
                    continue  # classification is best-effort surfacing
                if label and label.lower() not in ("none", "ok", ""):
                    lines.append(f"{d.ref}：卡在 {label}")
        import json

        return json.dumps({"runs": lines, "pending": pending}, ensure_ascii=False)

    async def resolve(self, gate_id: str, resolution: str) -> str:
        """Resolve a decision gate (head tool for "unblock it with X")."""
        await self.lane.resolve_gate(gate_id, resolution)
        await self.bus.emit("orch.progress",
                            {"note": f"gate {gate_id} → {resolution}"})
        import json

        return json.dumps({"status": "resolved", "gate": gate_id,
                           "resolution": resolution}, ensure_ascii=False)

    # ---- head tool 3: cancel ----

    async def cancel(self, ref_or_run: str) -> str:
        """Cancel by ref (vh-…) or run_id; kills pending phase-2 task."""
        dispatch = self._runs.get(ref_or_run)
        run_id = dispatch.run_id if dispatch else ref_or_run
        task = self._pending.get(ref_or_run) or self._pending.get(run_id)
        if task and not task.done():
            task.cancel()
        if self.liaison_session:
            body = f"CANCEL {ref_or_run} run={run_id}"
            return await self._liaison_roundtrip(body, run_id)
        # live DAG dispatches fail individually (best-effort; the pending
        # kill above is the authoritative stop)
        for handle in list(getattr(dispatch, "dag_ctx", {}).values()):
            try:
                await self.lane.fail_dispatch(handle, "cancelled by voice user")
            except Exception:
                pass
        if self.lane_mode == "b-orca" and run_id and "::" in run_id:
            state = "canceled"
            try:
                await self.lane_orca.stop(run_id)  # type: ignore[union-attr]
                await self.lane_orca._run("worktree", "rm", "--worktree", run_id)  # type: ignore[union-attr]
            except Exception:
                pass  # teardown best-effort; residual listed by worktree_ps
        elif self.lane_a is not None and run_id.startswith("t_"):
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
