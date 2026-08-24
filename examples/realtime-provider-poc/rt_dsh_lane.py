#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""DaisLane: async wrapper over ``dais orchestration`` CLI (WS1 lane B;
docs/kg/01-ws1-head-dsh.md §2).

Every method maps 1:1 to a CLI contract probed on the live binary
(2026-08-22, dais app mode). Output is line-oriented even with
``--output-format json`` (streamed NDJSON-ish); parsers are tolerant of
both the plain and json flavors.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path

DAIS_BIN_DEFAULT = str(Path("~/.local/bin/dais").expanduser())
WORKER_DONE = "worker_done"


class DaisLaneError(RuntimeError):
    """CLI invocation failed (exit != 0 or unparseable output)."""


@dataclass
class DaisLane:
    """Async facade over the dais orchestration CLI.

    Args:
        binary: dais binary path.
        default_timeout_s: per-invocation timeout.
        runner: injectable async runner ``(argv) -> (stdout, stderr)``
            (tests inject a mock; production uses asyncio subprocess).
    """

    binary: str = DAIS_BIN_DEFAULT
    default_timeout_s: float = 60.0
    runner: object | None = None
    _call_log: list[list[str]] = field(default_factory=list)
    _inbox: dict[str, list[dict]] = field(default_factory=dict)
    _lock: object = field(default=None, init=False, repr=False)

    def __post_init__(self):
        # dais bus invocations serialize on a process-wide lock: concurrent
        # check-messages --wait callers starve each other past their timeouts
        # (the --timeout-ms bounds the wait-for-message, not lock queuing;
        # live-probed 2026-08-23).
        self._lock = asyncio.Lock()

    async def _run(self, *args: str, timeout_s: float | None = None) -> str:
        """Run one CLI invocation; return stdout (stderr on failure in the error)."""
        argv = [self.binary, "orchestration", *args]
        self._call_log.append(argv)
        async with self._lock:
            if self.runner is not None:
                stdout, stderr = await self.runner(argv)
            else:
                proc = await asyncio.create_subprocess_exec(
                    *argv,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                try:
                    out, err = await asyncio.wait_for(
                        proc.communicate(), timeout=timeout_s or self.default_timeout_s
                    )
                except asyncio.TimeoutError:
                    proc.kill()
                    raise DaisLaneError(f"timeout: {' '.join(args)}") from None
                stdout, stderr = out.decode(errors="replace"), err.decode(errors="replace")
                if proc.returncode != 0:
                    raise DaisLaneError(
                        f"exit={proc.returncode} {' '.join(args[:3])}…: {stderr.strip()[:160]}"
                    )
        if isinstance(stdout, bytes):
            stdout = stdout.decode(errors="replace")
        return stdout

    # ---- identity / lifecycle ----

    async def create_run(self, objective: str) -> str:
        """``create-run --objective`` → ``run_<id>``."""
        out = await self._run("create-run", "--objective", objective)
        m = re.search(r"run_[0-9a-f]+", out)
        if not m:
            raise DaisLaneError(f"no run id in output: {out.strip()[:120]!r}")
        return m.group(0)

    async def create_task(self, run_id: str, spec: str, deps: list[str] | None = None) -> str:
        """``create-task <run> <spec> [--dep t_..]*`` → ``task_<id>``."""
        args = ["create-task", run_id, spec]
        for dep in deps or []:
            args += ["--dep", dep]
        out = await self._run(*args)
        m = re.search(r"task_[0-9a-f]+", out)
        if not m:
            raise DaisLaneError(f"no task id in output: {out.strip()[:120]!r}")
        return m.group(0)

    async def start_worker(self, task_id: str, command: str | None = None) -> str:
        """``start-worker <task> [--command <cmd>]`` → ``ctx_<id>`` (dispatch handle)."""
        args = ["start-worker", task_id]
        if command:
            args += ["--command", command]
        out = await self._run(*args)
        m = re.search(r"ctx_[0-9a-f]+", out)
        if not m:
            raise DaisLaneError(f"no dispatch id in output: {out.strip()[:120]!r}")
        return m.group(0)

    # ---- messaging (intent in, done out) ----

    async def send_intent(self, run_id: str, handle: str, raw_intent: str, ref: str,
                          from_id: str = "voice-head") -> int:
        """``send-message <run> <from> <to> --message-type status`` → seq number.

        The body carries the ``[ref:<ref>]`` prefix convention (cb-send
        envelope compatibility; docs/kg/00-INDEX.md §4.2).
        """
        body = f"[ref:{ref}] {raw_intent}"
        out = await self._run(
            "send-message", run_id, from_id, handle,
            "--message-type", "status", "--subject", "intent", "--body", body,
        )
        m = re.search(r"seq[=:\s]+(\d+)", out)
        return int(m.group(1)) if m else -1

    async def send_reply(self, run_id: str, from_handle: str, to_handle: str,
                         body: str, ref: str, subject: str = "done") -> int:
        """Orchestrator-side counterpart of ``send_intent``: reply with the
        same ``[ref:]`` prefix so the awaiting head matches it."""
        out = await self._run(
            "send-message", run_id, from_handle, to_handle,
            "--message-type", "status", "--subject", subject,
            "--body", f"[ref:{ref}] {body}",
        )
        m = re.search(r"seq[=:\s]+(\d+)", out)
        return int(m.group(1)) if m else -1

    async def check_messages(self, handle: str, wait_s: float | None = None,
                             message_type: str | None = None) -> list[dict]:
        """``check-messages <handle> --timeout-ms N [--type T]`` → rows.

        Mailbox semantics (live-probed 2026-08-23):

        - the handle names the RECIPIENT mailbox; reads CONSUME
        - ``--timeout-ms`` bounds the call and returns CURRENT unread as
          a snapshot (fast on empty); the flag is therefore always sent
        - a flagless call blocks FOREVER on an empty mailbox, and
          ``--wait`` long-polls for arrivals during its window only,
          missing mail already sitting unread — neither is used here

        Rows: {seq, from, to, type, subject, body}.
        """
        args = ["check-messages", handle,
                "--timeout-ms", str(int((wait_s if wait_s is not None else 1.0) * 1000))]
        if message_type:
            args += ["--type", message_type]
        out = await self._run(*args)
        return _parse_message_rows(out)

    async def await_done(self, handle: str, ref: str, timeout_s: float = 1800.0,
                         poll_s: float = 2.0, poll_max_s: float = 8.0,
                         after_seq: int | None = None,
                         from_filter: str | None = None) -> str:
        """Block until a done/status reply carrying ``[ref:<ref>]`` arrives.

        ``handle`` must be the HEAD's own mailbox (replies are addressed
        to the sender of the intent). ``after_seq``/``from_filter`` guard
        against self-matching the intent we sent ourselves. Rows that
        match OTHER refs stay buffered (``_inbox``) so concurrent waiters
        each get their own reply — reads consume, so a poll may drain
        several pending replies at once.

        The sleep between polls escalates from ``poll_s`` by ×1.5 up to
        ``poll_max_s``: each snapshot poll consumes (a write transaction
        on the daemon store), and a flat cadence keeps the store lock
        busy enough to starve in-flight senders — live-caught in the
        LB-002-A V5 run (the liaison's send-message hung 60s/150s behind
        the head's flat 1s poller; the plane recovered the moment the
        poller exited).

        Returns the reply body (without the ref prefix). Raises
        TimeoutError when still pending — callers turn that into a spoken
        "still running" state.
        """
        deadline = time.monotonic() + timeout_s
        delay = poll_s

        def _take() -> str | None:
            buf = self._inbox.get(handle, [])
            for i, row in enumerate(buf):
                body = row.get("body", "")
                seq = row.get("seq") or 0
                frm = row.get("from") or ""
                if f"[ref:{ref}]" not in body:
                    continue
                if after_seq is not None and seq <= after_seq:
                    continue
                if from_filter and frm != from_filter:
                    continue
                del buf[i]
                return body.replace(f"[ref:{ref}]", "", 1).strip()
            return None

        while True:
            got = _take()
            if got is not None:
                return got
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"no reply for ref {ref} within {timeout_s}s")
            # snapshot poll (consumes unread into the shared buffer), then
            # sleep with escalating backoff — --wait would only catch
            # arrivals inside its window
            rows = await self.check_messages(handle)
            self._inbox.setdefault(handle, []).extend(rows)
            got = _take()
            if got is not None:
                return got
            await asyncio.sleep(min(delay, max(0.05, remaining)))
            delay = min(delay * 1.5, poll_max_s)

    # ---- supervision ----

    async def check_status(self, run_id: str | None = None) -> dict:
        """``check-status [--run-id]`` → {runs: N, entries: [{id, summary, tasks}]}."""
        args = ["check-status"]
        if run_id:
            args += ["--run-id", run_id]
        out = await self._run(*args)
        return _parse_status(out)

    async def read_worker(self, dispatch_id: str, after: int = 0, lines: int = 40) -> tuple[str, int]:
        """``read-worker <ctx> [--after N] [--lines L]`` → (tail_text, new_cursor).

        The machine cursor is on STDERR as ``cursor: <n>`` per the skill
        contract; with the injectable runner we accept it from either
        stream to stay mock-friendly.
        """
        args = ["read-worker", dispatch_id, "--after", str(after), "--lines", str(lines)]
        argv = [self.binary, "orchestration", *args]
        self._call_log.append(argv)
        async with self._lock:
            if self.runner is not None:
                stdout, stderr = await self.runner(argv)
            else:
                proc = await asyncio.create_subprocess_exec(
                    *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                out, err = await asyncio.wait_for(proc.communicate(), timeout=self.default_timeout_s)
                stdout, stderr = out.decode(errors="replace"), err.decode(errors="replace")
                if proc.returncode != 0:
                    raise DaisLaneError(f"read-worker exit={proc.returncode}: {stderr[:120]}")
        if isinstance(stdout, bytes):
            stdout = stdout.decode(errors="replace")
        if isinstance(stderr, bytes):
            stderr = stderr.decode(errors="replace")
        # Soft-error contract (dais >= 2026-08-23 orchestration build): a failed
        # view lookup exits 0 with a JSON body {"error": ..., "executed": true}
        # instead of the pre-rebuild exit-1 form. Normalize both to DaisLaneError
        # so callers see one error surface.
        err_match = re.search(r'"error"\s*:\s*"([^"]+)"', stdout)
        if err_match:
            raise DaisLaneError(f"read-worker {dispatch_id}: {err_match.group(1)[:160]}")
        m = re.search(r"cursor:\s*(\d+)", stderr) or re.search(r"cursor:\s*(\d+)", stdout)
        cursor = int(m.group(1)) if m else after + stdout.count("\n")
        return stdout, cursor

    async def fail_dispatch(self, dispatch_id: str, reason: str) -> None:
        """``fail-dispatch <ctx> <err>`` (circuit breaker ++)."""
        await self._run("fail-dispatch", dispatch_id, reason)

    async def scan_wait_blocked(self, dispatch_id: str) -> str:
        """``scan-wait-blocked <ctx>`` → classification label."""
        return (await self._run("scan-wait-blocked", dispatch_id)).strip()

    async def resolve_gate(self, gate_id: str, resolution: str) -> None:
        """``resolve-gate <gate> <resolution>`` (unblock the gated task)."""
        await self._run("resolve-gate", gate_id, resolution)

    # ---- worktree / project plane (LB-002; dais >= 2026-08-24 probe) ----
    # dais itself carries worktree + terminal capabilities the voice link
    # never wired: these close the plane gap vs the orca lane (isolation,
    # spawn-into-terminal) while keeping the DAG/mailbox primitives.

    async def worktree_create(self, project_path: str, name: str) -> str:
        """``worktree-create <project> <name>`` → the new worktree's path
        (``<project>/../<repo>-<name>``, branch ``<name>`` from HEAD;
        registered as a project)."""
        out = await self._run("worktree-create", project_path, name)
        line = out.strip().splitlines()[-1] if out.strip() else ""
        if not line.startswith("/"):
            raise DaisLaneError(f"no worktree path in output: {out.strip()[:120]!r}")
        return line

    async def worktree_list(self, path: str | None = None) -> list[dict]:
        """``worktree-list [<path>]`` → rows. Live flavor (probed
        2026-08-24): one absolute path per line per registered repo (main
        trees included); a porcelain ``<path> <head> [<branch>]`` line is
        also accepted."""
        args = ["worktree-list"]
        if path:
            args.append(path)
        rows: list[dict] = []
        for line in (await self._run(*args)).splitlines():
            line = line.strip()
            if not line or line.startswith(("path", "worktree", "---")):
                continue
            if line.startswith("{"):
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        rows.append(obj)
                        continue
                except json.JSONDecodeError:
                    pass
            parts = line.split()
            if parts and parts[0].startswith("/"):
                rows.append({"path": parts[0],
                             "head": parts[1] if len(parts) > 1 else ""})
        return rows

    async def worktree_remove(self, path: str, force: bool = False) -> None:
        """``worktree-remove <path> [--force]`` (force closes referencing
        terminals first: harness interrupt → PTY shutdown → tab close)."""
        args = ["worktree-remove", path]
        if force:
            args.append("--force")
        await self._run(*args)

    async def project_add(self, path: str) -> None:
        """``project-add <path>`` — idempotent (refreshes last_opened_ts)."""
        await self._run("project-add", path)

    async def project_list(self) -> list[dict]:
        """``project-list`` → [{path, added_ts, last_opened_ts}] (tab-separated
        rows: ``path<TAB>added<TAB>last_opened``)."""
        rows: list[dict] = []
        for line in (await self._run("project-list")).splitlines():
            parts = line.split("\t")
            if len(parts) >= 1 and parts[0].strip().startswith("/"):
                rows.append({
                    "path": parts[0].strip(),
                    "added_ts": parts[1].strip() if len(parts) > 1 else "",
                    "last_opened_ts": parts[2].strip() if len(parts) > 2 else "",
                })
        return rows

    async def project_remove(self, path: str, force: bool = False) -> None:
        """``project-remove <path> [--force]`` — refuses while terminals
        reference the project unless forced."""
        args = ["project-remove", path]
        if force:
            args.append("--force")
        await self._run(*args)

    # ---- terminal plane ----

    async def new_terminal(self, project_path: str, cwd: str | None = None) -> str:
        """``new-terminal <project> [--cwd]`` → the terminal's session
        mailbox handle (``session_<sid>``). Requires a running GUI."""
        args = ["new-terminal", project_path]
        if cwd:
            args += ["--cwd", cwd]
        out = await self._run(*args)
        m = re.search(r"session_[0-9a-zA-Z-]+", out)
        if not m:
            raise DaisLaneError(f"no session handle in output: {out.strip()[:120]!r}")
        return m.group(0)

    async def close_terminal(self, handle: str, force: bool = False) -> None:
        """``close-terminal <session_<sid>> [--force]`` (force = Ctrl-C the
        harness + PTY shutdown before the tab close)."""
        args = ["close-terminal", handle]
        if force:
            args.append("--force")
        await self._run(*args)

    async def inject_prompt(self, dispatch_id: str, text: str, force: bool = False) -> None:
        """``inject-prompt <ctx> <text> [--force]`` — bracketed paste into
        the dispatched worker's terminal; checks idle first unless forced."""
        args = ["inject-prompt", dispatch_id, text]
        if force:
            args.append("--force")
        await self._run(*args)

    async def answer_prompt(self, dispatch_id: str, text: str | None = None,
                            enter: bool = False, interrupt: bool = False) -> None:
        """``answer <ctx> [--text] [--enter] [--interrupt]`` — answer an
        interactive prompt in the dispatched worker's terminal."""
        args = ["answer", dispatch_id]
        if text is not None:
            args += ["--text", text]
        if enter:
            args.append("--enter")
        if interrupt:
            args.append("--interrupt")
        await self._run(*args)

    async def assign(self, dispatch_id: str) -> None:
        """``assign <ctx>`` — bind the dispatch to the active terminal pane
        (registers view + session so inject/read/bridge target it)."""
        await self._run("assign", dispatch_id)

    # ---- scheduling plane ----

    async def promote_tasks(self, run_id: str) -> None:
        """``promote-tasks <run>`` — pending tasks whose deps are all
        completed → ready."""
        await self._run("promote-tasks", run_id)

    async def mark_ready(self, dispatch_id: str) -> None:
        """``mark-ready <ctx>`` — worker → ready, dispatch → dispatched,
        task → dispatched."""
        await self._run("mark-ready", dispatch_id)

    async def transition_worker(self, dispatch_id: str, state: str) -> None:
        """``transition-worker <ctx> <state>``."""
        await self._run("transition-worker", dispatch_id, state)

    async def create_gate(self, task_id: str, question: str,
                          options: list[str] | None = None) -> str:
        """``create-gate <task> --question <q> [--option <o>]*`` → gate id."""
        args = ["create-gate", task_id, "--question", question]
        for opt in options or []:
            args += ["--option", opt]
        out = await self._run(*args)
        m = re.search(r"gate_[0-9a-zA-Z-]+", out)
        if not m:
            raise DaisLaneError(f"no gate id in output: {out.strip()[:120]!r}")
        return m.group(0)

    async def expire_gate(self, gate_id: str) -> None:
        """``expire-gate <gate>`` — expire it (fails its blocked task)."""
        await self._run("expire-gate", gate_id)


# ---- tolerant output parsers (live two-line + mock kv + json flavors) ----

# Live flavor (probed 2026-08-23): a header line, then body lines until the
# next header or end of output.
#   --- seq 3 from session_orch [status] done ---
#   [ref:vh-x] done 【凭证R-1】 结论 41%
_LIVE_HEADER = re.compile(
    r"^---\s*seq\s+(\d+)\s+from\s+(\S+)\s+\[([^\]]+)\](?:\s+(.*?))?\s*---$"
)


def _parse_message_rows(out: str) -> list[dict]:
    """Parse check-messages output; ``to`` is empty in the live flavor
    (the mailbox handle itself is the recipient)."""
    rows: list[dict] = []
    pending: dict | None = None

    def flush():
        nonlocal pending
        if pending is not None:
            pending["body"] = "\n".join(pending.pop("_lines")).strip()
            rows.append(pending)
            pending = None

    for raw in out.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("{"):
            try:
                obj = json.loads(line)
                if isinstance(obj, dict) and ("body" in obj or "type" in obj):
                    flush()
                    rows.append(obj)
                    continue
            except json.JSONDecodeError:
                pass
        m = _LIVE_HEADER.match(line)
        if m:
            flush()
            pending = {
                "seq": int(m.group(1)), "from": m.group(2), "to": "",
                "type": m.group(3), "subject": (m.group(4) or "").strip(),
                "_lines": [],
            }
            continue
        if line.startswith("no unread messages"):
            flush()
            continue
        if pending is not None:
            pending["_lines"].append(line)
            continue
        # mock kv flavor: "seq=3 from=x to=y type=status body=[ref:x] done ok"
        m = re.match(r"seq[=:\s]+(\d+)\s+from[=:\s]+(\S+)\s+to[=:\s]+(\S+)\s+type[=:\s]+(\S+)(?:\s+body[=:\s]?(.*))?$", line)
        if m:
            rows.append({
                "seq": int(m.group(1)), "from": m.group(2), "to": m.group(3),
                "type": m.group(4), "body": (m.group(5) or "").strip(),
            })
    flush()
    return rows


def _parse_status(out: str) -> dict:
    entries = []
    runs = None
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("{"):
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    entries.append(obj)
                    continue
            except json.JSONDecodeError:
                pass
        m = re.match(r"^(\d+)\s+runs?$", line)
        if m:
            runs = int(m.group(1))
            continue
        m = re.match(r"^(run_[0-9a-f]+)\s+(.*)$", line)
        if m:
            entries.append({"id": m.group(1), "summary": m.group(2).strip()})
            continue
        m = re.match(r"^Run\s+(run_[0-9a-f]+):\s*(\d+)\s+tasks?$", line)
        if m:
            entries.append({"id": m.group(1), "tasks": int(m.group(2))})
    return {"runs": runs if runs is not None else len({e.get("id") for e in entries if e.get("id")}), "entries": entries}
