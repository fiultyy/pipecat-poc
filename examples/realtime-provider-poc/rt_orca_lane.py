#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""OrcaLane: async wrapper over the ``orca-ide`` CLI (lane B;
docs/kg/07-laneb-orca-ade.md §1).

Every method maps 1:1 to a CLI contract probed on the live binary
(2026-08-23, orca app 1.4.185). All commands are sent with ``--json``
and parsed from the common envelope ``{id, ok, result | error}``;
failures raise OrcaLaneError (form aligned with DaisLaneError).

R1 discipline: the binary is hardcoded ``orca-ide``. Bare ``orca`` on
Linux is the GNOME screen reader and is rejected outright.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

ORCA_BIN = "orca-ide"

# Bounded-wait conditions (terminal wait --for).
WAIT_CONDITIONS = ("exit", "tui-idle")
# worktree create --setup policy values.
SETUP_POLICIES = ("run", "skip", "inherit")


class OrcaLaneError(RuntimeError):
    """CLI invocation failed (exit != 0, ok:false, or unparseable --json)."""


class OrcaLaneTimeout(OrcaLaneError):
    """``terminal wait`` exceeded its ``--timeout-ms`` (bounded wait)."""


@dataclass
class OrcaLane:
    """Async facade over the orca-ide CLI (lane B execution plane).

    Unlike DaisLane there is no bus lock: orca-ide invocations hit a
    local runtime socket and do not contend the way dais bus reads do.

    Args:
        binary: orca-ide binary (default hardcoded; bare ``orca`` is
            rejected — R1).
        default_timeout_s: per-invocation subprocess timeout.
        runner: injectable async runner ``(argv) -> (stdout, stderr)``
            (tests inject a mock; production uses asyncio subprocess).
    """

    BIN = ORCA_BIN
    binary: str = ORCA_BIN
    default_timeout_s: float = 60.0
    runner: Callable[[list[str]], Awaitable[tuple[str, str]]] | None = None
    _call_log: list[list[str]] = field(default_factory=list)

    def __post_init__(self):
        if self.binary == "orca":
            raise ValueError(
                "bare `orca` is the GNOME screen reader; use `orca-ide` (R1)"
            )

    async def _run(self, *args: str, timeout_s: float | None = None) -> object:
        """Run one CLI invocation with ``--json``; return the envelope
        ``result`` payload. Failures raise OrcaLaneError (DaisLaneError
        form: exit code or CLI error code + truncated detail)."""
        argv = [self.binary, *args, "--json"]
        self._call_log.append(argv)
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
                raise OrcaLaneError(f"timeout: {' '.join(args)}") from None
            stdout, stderr = out.decode(errors="replace"), err.decode(errors="replace")
            if proc.returncode != 0:
                raise OrcaLaneError(
                    f"exit={proc.returncode} {' '.join(args[:3])}…: "
                    f"{(stderr.strip() or stdout.strip())[:160]}"
                )
        if isinstance(stdout, bytes):
            stdout = stdout.decode(errors="replace")
        try:
            envelope = json.loads(stdout.strip() or "null")
        except json.JSONDecodeError:
            raise OrcaLaneError(
                f"non-json output {' '.join(args[:3])}…: {stdout.strip()[:120]!r}"
            ) from None
        if not isinstance(envelope, dict):
            raise OrcaLaneError(
                f"unexpected envelope {' '.join(args[:3])}…: {str(envelope)[:120]!r}"
            )
        if not envelope.get("ok"):
            err = envelope.get("error") or {}
            code, message = err.get("code", "error"), err.get("message", "")
            detail = f"{code}: {message}".strip()
            if code == "timeout":
                raise OrcaLaneTimeout(f"{' '.join(args[:3])}…: {detail[:160]}")
            raise OrcaLaneError(f"{' '.join(args[:3])}…: {detail[:160]}")
        return envelope.get("result")

    # ---- 探活 ----

    async def status(self) -> dict:
        """``status`` → {app, runtime, graph} readiness (live-test probe,
        lane-B counterpart of DaisLane bus health checks)."""
        result = await self._run("status")
        assert isinstance(result, dict)
        return result

    # ---- spawn（车道B 的 create-run/create-task/start-worker 合体）----

    async def spawn_worktree(self, name: str, repo: str, agent: str,
                             prompt: str, *, base_branch: str | None = None,
                             setup: str = "run") -> dict:
        """``worktree create --name --repo <selector> --agent --prompt
        [--base-branch] --setup run|skip|inherit`` →
        ``{worktreeId, terminalHandle}``.

        One call = new worktree + agent launch + initial prompt (lane-A
        create-run/create-task/start-worker + send-message equivalent).
        ``repo`` is a verbatim selector (``id:<id>``/``name:<n>``
        /``path:<p>``). The agent handle comes from ``agentTerminalHandle``
        (current runtimes) or ``startupTerminal.handle`` (legacy); may be
        None when the runtime returns neither (folder-based repos).
        """
        if setup not in SETUP_POLICIES:
            raise ValueError(f"setup must be one of {SETUP_POLICIES}, got {setup!r}")
        args = ["worktree", "create", "--name", name, "--repo", repo,
                "--agent", agent, "--prompt", prompt]
        if base_branch:
            args += ["--base-branch", base_branch]
        args += ["--setup", setup]
        result = await self._run(*args)
        if not isinstance(result, dict):
            raise OrcaLaneError(f"worktree create: unexpected result {str(result)[:120]!r}")
        worktree = result.get("worktree") or {}
        worktree_id = worktree.get("id")
        if not worktree_id:
            raise OrcaLaneError(
                f"no worktree id in result: {str(result)[:120]!r}"
            )
        handle = (result.get("agentTerminalHandle")
                  or (result.get("startupTerminal") or {}).get("handle"))
        return {"worktreeId": worktree_id, "terminalHandle": handle}

    # ---- 监控（车道B 的 read-worker / worker_done 等价物）----

    async def terminal_list(self, worktree: str | None = None) -> list[dict]:
        """``terminal list [--worktree <selector>]`` → live terminal rows
        ({handle, worktreeId, worktreePath, branch, title, ...})."""
        args = ["terminal", "list"]
        if worktree:
            args += ["--worktree", worktree]
        result = await self._run(*args)
        terminals = result.get("terminals", []) if isinstance(result, dict) else []
        return list(terminals)

    async def read(self, terminal: str, *, cursor: str | int = 0,
                   limit: int = 40) -> tuple[str, str]:
        """``terminal read --terminal <h> [--cursor <n>] [--limit <n>]``
        → ``(tail_text, next_cursor)`` (read-worker --after equivalent).

        Cursor semantics (live-probed 2026-08-23): cursors are STRINGS
        (``nextCursor``/``oldestCursor``/``latestCursor``); pass the
        previous read's ``nextCursor`` as ``--cursor`` to fetch only new
        output; ``limited`` in the raw envelope flags more pages — callers
        page while limited. ``oldestCursor`` marks dropped older lines.
        """
        result = await self._run(
            "terminal", "read", "--terminal", terminal,
            "--cursor", str(cursor), "--limit", str(limit),
        )
        if not isinstance(result, dict) or "terminal" not in result:
            raise OrcaLaneError(f"terminal read: unexpected result {str(result)[:120]!r}")
        term = result["terminal"]
        tail_lines = term.get("tail") or []
        return "\n".join(str(line) for line in tail_lines), str(term.get("nextCursor", cursor))

    async def wait(self, terminal: str, *, what: str = "exit",
                   timeout_ms: int = 30000) -> dict:
        """``terminal wait --terminal <h> --for exit|tui-idle
        --timeout-ms <ms>`` → condition result dict.

        Bounded wait (dais discipline: timeout always sent, never a naked
        wait). A CLI ``timeout`` error raises OrcaLaneTimeout so callers
        map it to "still running" instead of a hard failure. The outer
        subprocess timeout exceeds ``timeout_ms`` so the CLI's own bound
        resolves first.
        """
        if what not in WAIT_CONDITIONS:
            raise ValueError(f"what must be one of {WAIT_CONDITIONS}, got {what!r}")
        if timeout_ms <= 0:
            raise ValueError(f"timeout_ms must be positive, got {timeout_ms}")
        result = await self._run(
            "terminal", "wait", "--terminal", terminal, "--for", what,
            "--timeout-ms", str(timeout_ms),
            timeout_s=max(self.default_timeout_s, timeout_ms / 1000 + 10),
        )
        assert isinstance(result, dict)
        return result

    # ---- 干预（车道B 的 answer 等价物）----

    async def send(self, terminal: str, text: str, *, enter: bool = True) -> None:
        """``terminal send --terminal <h> --text <t> [--enter]`` — inject
        input / answer a prompt."""
        args = ["terminal", "send", "--terminal", terminal, "--text", text]
        if enter:
            args.append("--enter")
        await self._run(*args)

    async def interrupt(self, terminal: str) -> None:
        """``terminal send --terminal <h> --interrupt`` — local interrupt
        (answer --interrupt equivalent; local break ≠ remote cancel)."""
        await self._run("terminal", "send", "--terminal", terminal, "--interrupt")

    async def stop(self, worktree: str) -> None:
        """``terminal stop --worktree <selector>`` — stop all terminals of
        a worktree (manager's last resort before escalating a timeout)."""
        await self._run("terminal", "stop", "--worktree", worktree)

    # ---- 汇总 ----

    async def worktree_ps(self) -> dict:
        """``worktree ps`` → cross-worktree orchestration summary
        (check-status aggregate equivalent; ``result.worktrees[]``)."""
        result = await self._run("worktree", "ps")
        assert isinstance(result, dict)
        return result
