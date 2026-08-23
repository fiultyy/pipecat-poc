#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""M0 probe matrix: orchestrator CLI reachability from the pipecat venv.

Probes (read-only, no state mutation on the orchestrator side):

- dais orchestration check-status  — message-bus CLI over the GUI runtime
  socket (runtime liveness via ~/.local/state/dais/dais-runtime.json).
- orca-ide status --json           — Orca app runtime state.

Each probe returns (ok, latency_ms, detail); ``run_matrix()`` aggregates
them and renders the evidence table for docs/kg/evidence/m0-probe.md.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

DAIS_BIN = str(Path("~/.local/bin/dais").expanduser())
ORCA_BIN = "orca-ide"  # never bare `orca` (GNOME screen reader; risk R1)
DAIS_RUNTIME = Path("~/.local/state/dais/dais-runtime.json").expanduser()

TIMEOUT_S = 15.0


@dataclass
class ProbeResult:
    name: str
    ok: bool
    latency_ms: float
    detail: str

    def row(self) -> str:
        mark = "PASS" if self.ok else "FAIL"
        detail = " / ".join(self.detail.splitlines())
        return f"| {self.name} | {mark} | {self.latency_ms:.0f} ms | {detail} |"


async def _timed_run(name: str, cmd: list[str]) -> tuple[ProbeResult, str]:
    """Run a command; return (result, full stdout text) for callers that parse payloads."""
    start = time.perf_counter()
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await asyncio.wait_for(proc.communicate(), timeout=TIMEOUT_S)
    except (FileNotFoundError, asyncio.TimeoutError) as e:
        return ProbeResult(name, False, (time.perf_counter() - start) * 1000, f"{type(e).__name__}: {e}"), ""
    latency = (time.perf_counter() - start) * 1000
    text = out.decode(errors="replace").strip()
    if proc.returncode != 0:
        return ProbeResult(
            name, False, latency, f"exit={proc.returncode}: {err.decode(errors='replace')[:120]}"
        ), text
    return ProbeResult(name, True, latency, text[:120] if text else "(no stdout)"), text


async def probe_dais_runtime() -> ProbeResult:
    start = time.perf_counter()
    try:
        rt = json.loads(DAIS_RUNTIME.read_text())
        alive = Path(f"/proc/{rt.get('pid')}").exists()
        detail = f"mode={rt.get('mode')} pid={rt.get('pid')} alive={alive}"
        return ProbeResult("dais runtime json", alive, (time.perf_counter() - start) * 1000, detail)
    except FileNotFoundError:
        # the resident daemon stopped writing dais-runtime.json (layout
        # change after M0); daemon liveness falls back to the CLI surface,
        # which the same matrix probes independently
        res, _ = await _timed_run(
            "dais runtime json (CLI fallback)", [DAIS_BIN, "orchestration", "check-status"]
        )
        return ProbeResult(
            "dais runtime json", res.ok, (time.perf_counter() - start) * 1000,
            f"runtime json absent; CLI fallback {'ok' if res.ok else 'failed'}",
        )
    except json.JSONDecodeError as e:
        return ProbeResult("dais runtime json", False, (time.perf_counter() - start) * 1000, str(e))


async def probe_dais_status() -> ProbeResult:
    res, _ = await _timed_run("dais check-status", [DAIS_BIN, "orchestration", "check-status"])
    return res


async def probe_orca_status() -> ProbeResult:
    res, full = await _timed_run("orca-ide status", [ORCA_BIN, "status", "--json"])
    if res.ok:
        try:
            data = json.loads(full)
        except json.JSONDecodeError:
            data = None
        if data and data.get("ok"):
            app = data.get("result", {}).get("app", {})
            runtime = data.get("result", {}).get("runtime", {})
            res.detail = f"running={app.get('running')} v{runtime.get('appVersion', '?')}"
        else:
            res.detail = f"json ok-flag false: {full[:80]}"
            res.ok = False
    return res


async def run_matrix() -> list[ProbeResult]:
    return [
        await probe_dais_runtime(),
        await probe_dais_status(),
        await probe_orca_status(),
    ]


def render_evidence(results: list[ProbeResult]) -> str:
    lines = [
        "# M0 探针矩阵证据（自动生成）",
        "",
        "探针定义：`examples/realtime-provider-poc/rt_probe_m0.py`；运行：`tests/test_m0_probe.py`。",
        "",
        "| 探针 | 结果 | 时延 | 明细 |",
        "|---|---|---|---|",
        *[r.row() for r in results],
        "",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    from rt_env import load_env

    load_env()
    for r in asyncio.run(run_matrix()):
        print(r.row())
