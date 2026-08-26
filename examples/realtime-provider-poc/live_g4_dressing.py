#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""LB-002-D (G4) live: dispatch 携 profile 正式穿衣 → 终稿人格痕迹。

Chain under test (all real, no mocks):

  session-spawn standard          → in-flight dsh session O (real agent)
  pool/spawn *new*                → persona profile saved in the store
  pool/spawn binding-mode         → PROFILE-INJECT envelope onto O
                                    (bindProfile → session.prompt queue)
  dais send-message (intent)      → voice-head → O's mailbox agent_g4_<code>
  session-send wake               → DSHMSG push wakes O's turn
  dais check-messages voice-head  → O's single done reply

Verdicts:
  D1 bind receipt injected:true, version pinned (no re-save)
  D2 done reply arrives with [ref:] match from O's mailbox
  D3 credential verbatim echo
  D4 persona trace: the profile-mandated signature appears in the final
     (the dressed session ADOPTED the persona, not just received it)

Evidence: docs/kg/evidence/lb-002d-dressing-live.md

Usage: .venv/bin/python examples/realtime-provider-poc/live_g4_dressing.py
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
import uuid
from pathlib import Path

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))

from rt_dsh_lane import DaisLane  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
PLUGIN_PORT = int(os.environ.get("A2A_PORT", "8790"))
DAIS = Path("~/.local/bin/dais").expanduser()
SESSION_SPAWN = Path("~/.dsh/maestro/bin/session-spawn").expanduser()
SESSION_SEND = Path("~/.dsh/maestro/bin/session-send").expanduser()
EVID = REPO / "docs/kg/evidence/lb-002d-dressing-live.md"
HEAD = "voice-head"
PERSONA_SIGN = "【人格痕迹·拾音官】"      # profile-mandated final signature
PROFILE_NAME = "vh-persona-g4"

RESULTS: list[tuple[bool, str, str]] = []


def verdict(ok: bool, label: str, detail: str = "") -> bool:
    mark = "PASS" if ok else "FAIL"
    print(f"  [{mark}] {label}" + (f" — {detail}" if detail else ""))
    RESULTS.append((ok, label, detail))
    return ok


def rpc(method: str, params: dict) -> dict:
    """One JSON-RPC call to the a2a plugin (proxy-stripped loopback)."""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    req = urllib.request.Request(
        f"http://127.0.0.1:{PLUGIN_PORT}/",
        data=json.dumps(payload, ensure_ascii=False).encode(),
        headers={"Content-Type": "application/json"})
    with opener.open(req, timeout=60) as resp:
        out = json.loads(resp.read())
    if "error" in out:
        raise RuntimeError(f"{method}: {out['error']}")
    return out["result"]


def sh(argv: list[str], timeout_s: float = 60.0) -> str:
    p = subprocess.run(argv, capture_output=True, text=True, timeout=timeout_s)
    if p.returncode != 0:
        raise RuntimeError(f"{argv[:3]}… exit={p.returncode}: {p.stderr[:160]}")
    return p.stdout


PERSONA_AGENTS_MD = f"""# 拾音官（voice-head 人格穿衣验证 profile）

你是一名严谨克制的语音编排调研员，代号「拾音官」。

### 人格输出规约（最高优先级，逐字执行）

你发出的每一条终稿（对上游的唯一一条回信正文）末尾必须另起一行附上
署名标记 {PERSONA_SIGN}，逐字、不增删、不解释。该标记是穿衣验证锚点，
缺失即验证失败。

### 任务规约

1. 回合首动作：运行 `~/.local/bin/dais orchestration check-messages <你的邮箱> --timeout-ms 2000`，
   快照排空你的邮箱取正文。唤醒信令只代表"有新任务"，正文一律以邮箱快照为准。
2. 从正文解析 `[ref:<ref>]` 前缀、任务描述、必须逐字回显的【凭证…】标记。
3. 完成任务后，用恰好一条 dais 回信回复上游（每 ref 只回这一条终稿）：
   `~/.local/bin/dais orchestration send-message <run_id> <你的邮箱> {HEAD} --message-type status --subject done --body "[ref:<ref>] <终稿正文含逐字凭证与署名标记>"`
   此前不另发受理确认或任何中间消息。
4. 终稿正文 ≤6 行：结论先行，凭证逐字，末行人格署名。
"""


async def main() -> int:
    t0 = time.time()
    print("== D0: spawn in-flight dsh session ==")
    spawn_out = sh([str(SESSION_SPAWN), "standard", "voice-orch-g4",
                    "G4 persona dressing live"])
    code = spawn_out.strip().splitlines()[-1].strip()
    fleet = json.loads(Path("~/.dsh/maestro/fleet.json").expanduser().read_text())
    entry = fleet["fleet"][code]
    session_id = entry["sessionId"]
    mailbox = f"agent_g4_{code}"
    ok0 = verdict(session_id.startswith("session-") and len(code) == 4,
                  "session-spawn 真身会话落地（fleet 在册）",
                  f"code={code} sid={session_id[:18]}… mailbox={mailbox}")

    print("== D1: pool/spawn *new* save + binding-mode dress (one RPC) ==")
    saved = rpc("pool/spawn", {
        "profile": "*new*", "name": PROFILE_NAME,
        "strategy": "binding-mode",
        "binding": {"sessionId": session_id},
        "role": "liaison", "mailbox": mailbox, "project": "voice-head",
        "projection": {"agents_md": PERSONA_AGENTS_MD,
                       "profile_json": {"agent_role": "liaison",
                                        "scenario": "人格穿衣验证"},
                       "description": "G4 dispatch 携 profile 穿衣 live 验证"},
    })
    bind = (saved.get("receipts") or [{}])[0]
    ok1 = verdict(bind.get("injected") is True and bind.get("target") == "binding",
                  "binding-mode 注入在飞会话（injected:true）",
                  f"receipt={json.dumps(bind, ensure_ascii=False)[:120]}")
    stored = rpc("profiles/get", {"name": PROFILE_NAME}).get("profile") or {}
    stored_ver = stored.get("version")
    ok1 &= verdict(str(bind.get("version")) == str(stored_ver)
                   == str((saved.get("profile") or {}).get("version")),
                   "版本钉死（信封引用库内 version，不重存）",
                   f"bind.v={bind.get('version')} store.v={stored_ver}")

    # give the dressed session's first turn a moment (envelope adoption)
    await asyncio.sleep(20)

    print("== D2: intent → O's mailbox + push wake ==")
    lane = DaisLane()
    ref = "vh-g4" + uuid.uuid4().hex[:6]
    cred = f"【凭证G4-{code.upper()}】"
    task = ("统计 ~/workspace-claw-02/pipecat-poc/src/pipecat/frames/frames.py "
            "中 class 定义的数量，结论一行给出数字。"
            f"回信正文必须逐字包含凭证标记 {cred}。")
    run_id = await lane.create_run(f"[voice-head-g4] {task[:120]}")
    seq = await lane.send_intent(run_id, mailbox, task, ref)
    wake = sh([str(SESSION_SEND), "orch1", session_id, "steer", "-",
               f"新任务到达：运行 check-messages {mailbox} --timeout-ms 2000 排空邮箱取正文，按你的 AGENTS 规约处理"],
              timeout_s=30)
    ok2 = verdict(seq >= 0 and "accepted=True" in wake,
                  "intent 落邮箱 + DSHMSG 推唤醒送达",
                  f"seq={seq} wake={wake.strip().splitlines()[-1][:60]!r}")

    print("== D3/D4: single done reply with credential + persona trace ==")
    body = ""
    deadline = time.time() + 240
    while time.time() < deadline:
        rows = await lane.check_messages(HEAD)
        for row in rows:
            if f"[ref:{ref}]" in row.get("body", "") and row.get("from") == mailbox:
                body = row["body"].replace(f"[ref:{ref}]", "").strip()
        if body:
            break
        await asyncio.sleep(3)
    ok3 = verdict(bool(body), "终稿唯一一条到达（[ref:] 匹配 + from=穿衣邮箱）",
                  f"body={body[:120]!r}")
    ok4 = verdict(cred in body, "凭证逐字回显", f"cred={cred!r}")
    ok5 = verdict(PERSONA_SIGN in body, "人格痕迹在场（穿衣被采纳）",
                  f"sign={PERSONA_SIGN!r}")

    ok = ok0 and ok1 and ok2 and ok3 and ok4 and ok5
    stamp = time.strftime("%Y-%m-%d %H:%M:%S")
    EVID.write_text(
        f"# LB-002-D (G4) 穿衣 live 取证 · {stamp}\n\n"
        f"- 会话: {code} ({session_id}) · 邮箱 {mailbox} · run {run_id} · ref {ref}\n"
        f"- profile: {PROFILE_NAME} (binding-mode, injected={bind.get('injected')})\n"
        f"- 终稿: {body!r}\n\n"
        "## 断言\n\n"
        + "\n".join(f"- [{'PASS' if o else 'FAIL'}] {l} — {d}"
                    for o, l, d in RESULTS)
        + f"\n\n判定: {'PASS' if ok else 'FAIL'} · 用时 {time.time() - t0:.0f}s\n")
    print(f"== evidence: {EVID} ==")
    print(f"LB-002-D dressing live {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
