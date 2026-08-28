"""E2E 验收探针（backend 级）：幽灵 worker 修复验证。
前置： liaison.json 不存在（Phase 0 后初始态）；db05 会话已归档。
流程：
  1. 伪造残留绑定 → liaison.json 指向已归档 db05 sessionId；
  2. 调 _liaison_ensure_sid（AUTO_NEW=1）→ 应核验归档态、清残留、换新 liaison；
  3. 断言新 sid ≠ 旧 sid、不在 archived、绑定文件指向新 sid、fleet.json 有新席位。
用法（需新代码 + 环境变量）:
  set -a; . ~/.config/voice-gateway/env; set +a
  .venv/bin/python /tmp/fleet_e2e_backend.py
输出: PASS/FAIL + 证据行；末尾打印新席位 code 供 ws cleanup 阶段使用。
"""
import asyncio
import json
import os
import sys
import time

REPO = "~/workspace-claw-02/pipecat-poc"
POC = f"{REPO}/examples/realtime-provider-poc"
sys.path.insert(0, POC)

STALE_CODE = "db05"
STALE_SID = "session-<id>"
LIAISON_PATH = os.path.expanduser("~/.local/state/voice-gateway/liaison.json")
FLEET_PATH = os.path.expanduser("~/.dsh/maestro/fleet.json")

results = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"{'PASS' if ok else 'FAIL'}  {name}  {detail}")


async def main():
    from rt_dsh_backend import DshBackend
    from rt_dsh_lane import DaisLane

    if os.environ.get("VOICE_LIAISON_AUTO_NEW") != "1":
        print("ENV 缺 VOICE_LIAISON_AUTO_NEW=1 —— 探针要求换新语义")
        return 2

    # 1. 伪造残留绑定
    os.makedirs(os.path.dirname(LIAISON_PATH), exist_ok=True)
    with open(LIAISON_PATH, "w") as f:
        json.dump({"code": STALE_CODE, "sessionId": STALE_SID,
                   "spawned_at": time.time()}, f)
    print(f"seeded stale binding -> {STALE_SID}")

    backend = DshBackend(lane=DaisLane(), liaison_session="")
    if not hasattr(DshBackend, "_liaison_bound_clear") or \
       not hasattr(DshBackend, "_archived_session_ids"):
        print("FAIL  新 API 缺失（_liaison_bound_clear/_archived_session_ids）—— 修复未落地")
        return 2

    sessions_value = await backend._dsh_api("session.list", {})
    archived = await backend._archived_session_ids() or set()
    check("db05 在归档集(前置)", STALE_SID in archived, f"|archived|={len(archived)}")

    # 2. ensure：应视为绑定失效并换新
    sid = await backend._liaison_ensure_sid(sessions_value)

    # 3. 断言
    check("换新生效", bool(sid) and sid != STALE_SID, f"new={sid}")
    archived2 = await backend._archived_session_ids() or set()
    check("新 sid 未归档", sid not in archived2)
    bound = json.load(open(LIAISON_PATH)) if os.path.exists(LIAISON_PATH) else {}
    check("绑定指向新会话", bound.get("sessionId") == sid, json.dumps(bound, ensure_ascii=False))
    check("旧 code 已不再被绑定引用", bound.get("code") != STALE_CODE,
          f"code={bound.get('code')}")
    fleet = json.load(open(FLEET_PATH))
    seats = {v.get("sessionId"): k for k, v in (fleet.get("fleet") or {}).items()}
    check("fleet.json 有新席位", sid in seats, f"code={seats.get(sid)}")
    check("db05 席位不在 fleet", STALE_CODE not in fleet.get("fleet", {}))

    print("\n=== 新席位（供 ws cleanup 阶段）===")
    print(json.dumps({"code": seats.get(sid), "sessionId": sid}, ensure_ascii=False))
    return 0 if all(ok for _, ok, _ in results) else 1


raise SystemExit(asyncio.run(main()))
