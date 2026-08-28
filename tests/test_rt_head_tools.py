#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Unit tests for the head tool surface over DshBackend (W1.4 + KG 14 PR4)."""

import asyncio
import inspect
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "realtime-provider-poc"))

from rt_dsh_backend import DshBackend  # noqa: E402
from rt_dsh_lane import DaisLane  # noqa: E402
from rt_event_bus import EventBus  # noqa: E402
from rt_head_tools import (  # noqa: E402
    DSH_TOOLS_DOCTRINE,
    dsh_head_tools,
    cancel_run_tool,
    dispatch_intent_tool,
    dispatch_plan_tool,
    edit_file_tool,
    find_files_tool,
    grep_files_tool,
    list_bodies_tool,
    query_status_tool,
    read_body_tool,
    read_file_tool,
    remain_silent_tool,
    write_file_tool,
)


@dataclass
class FakeParams:
    """Minimal FunctionCallParams stand-in: resources + captured callback."""

    app_resources: dict = field(default_factory=dict)
    results: list = field(default_factory=list)

    async def result_callback(self, value):
        self.results.append(value)


def make_backend(**overrides):
    state = {"ref": None}
    script = {
        "create-run": ["run_<redacted>\n"],
        "send-message": ["enqueued seq=1\n"],
        "check-messages": [
            'seq=5 from=session_orch to=voice-head type=status '
            'body=[ref:{ref}] 调研完成 【凭证R-7734】 结论 23%\n'
        ],
        "check-status": ["Run run_<redacted>: 2 tasks\n"],
        "fail-dispatch": ["ok\n"],
    }

    async def runner(argv):
        sub = argv[2]
        if sub == "send-message" and "--body" in argv:
            body = argv[argv.index("--body") + 1]
            m = re.search(r"\[ref:(vh-[0-9a-f]+)\]", body)
            if m:
                state["ref"] = m.group(1)
        out = script.get(sub, [""])[0]
        if "{ref}" in out and state["ref"]:
            out = out.replace("{ref}", state["ref"])
        return (out, "")

    finals = []

    async def on_final(ref, message):
        finals.append((ref, message))

    backend = DshBackend(
        lane=DaisLane(runner=runner),
        bus=EventBus(),
        orchestrator_handle="session_orch",
        on_final=on_final,
        await_timeout_s=1,
        **overrides,
    )
    return backend, finals, script


# ---- 台账只读两件套（KG 14 §2.3，PR4）：fake store 注入 app_resources ----


def make_rec(ref="vh-<redacted>", no=1, status="done", body="采纳方案B，收益约41%\n对比明细……",
             ts=1759300000.0):
    """台账条目替身（字段对齐 rt_session_store._COLUMNS，无 credentials）。"""
    first = body.splitlines()[0]
    return {"ref": ref, "no": no, "status": status, "title": first[:16],
            "summary": first[:60], "body": body, "chars": len(body), "ts": ts}


class FakeStore:
    """SessionStore.get/list 鸭子面替身（同步 API；真库另见 test_rt_session_store）。"""

    def __init__(self, recs):
        self.recs = {r["ref"]: dict(r) for r in recs}

    def get(self, ref):
        rec = self.recs.get(ref)
        return dict(rec) if rec else None

    def list(self, limit=50):
        rows = sorted(self.recs.values(), key=lambda r: r.get("no") or 0)
        if limit is not None and int(limit) <= 0:
            return []
        rows = rows[-int(limit):] if limit is not None else rows
        return [{k: v for k, v in r.items() if k != "body"} for r in rows]


class ExplodingStore:
    """get/list 一律抛错：C 降级 error 形（不炸）用。"""

    def get(self, ref):
        raise RuntimeError("sqlite locked")

    def list(self, limit=50):
        raise RuntimeError("sqlite locked")


def voice_params(store):
    return FakeParams(app_resources={"voice_store": store})


@pytest.mark.asyncio
async def test_dispatch_intent_tool_returns_phase1_receipt():
    backend, finals, _ = make_backend()
    params = FakeParams(app_resources={"dsh_backend": backend})
    await dispatch_intent_tool(params, "调研 WebGPU 现状")
    receipt = json.loads(params.results[0])
    assert receipt["status"] == "accepted"
    assert receipt["ref"].startswith("vh-")
    assert receipt["credentials"][0].startswith("【凭证")
    for t in backend._pending.values():
        t.cancel()


@pytest.mark.asyncio
async def test_dispatch_intent_tool_phase2_final_flows_to_on_final():
    backend, finals, _ = make_backend()
    params = FakeParams(app_resources={"dsh_backend": backend})
    await dispatch_intent_tool(params, "调研 X")
    for _ in range(100):
        if finals:
            break
        await asyncio.sleep(0.02)
    assert finals and finals[0][1].startswith('"Agent Final Message":')


@pytest.mark.asyncio
async def test_query_status_tool():
    """本地状态汇总：台账终态 ∪ 运行态；counts 齐口径；无台账降级带 note。"""
    import time as _time

    backend, _, _ = make_backend()
    backend._runs["vh-run1"] = type(
        "D", (), {"ts": _time.time() - 120, "credentials": []})()
    backend._runs["vh-done1"] = type("D", (), {"ts": 0})()
    store = FakeStore([
        make_rec(ref="vh-done1", no=1, status="done", body="结论甲" * 3),
        make_rec(ref="vh-old2", no=2, status="failed", body="失败正文"),
    ])
    params = FakeParams(app_resources={"dsh_backend": backend,
                                       "voice_store": store})
    await query_status_tool(params)
    out = params.results[0]
    assert out["status"] == "ok"
    assert out["counts"] == {"done": 1, "failed": 1, "running": 1}
    by_ref = {t["ref"]: t for t in out["tasks"]}
    assert by_ref["vh-done1"]["status"] == "done" and by_ref["vh-done1"]["no"] == 1
    assert by_ref["vh-run1"]["status"] == "running"
    assert 100 <= by_ref["vh-run1"]["elapsed_s"] <= 200
    assert "body" not in by_ref["vh-done1"]  # 正文不进状态面
    assert "note" not in out
    # 无台账：仅运行态 + note
    params2 = FakeParams(app_resources={"dsh_backend": backend})
    await query_status_tool(params2)
    out2 = params2.results[0]
    assert out2["tasks"] and all(t["status"] == "running" for t in out2["tasks"])
    assert "台账不可用" in out2["note"]


@pytest.mark.asyncio
async def test_cancel_run_tool():
    backend, _, script = make_backend()
    script["check-messages"] = ["(no messages)\n"]  # keep phase-2 pending
    params = FakeParams(app_resources={"dsh_backend": backend})
    await dispatch_intent_tool(params, "可取消任务")
    ref = json.loads(params.results[0])["ref"]
    await asyncio.sleep(0.05)
    await cancel_run_tool(params, ref)
    out = json.loads(params.results[-1])
    assert out["status"] == "canceled"


@pytest.mark.asyncio
async def test_remain_silent_tool():
    params = FakeParams()
    await remain_silent_tool(params)
    assert params.results == [{"status": "silent"}]


@pytest.mark.asyncio
async def test_cancel_run_tool_takes_ref_only():
    # PR4：参数名 ref，schema 面不再广告 run_id（裁决 #13 B 签名收敛）
    assert list(inspect.signature(cancel_run_tool).parameters) == ["params", "ref"]
    assert "run_id" not in (cancel_run_tool.__doc__ or "")


# ---- read_body / list_bodies（KG 14 裁决 #13：B 签名 + C 降级）----


@pytest.mark.asyncio
async def test_read_body_hit_by_ref():
    rec = make_rec(ref="vh-<redacted>", no=2, body="第一行结论\n第二行细节")
    params = voice_params(FakeStore([rec]))
    await read_body_tool(params, "vh-<redacted>")
    out = params.results[0]
    assert out == {"status": "ok", "ref": "vh-<redacted>", "no": 2,
                   "task_status": "done", "title": rec["title"],
                   "chars": len(rec["body"]), "body": rec["body"]}
    assert "truncated" not in out


@pytest.mark.asyncio
async def test_read_body_hit_by_spoken_no():
    recs = [make_rec(ref="vh-r1", no=1, body="任务一正文"),
            make_rec(ref="vh-r2", no=2, body="任务二正文")]
    params = voice_params(FakeStore(recs))
    await read_body_tool(params, 2)  # 用户念"任务2"：整型 no
    assert params.results[0]["ref"] == "vh-r2" and params.results[0]["body"] == "任务二正文"
    await read_body_tool(params, "2")  # 纯数字串同义
    assert params.results[-1]["ref"] == "vh-r2"


@pytest.mark.asyncio
async def test_read_body_miss_returns_original_ref_or_no():
    params = voice_params(FakeStore([make_rec(ref="vh-have", no=1)]))
    await read_body_tool(params, "vh-nobody")
    assert params.results[0] == {"status": "miss", "ref_or_no": "vh-nobody"}
    await read_body_tool(params, 99)  # no 解析不中也是 miss
    assert params.results[-1] == {"status": "miss", "ref_or_no": 99}


@pytest.mark.asyncio
async def test_read_body_degrades_without_store():
    for resources in ({}, {"voice_store": None}):
        params = FakeParams(app_resources=resources)
        await read_body_tool(params, "vh-x")
        assert params.results[0]["status"] == "error"
        assert params.results[0]["reason"] == "台账不可用"


@pytest.mark.asyncio
async def test_read_body_store_failure_degrades_to_error():
    params = voice_params(ExplodingStore())
    await read_body_tool(params, "vh-x")
    assert params.results[0]["status"] == "error"
    assert "台账读取失败" in params.results[0]["reason"]


@pytest.mark.asyncio
async def test_read_body_max_chars_truncates_from_head():
    body = "头" * 60 + "尾" * 60
    params = voice_params(FakeStore([make_rec(ref="vh-long", no=1, body=body)]))
    await read_body_tool(params, "vh-long", max_chars=20)
    out = params.results[0]
    assert out["chars"] == 120 and out["returned_chars"] == 20
    assert out["truncated"] is True and out["body"] == "头" * 20


@pytest.mark.asyncio
async def test_read_body_max_chars_live_drift_degrades_to_full():
    # live drift：数字串照常兑现（coercion），不可解析值退化为不截断，均不炸
    body = "头" * 60 + "尾" * 60
    params = voice_params(FakeStore([make_rec(ref="vh-long", no=1, body=body)]))
    await read_body_tool(params, "vh-long", max_chars="20")
    out = params.results[0]
    assert out["truncated"] is True and out["body"] == "头" * 20
    for bad in ("many", None):
        params = voice_params(FakeStore([make_rec(ref="vh-long", no=1, body=body)]))
        await read_body_tool(params, "vh-long", max_chars=bad)
        out = params.results[0]
        assert out["status"] == "ok" and out["body"] == body
        assert "truncated" not in out


@pytest.mark.asyncio
async def test_read_body_from_tail_takes_last_chars():
    body = "头" * 60 + "尾" * 60
    params = voice_params(FakeStore([make_rec(ref="vh-long", no=1, body=body)]))
    await read_body_tool(params, "vh-long", max_chars=20, from_tail=True)
    out = params.results[0]
    assert out["truncated"] is True and out["body"] == "尾" * 20
    # from_tail 无 max_chars 时不截断（无从取尾）
    params2 = voice_params(FakeStore([make_rec(ref="vh-long", no=1, body=body)]))
    await read_body_tool(params2, "vh-long", from_tail=True)
    assert params2.results[0]["body"] == body and "truncated" not in params2.results[0]


@pytest.mark.asyncio
async def test_list_bodies_shape_default_and_limit():
    recs = [make_rec(ref=f"vh-n{i:02d}", no=i, body=f"任务{i}正文") for i in range(1, 13)]
    params = voice_params(FakeStore(recs))
    await list_bodies_tool(params)  # 缺省 10 条（最近）
    out = params.results[0]
    assert out["status"] == "ok" and len(out["items"]) == 10
    assert [it["no"] for it in out["items"]] == list(range(3, 13))  # 最近 10、no 升序
    assert set(out["items"][0]) == {"no", "ref", "status", "title", "summary", "chars", "ts"}
    await list_bodies_tool(params, limit=3)
    assert [it["no"] for it in params.results[-1]["items"]] == [10, 11, 12]
    big = [make_rec(ref=f"vh-b{i:02d}", no=i) for i in range(1, 26)]
    params2 = voice_params(FakeStore(big))
    await list_bodies_tool(params2, limit=30)  # 上限 20
    assert len(params2.results[0]["items"]) == 20


@pytest.mark.asyncio
async def test_list_bodies_degrades():
    params = FakeParams(app_resources={})
    await list_bodies_tool(params)
    assert params.results[0] == {"status": "error", "reason": "台账不可用"}
    params2 = voice_params(ExplodingStore())
    await list_bodies_tool(params2)
    assert params2.results[0]["status"] == "error"


@pytest.mark.asyncio
async def test_dispatch_plan_tool_parses_and_dispatches_dag():
    import asyncio as _aio

    from rt_dsh_backend import DagTaskSpec

    captured: dict = {}

    async def fake_dispatch_dag(objective, tasks):
        captured["objective"] = objective
        captured["tasks"] = tasks
        return '{"status": "accepted", "tasks": 2}'

    backend, _, _ = make_backend()
    backend.dispatch_dag = fake_dispatch_dag  # type: ignore[method-assign]
    params = FakeParams(app_resources={"dsh_backend": backend})
    subtasks = [{"spec": "统计帧类", "command": "grep -c 'class.*Frame' a.py"},
                {"spec": "统计处理器", "deps": [0], "cmd": "grep -rc b"}]
    await dispatch_plan_tool(
        params, "对比两数",
        json.dumps(subtasks, ensure_ascii=False))
    receipt = json.loads(params.results[0])
    assert receipt["status"] == "accepted"
    assert captured["objective"] == "对比两数"
    tasks = captured["tasks"]
    assert all(isinstance(t, DagTaskSpec) for t in tasks)
    assert tasks[0].deps == [] and tasks[0].command.startswith("grep")
    assert tasks[1].deps == [0] and tasks[1].command == "grep -rc b"


@pytest.mark.asyncio
async def test_dispatch_plan_tool_clarify_on_garbage():
    backend, _, _ = make_backend()
    params = FakeParams(app_resources={"dsh_backend": backend})
    await dispatch_plan_tool(params, "目标", "不是JSON")
    assert json.loads(params.results[0])["status"] == "clarify"
    for t in backend._pending.values():
        t.cancel()


def test_dsh_head_tools_registry():
    names = {fn.__name__ for fn in dsh_head_tools()}
    assert names == {
        "dispatch_intent_tool", "dispatch_plan_tool", "query_status_tool",
        "read_body_tool", "list_bodies_tool", "cancel_run_tool",
        "remain_silent_tool", "find_files_tool", "grep_files_tool",
        "read_file_tool", "edit_file_tool", "write_file_tool",
    }
    assert len(dsh_head_tools()) == 12


def test_doctrine_covers_all_tools_and_two_phase_rule():
    for name in ("dispatch_intent", "dispatch_plan", "query_status",
                 "read_body", "list_bodies", "cancel_run", "remain_silent",
                 "find_files", "grep_files", "read_file", "edit_file",
                 "write_file"):
        assert name in DSH_TOOLS_DOCTRINE


def test_doctrine_four_section_shape():
    headers = ("# Persona and Role", "# Tools", "# After Tool Calls",
               "# Personality and Tone")
    positions = [DSH_TOOLS_DOCTRINE.find(h) for h in headers]
    assert all(p >= 0 for p in positions)
    assert positions == sorted(positions)


def test_doctrine_persona_section_keywords():
    persona = DSH_TOOLS_DOCTRINE.split("# Tools")[0]
    assert "Nova" in persona
    assert "任务助手" in persona
    assert "精炼表述" in persona and "状态优先" in persona
    assert "详情栏" in persona


def test_doctrine_numbering_protocol():
    assert "不念编号" in DSH_TOOLS_DOCTRINE
    assert "任务N" in DSH_TOOLS_DOCTRINE
    assert "完整 ref" in DSH_TOOLS_DOCTRINE
    # cancel_run 条款：head 从上下文回执解析 ref，不让用户念编号
    assert "由你从上下文" in DSH_TOOLS_DOCTRINE
    assert "不让用户念编号" in DSH_TOOLS_DOCTRINE


def test_doctrine_receipt_rule_no_verbatim_relay():
    after = DSH_TOOLS_DOCTRINE.split("# After Tool Calls")[1]
    # 受理回执只回一个状态；不念凭证语义（防 SLIM=0 全形回退时照念）
    assert "只回一个状态" in after
    assert "不念凭证" in after
    assert "不念 ref" in after
    assert "不念 run_id" in after
    assert "不复述" in after and "JSON" in after
    # 明令封掉的啰嗦尾巴
    assert "详情栏可看进度" not in after
    # 终稿/通报通用条款：罩住全文注入与 [编排通报] JSON 两种载荷
    assert "Agent Final Message" in after
    assert "[编排通报]" in after
    # 连续工具调用中间步不出声
    assert "中间步骤不出声" in after
    # 旧逐字转述条款已废除
    assert "逐字" not in DSH_TOOLS_DOCTRINE
    assert "一个字符" not in DSH_TOOLS_DOCTRINE


def test_doctrine_pr4_tool_clauses():
    # read_body 条款：用户要正文时用；ref 或用户念的编号；超长分段
    tools = DSH_TOOLS_DOCTRINE.split("# Tools")[1].split("# After Tool Calls")[0]
    read_clause = next(ln for ln in tools.splitlines() if ln.startswith("- read_body"))
    assert "终稿正文" in read_clause and "完整 ref" in read_clause
    assert "编号" in read_clause and "max_chars" in read_clause and "from_tail" in read_clause
    # list_bodies 条款："都有什么任务/什么状态"列台账
    list_clause = next(ln for ln in tools.splitlines() if ln.startswith("- list_bodies"))
    assert "都有什么任务" in list_clause and "索引" in list_clause


def test_doctrine_after_calls_covers_read_tools():
    after = DSH_TOOLS_DOCTRINE.split("# After Tool Calls")[1]
    clause = next(ln for ln in after.splitlines() if "read_body/list_bodies" in ln)
    assert "按用户所问" in clause and "结构与要点" in clause
    # C 降级话术：miss/error 各有口径，不编造
    assert "miss" in clause and "error" in clause
    assert "暂不可用" in clause and "不编造" in clause


def test_doctrine_file_tools_clause():
    """文件工具条款：读取类不出声；edit/write 一句话状态；不倒 diff。"""
    after = DSH_TOOLS_DOCTRINE.split("# After Tool Calls")[1]
    clause = next(ln for ln in after.splitlines() if "find_files/grep_files" in ln)
    assert "中间步骤不出声" in clause and "改好了" in clause
    assert "不念文件内容" in clause and "diff" in clause
    assert "不编造" in clause
    tools = DSH_TOOLS_DOCTRINE.split("# Tools")[1].split("# After Tool Calls")[0]
    edit_clause = next(ln for ln in tools.splitlines() if ln.startswith("- edit_file"))
    assert "完全一致" in edit_clause and "replace_all" in edit_clause


# ---- 工作区文件五件套 ----


def ws_params(root) -> FakeParams:
    return FakeParams(app_resources={"workspace_root": str(root)})


@pytest.fixture
def ws(tmp_path):
    """最小工作区：源码 + 子包 + 垃圾目录 + 二进制文件。"""
    (tmp_path / "app.py").write_text(
        "def run():\n    return 1\n\n\ndef stop():\n    return run()\n",
        encoding="utf-8")
    pkg = tmp_path / "pkg"
    pkg.mkdir()
    (pkg / "util.py").write_text("VALUE = 7\n", encoding="utf-8")
    (tmp_path / "notes.md").write_text("# 笔记\n正文\n", encoding="utf-8")
    junk = tmp_path / "node_modules"
    junk.mkdir()
    (junk / "j.js").write_text("junk junk junk\n", encoding="utf-8")
    gitdir = tmp_path / ".git"
    gitdir.mkdir()
    (gitdir / "HEAD").write_text("ref: refs/heads/main\n", encoding="utf-8")
    (tmp_path / "data.bin").write_bytes(b"\x00\x01\x02binary")
    return tmp_path


@pytest.mark.asyncio
async def test_file_tools_without_workspace_root(ws):
    params = FakeParams(app_resources={})
    for fn, args in ((find_files_tool, ("*.py",)), (grep_files_tool, ("run",)),
                     (read_file_tool, ("app.py",)),
                     (edit_file_tool, ("app.py", "a", "b")),
                     (write_file_tool, ("x.txt", "hi"))):
        await fn(params, *args)
    assert all(r == {"status": "error", "reason": "工作区不可用"}
               for r in params.results)


@pytest.mark.asyncio
async def test_read_file_window_numbering_and_clip(ws):
    params = ws_params(ws)
    long_line = "x" * 1500
    (ws / "long.py").write_text("one\n" + long_line + "\nthree\n", encoding="utf-8")
    await read_file_tool(params, "long.py", offset=2, limit=2)
    out = params.results[0]
    assert out["status"] == "ok" and out["total_lines"] == 3
    assert out["offset"] == 2 and out["returned"] == 2
    assert out["lines"][0].startswith("2: ") and out["lines"][0].endswith("…")
    assert len(out["lines"][0]) <= 1005
    assert out["lines"][1] == "3: three"
    # 缺省窗口：整文件（小文件）；limit 上限收敛
    await read_file_tool(params, "pkg/util.py")
    assert params.results[-1]["lines"] == ["1: VALUE = 7"]
    await read_file_tool(params, "app.py", limit="9999")  # 字串数字也要收敛
    assert params.results[-1]["returned"] == 6  # app.py 共 6 行（上限 1000 未起作用）


@pytest.mark.asyncio
async def test_read_file_miss_and_escape(ws):
    params = ws_params(ws)
    await read_file_tool(params, "nope.py")
    assert params.results[0] == {"status": "miss", "path": "nope.py"}
    for escape in ("../../etc/passwd", "/etc/passwd"):
        await read_file_tool(params, escape)
        assert params.results[-1]["status"] == "error" and "越界" in params.results[-1]["reason"]
    await read_file_tool(params, "")
    assert params.results[-1]["status"] == "error"


@pytest.mark.asyncio
async def test_find_files_glob_skips_junk(ws):
    params = ws_params(ws)
    await find_files_tool(params, "*.py")
    out = params.results[0]
    assert out["status"] == "ok" and out["files"] == ["app.py", "pkg/util.py"]
    await find_files_tool(params, "pkg/*.py")  # 含 "/"：按相对路径整体匹配
    assert params.results[-1]["files"] == ["pkg/util.py"]
    await find_files_tool(params, "*.zzz")
    assert params.results[-1]["count"] == 0
    await find_files_tool(params, "*", limit=1)
    assert len(params.results[-1]["files"]) == 1  # limit 生效


@pytest.mark.asyncio
async def test_grep_files_match_include_limit_scope(ws):
    params = ws_params(ws)
    await grep_files_tool(params, "return")  # app.py 两处 + 垃圾目录已被剪掉
    out = params.results[0]
    assert out["status"] == "ok" and out["count"] == 2
    assert all(m["path"] == "app.py" for m in out["matches"])
    assert out["matches"][0] == {"path": "app.py", "line": 2, "text": "return 1"}
    await grep_files_tool(params, "return", path="pkg")  # 子目录无命中
    assert params.results[-1]["count"] == 0
    await grep_files_tool(params, "junk")  # node_modules 被剪，搜不到
    assert params.results[-1]["count"] == 0
    await grep_files_tool(params, "run", limit=1)
    out = params.results[-1]
    assert out["count"] == 1 and out["truncated"] is True
    await grep_files_tool(params, "(unclosed")  # 非法正则
    assert params.results[-1]["status"] == "error" and "正则" in params.results[-1]["reason"]
    await grep_files_tool(params, "run", path="nope/")  # 子目录不存在
    assert params.results[-1] == {"status": "miss", "path": "nope/"}
    await grep_files_tool(params, "binary")  # 二进制（含 \x00）跳过
    assert params.results[-1]["count"] == 0


@pytest.mark.asyncio
async def test_edit_file_unique_ambiguous_replace_all(ws):
    params = ws_params(ws)
    await edit_file_tool(params, "app.py", "return 1", "return 42")
    out = params.results[0]
    assert out["status"] == "ok" and out["replacements"] == 1
    assert "return 42" in (ws / "app.py").read_text(encoding="utf-8")
    dup = ws / "dup.txt"
    dup.write_text("foo A\nfoo B\n", encoding="utf-8")
    await edit_file_tool(params, "dup.txt", "foo ", "bar ")  # 两处：拒绝
    out = params.results[-1]
    assert out["status"] == "error" and "2 处匹配" in out["reason"]
    assert dup.read_text(encoding="utf-8") == "foo A\nfoo B\n"  # 未写入
    await edit_file_tool(params, "dup.txt", "foo ", "bar ", replace_all=True)
    assert params.results[-1]["replacements"] == 2
    assert dup.read_text(encoding="utf-8") == "bar A\nbar B\n"
    await edit_file_tool(params, "app.py", "不存在原文", "x")  # miss
    assert params.results[-1]["status"] == "miss"
    await edit_file_tool(params, "../../etc/passwd", "a", "b")  # 越界
    assert params.results[-1]["status"] == "error" and "越界" in params.results[-1]["reason"]


@pytest.mark.asyncio
async def test_write_file_create_overwrite_cap(ws):
    params = ws_params(ws)
    await write_file_tool(params, "docs/new/plan.md", "# 计划\n")  # 含父目录创建
    out = params.results[0]
    assert out["status"] == "ok" and out["created"] is True
    assert (ws / "docs" / "new" / "plan.md").read_text(encoding="utf-8") == "# 计划\n"
    await write_file_tool(params, "docs/new/plan.md", "# 改\n")
    assert params.results[-1]["created"] is False
    assert (ws / "docs" / "new" / "plan.md").read_text(encoding="utf-8") == "# 改\n"
    await write_file_tool(params, "big.txt", "x" * 65537)  # 超上限拒绝
    assert params.results[-1]["status"] == "error" and "上限" in params.results[-1]["reason"]
    assert not (ws / "big.txt").exists()
