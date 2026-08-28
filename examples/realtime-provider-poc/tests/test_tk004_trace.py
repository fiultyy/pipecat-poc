#!/usr/bin/env python3
#
# SPDX-License-Identifier: BSD 2-Clause License
#
"""TK-004 · 轨迹视图 tab 测试（<internal-repo> spec-tk-client §TK-004）.

- 纯函数面：查询参数组装（空值剔除/seq 收敛 int，参数组合=请求幂等键）、
  折叠快照展开续查参数、turn 分组（data.turn 编组，trace.compact 归前导）、
  行渲染（#seq·类型·工具名·摘要）与折叠摘要行、视图确定性（同 entries
  同视图=过滤器重放幂等键）、类型相着色映射
- 集成面：op=trace 经网关透传往返（真 rt_gateway + 假 SSE 源，G3 映射后
  网关走 GET /op/trace）；折叠 fixture → 视图含折叠摘要行、展开参数与
  fixture 边界一致。20k 字符滚动实测与真服大样本腿由联合门覆盖。
"""

from __future__ import annotations

import json
import queue
import sys
import tempfile
import unittest
import os
from pathlib import Path

POC_DIR = Path(__file__).resolve().parents[1]
if str(POC_DIR) not in sys.path:
    sys.path.insert(0, str(POC_DIR))

import rt_gateway  # noqa: E402
import rt_voice_app as rva  # noqa: E402
from tests.test_tk001_pm import (  # noqa: E402
    FakePMSSE,
    Inbox,
    TOKEN,
)


def _turn(n, seq):
    return {"type": "turn/start", "seq": seq, "time": 0, "data": {"turn": n}}


def _call(n, seq, name="bash"):
    return {"type": "tool/call", "seq": seq, "time": 0,
            "data": {"turn": n, "name": name, "command": "echo hi"}}


class Tk004PureTest(unittest.TestCase):
    """纯函数面：参数组装 / 展开参数 / 分组 / 行渲染 / 视图确定。"""

    def test_query_params_strips_and_coerces(self):
        p = rva.trace_query_params("s-1", " a , b ,, ", "bash", "",
                                   "12", "x")
        self.assertEqual(p, {"sessionId": "s-1", "type": "a,b",
                             "tool": "bash", "seqFrom": 12})
        self.assertEqual(rva.trace_query_params(None), {})
        self.assertEqual(rva.trace_query_params("", "", "", "", None, 3),
                         {"seqTo": 3})

    def test_params_key_is_idempotent(self):
        a = rva.trace_query_params("s-1", "tool/call", "", "hi", 5, 9)
        b = rva.trace_query_params("s-1", "tool/call", "", "hi", 5, 9)
        self.assertEqual(rva.trace_params_key(a), rva.trace_params_key(b))
        self.assertEqual(
            rva.trace_params_key(a),
            json.dumps(a, ensure_ascii=False, sort_keys=True))

    def test_expand_params_from_folded_snapshot(self):
        data = {"sessionId": "s-1", "folded": True,
                "filter": {"type": "tool/call", "tool": None,
                           "text": None, "seqFrom": None, "seqTo": None},
                "matched": {"seq_range": [3, 900]},
                "entries": [{"type": "trace.compact"},
                            {"type": "tool/call", "seq": 880}]}
        self.assertEqual(rva.trace_expand_params(data),
                         {"sessionId": "s-1", "type": "tool/call",
                          "seqFrom": 3, "seqTo": 879})
        self.assertIsNone(rva.trace_expand_params({"folded": False}))
        self.assertIsNone(rva.trace_expand_params(
            {"folded": True, "entries": [{"type": "trace.compact"}]}))
        self.assertIsNone(rva.trace_expand_params(None))

    def test_groups_by_turn_with_leading(self):
        entries = [{"type": "trace.compact", "seq": 0},
                   _turn(1, 2), _call(1, 3), _turn(2, 4),
                   _call(2, 5, "python")]
        groups = rva.trace_groups(entries)
        self.assertEqual([k for k, _ in groups],
                         ["· 前导", "turn 1", "turn 2"])
        self.assertEqual(len(groups[0][1]), 1)          # 摘要行归前导
        self.assertEqual(len(groups[1][1]), 2)
        self.assertEqual(len(groups[2][1]), 2)

    def test_line_and_fold_rendering(self):
        line = rva.trace_line_of(_call(1, 42, "bash"))
        self.assertTrue(line.startswith("#42 · tool/call[turn1] · bash"))
        self.assertIn("echo hi", line)
        long = {"type": "tool/call", "seq": 1,
                "data": {"turn": 1, "name": "x", "command": "ab " * 60}}
        self.assertLessEqual(len(rva.trace_line_of(long)), 120)
        fold = rva.trace_fold_line({
            "dropped": {"entries": 22530, "chars": 10921379},
            "kept": {"entries": 1, "chars": 1527}, "seq_range": [1, 193482]})
        self.assertIn("trace.compact", fold)
        self.assertIn("可展开", fold)
        self.assertIn("22530", fold)

    def test_view_deterministic_and_phases(self):
        entries = [_turn(1, 2), _call(1, 3, "bash"),
                   {"type": "step/start", "seq": 4, "data": {"turn": 1}}]
        v1 = rva.trace_view(list(entries))
        v2 = rva.trace_view(list(entries))
        self.assertEqual(v1, v2)                        # 重放幂等键
        tags = {t for t, _ in v1}
        self.assertIn("turn", tags)
        self.assertIn("tool", tags)
        self.assertIn("step", tags)
        self.assertTrue(any(ln.startswith("── turn 1") for _, ln in v1))

    def test_fold_line_in_view(self):
        entries = [{"type": "trace.compact", "dropped": {"entries": 2,
                                                          "chars": 600},
                    "kept": {"entries": 1, "chars": 100},
                    "seq_range": [1, 9]},
                   _turn(1, 5)]
        view = rva.trace_view(entries)
        self.assertTrue(any(t == "fold" and "可展开" in ln for t, ln in view))


class Tk004LinkTest(unittest.IsolatedAsyncioTestCase):
    """op=trace 经网关透传往返（真网关 + 假 SSE 源 /op/trace 路由）。"""

    async def asyncSetUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.port_file = Path(self._tmp.name) / "pm.port"
        self._old_env = os.environ.get("PM_HOST_PORT_FILE")
        os.environ["PM_HOST_PORT_FILE"] = str(self.port_file)
        rt_gateway._reset_pm_port_cache()
        self.fake = FakePMSSE()
        runner = __import__("aiohttp").web.AppRunner(
            self.fake.app(), handler_cancellation=True)
        await runner.setup()
        site = __import__("aiohttp").web.TCPSite(
            runner, "127.0.0.1", 0, shutdown_timeout=0.5)
        await site.start()
        self._runner = runner
        self.port_file.write_text(
            __import__("json").dumps({"port": runner.addresses[0][1]}))
        self.addAsyncCleanup(runner.cleanup)
        self.links = []

    async def asyncTearDown(self) -> None:
        for link in self.links:
            link.close()
        if self._old_env is None:
            os.environ.pop("PM_HOST_PORT_FILE", None)
        else:
            os.environ["PM_HOST_PORT_FILE"] = self._old_env
        rt_gateway._reset_pm_port_cache()
        self._tmp.cleanup()

    async def test_trace_full_roundtrip_through_gateway(self):
        gw = rt_gateway.VoiceGateway(port=0, token=TOKEN, head_provider=None,
                                     topic_sources=None)
        await gw.start()
        self.addAsyncCleanup(gw.stop)
        q: "queue.Queue[dict]" = queue.Queue()
        link = rva.ObserveLink(f"ws://127.0.0.1:{gw.port}/ws", TOKEN, q,
                               lambda s: None, pm_kinds=("tickets",))
        self.links.append(link)
        link.start()
        inbox = Inbox(q)
        await inbox.wait(
            lambda f: f.get("t") == "pm.res"
            and isinstance(f.get("data"), dict)
            and f["data"].get("subscribed") == ["tickets"],
            8, "订阅受理")
        params = rva.trace_query_params("s-fake", "", "", "", "", "")
        link.send_request({"t": "pm.req", "id": "pmt-1", "op": "trace",
                           "params": params})
        res = await inbox.wait(
            lambda f: f.get("t") == "pm.res" and f.get("id") == "pmt-1",
            8, "op=trace 回包")
        self.assertIn("data", res)
        self.assertTrue(res["data"]["folded"])
        view = rva.trace_view(res["data"]["entries"])
        self.assertTrue(any(t == "fold" for t, _ in view))
        self.assertTrue(any("turn 2" in ln for _, ln in view))
        # 过滤器重放同视图：同 entries 必同视图（客户端幂等键）
        self.assertEqual(view, rva.trace_view(list(res["data"]["entries"])))
        # 展开参数与 fixture 边界一致（保留区首条 seq 5 → seqTo=4）
        self.assertEqual(rva.trace_expand_params(res["data"]),
                         {"sessionId": "s-fake", "seqFrom": 1, "seqTo": 4})


if __name__ == "__main__":
    unittest.main()
