#!/usr/bin/env python3
#
# SPDX-License-Identifier: BSD 2-Clause License
#
"""TK-002 · 票板 tab 测试（<internal-repo> spec-tk-client §TK-002）.

- 纯函数面：normalize（deps/refs JSON 串容错整形）、kanban view（五列
  +「其他」兜底、updated_at 降序、同 state 必同视图=重放幂等键）、
  卡片行块（⛓deps 边/🏷refs chips/👤lease_owner/✅outcome）
- 集成面：op=tickets 经网关透传往返（真 rt_gateway + 假 SSE 源，
  G3 映射后网关走 GET /op/tickets）；事件面=失效通知（无载荷）。
  App 渲染管线（防抖重拉→覆写重绘）由联合门 xvfb 真 App 腿覆盖。
"""

from __future__ import annotations

import asyncio
import os
import re
import sys
import tempfile
import unittest
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

import queue  # noqa: E402


def _ticket(tid, state="running", updated="2026-08-29T10:00:00+00:00", **kw):
    base = {"ticket_id": tid, "title": kw.get("title", ""), "state": state,
            "deps": kw.get("deps", "[]"), "lease_owner": kw.get("lease_owner"),
            "refs": kw.get("refs", "{}"), "outcome": kw.get("outcome"),
            "updated_at": updated}
    return base


class Tk002PureTest(unittest.TestCase):
    """纯函数面：normalize / 列归属 / 视图确定性 / 卡片行块。"""

    def test_normalize_unwraps_json_strings(self):
        st = rva.tickets_normalize([_ticket(
            "A", title="第一行\n第二行", deps='["B","C"]',
            refs='{"evidence": "docs/a.md"}', lease_owner="w-1")])
        self.assertEqual(st["A"]["deps"], ["B", "C"])
        self.assertEqual(st["A"]["refs"], {"evidence": "docs/a.md"})
        self.assertEqual(st["A"]["lease_owner"], "w-1")
        self.assertNotIn("\n", st["A"]["title"])

    def test_normalize_tolerates_junk(self):
        st = rva.tickets_normalize([
            {"ticket_id": "A", "deps": "not json", "refs": "not json",
             "state": 7, "title": None},
            {"nope": 1}, "junk", {"ticket_id": "   "},
        ])
        self.assertEqual(st["A"]["deps"], ["not json"])     # 逗号退化
        self.assertEqual(st["A"]["refs"], {})
        self.assertEqual(st["A"]["state"], "7")
        self.assertEqual(len(st), 1)                        # 其余条目跳过

    def test_normalize_non_list(self):
        self.assertEqual(rva.tickets_normalize(None), {})
        self.assertEqual(rva.tickets_normalize("junk"), {})

    def test_column_routing_and_overflow(self):
        st = rva.tickets_normalize([_ticket("A", "dispatched"),
                                    _ticket("M", "merged"),
                                    _ticket("X", "weird"),
                                    _ticket("E", "")])
        self.assertEqual(rva.tickets_column_of(st["A"]), "dispatched")
        self.assertEqual(rva.tickets_column_of(st["M"]), "merged")
        self.assertEqual(rva.tickets_column_of(st["X"]), "其他")
        self.assertEqual(rva.tickets_column_of(st["E"]), "其他")

    def test_view_shape_and_ordering(self):
        state = rva.tickets_normalize([
            _ticket("T1", "done", updated="2026-08-29T10:00:00+00:00"),
            _ticket("T2", "done", updated="2026-08-29T11:00:00+00:00"),
            _ticket("R1", "running"),
            _ticket("X1", "weird"),
        ])
        view = rva.tickets_kanban_view(state)
        self.assertEqual(set(view), set(rva.TICKET_STATES) | {"其他"})
        self.assertEqual([ln.split(" ")[0] for ln in view["done"] if ln],
                         ["T2", "T1"])
        self.assertTrue(view["dispatched"] == [])
        # 未知态兜底列收容两张怪票
        self.assertIn("X1", "\n".join(view["其他"]))
        # 时间标签本地化格式（不锚定时区）
        self.assertTrue(re.match(r"^T2 · \d\d-\d\d \d\d:\d\d$",
                                 view["done"][0]))

    def test_view_deterministic_for_same_state(self):
        raw = [_ticket("A", "running"), _ticket("B", "done"),
               _ticket("C", "blocked", deps='["A"]')]
        v1 = rva.tickets_kanban_view(rva.tickets_normalize(raw))
        v2 = rva.tickets_kanban_view(rva.tickets_normalize(list(raw)))
        self.assertEqual(v1, v2)                            # 重放幂等键
        self.assertEqual(v1, rva.tickets_kanban_view(v1 and {
            k: dict(v) for k, v in rva.tickets_normalize(raw).items()}))

    def test_card_lines_segments(self):
        t = rva.tickets_normalize([_ticket(
            "A", "blocked", deps='["B","C"]', lease_owner="w-9",
            refs='{"zz": 1, "aa": 2}', outcome="已收口",
            title="标题")] )["A"]
        lines = rva.tickets_card_lines(t)
        self.assertTrue(lines[0].startswith("A · "))
        self.assertIn("标题", lines[1])
        self.assertIn("⛓ B，C", lines[2])
        self.assertIn("🏷 aa zz", lines[3])                 # chips=键名，字典序
        self.assertIn("👤 w-9", lines[4])
        self.assertIn("✅ 已收口", lines[5])

    def test_card_lines_omits_empty_segments(self):
        t = rva.tickets_normalize([_ticket("BARE", "dispatched", updated="")])["BARE"]
        lines = rva.tickets_card_lines(t)
        self.assertEqual(len(lines), 1)                     # 空段全部省略
        self.assertEqual(lines[0].rstrip(), "BARE ·")

    def test_time_label_fallback(self):
        self.assertEqual(rva.ticket_time_label("garbage"), "garbage")
        self.assertTrue(re.match(r"^\d\d-\d\d \d\d:\d\d$",
                                 rva.ticket_time_label("2026-08-29T10:00:00+00:00")))


class Tk002LinkTest(unittest.IsolatedAsyncioTestCase):
    """op=tickets 经网关透传往返（真网关 + 假 SSE 源 /op/tickets 路由）。"""

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

    async def test_tickets_full_roundtrip_through_gateway(self):
        gw = rt_gateway.VoiceGateway(port=0, token=TOKEN, head_provider=None,
                                     topic_sources=None)
        await gw.start()
        self.addAsyncCleanup(gw.stop)
        q: "queue.Queue[dict]" = queue.Queue()
        link = rva.ObserveLink(f"ws://127.0.0.1:{gw.port}/ws", TOKEN, q,
                               lambda s: None, pm_kinds=("fleet", "tickets"))
        self.links.append(link)
        link.start()
        inbox = Inbox(q)
        await inbox.wait(
            lambda f: f.get("t") == "pm.res"
            and isinstance(f.get("data"), dict)
            and f["data"].get("subscribed") == ["fleet", "tickets"],
            8, "订阅受理")
        link.send_request({"t": "pm.req", "id": "pmt-1",
                           "op": "tickets", "params": {}})
        res = await inbox.wait(
            lambda f: f.get("t") == "pm.res" and f.get("id") == "pmt-1",
            8, "op=tickets 回包")
        self.assertIn("data", res)
        self.assertEqual(res["data"]["count"], 2)
        state = rva.tickets_normalize(res["data"]["tickets"])
        view = rva.tickets_kanban_view(state)
        self.assertIn("T-A", "\n".join(view["running"]))
        # 同快照重渲染逐字一致（全量重放幂等键）
        self.assertEqual(view, rva.tickets_kanban_view(
            rva.tickets_normalize(res["data"]["tickets"])))


if __name__ == "__main__":
    unittest.main()
