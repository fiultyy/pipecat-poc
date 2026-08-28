#!/usr/bin/env python3
#
# SPDX-License-Identifier: BSD 2-Clause License
#
"""TK-003 · 席位舰 tab 测试（<internal-repo> spec-tk-client §TK-003）.

- 纯函数面：两源一卡（fleet.json 全文卡带租约/准入/换代 vs op=fleet
  投影 seats 卡缺省降级）、准入态透出（probing/verified/mismatch 原样，
  stale 按（可配）阈值派生）、lastSeen 时长、租约到期倒计时、换代中
  瞬态行、视图确定性（同 state+now 同视图=重放幂等键）
- 集成面：op=fleet 经网关透传往返（真 rt_gateway + 假 SSE 源，G3 映射
  后网关走 GET /op/fleet）；事件面=fleet.kind 失效通知（无载荷）。
  App 渲染管线（防抖重拉→覆写重绘）与尾随 fleet.snapshot 全文面由联合
  门 xvfb 真 App 腿覆盖。
"""

from __future__ import annotations

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

NOW = 1_800_000_000.0


def _maestro(code="0699", **kw):
    e = {"sessionId": f"s-{code}", "role": "worker", "node": "gw-002",
         "preset": "maestro", "spawnedAt": "2026-08-28T17:05:11+00:00",
         "status": "active"}
    e.update(kw)
    return e


def _orca(code="t9ab", **kw):
    e = {"kind": "orca-terminal", "handle": f"h-{code}",
         "status": "probing", "alias": "dev", "lastSeenAt": NOW - 5}
    e.update(kw)
    return e


class Tk003PureTest(unittest.TestCase):
    """纯函数面：两源一卡 / 准入态 / 时长 / 租约 / 换代瞬态 / 视图确定。"""

    def test_cards_doc_unwraps_and_normalizes(self):
        doc = {"port": 3080, "fleet": {"0699": _maestro()}}
        cards = rva.fleet_ship_cards_doc(doc)
        self.assertEqual(set(cards), {"0699"})
        c = cards["0699"]
        self.assertEqual(c["kind"], "maestro")          # 缺 kind 缺省
        self.assertEqual(c["status"], "active")
        self.assertEqual(c["preset"], "maestro")
        self.assertIsNone(c["lease_until"])             # 无租约 → None
        self.assertFalse(c["handover"])

    def test_cards_doc_flat_shape_and_junk(self):
        cards = rva.fleet_ship_cards_doc({"fleet": {
            "t9ab": _orca(status="verified"),
            "bad": "junk", "skip": None}})
        self.assertEqual(set(cards), {"t9ab"})
        self.assertEqual(cards["t9ab"]["term"], "h-t9ab")
        self.assertEqual(rva.fleet_ship_cards_doc(None), {})
        self.assertEqual(rva.fleet_ship_cards_doc("junk"), {})

    def test_cards_seats_projection(self):
        seats = [{"code": "0699", "status": "active", "preset": "maestro",
                  "spawnedAt": "2026-08-28T17:05:11+00:00",
                  "session": {"running": True, "blank": False}},
                 {"nope": 1}, "junk"]
        cards = rva.fleet_ship_cards_seats(seats)
        self.assertEqual(set(cards), {"0699"})
        self.assertTrue(cards["0699"]["running"])       # session join 并入
        self.assertIsNone(cards["0699"]["lease_until"])  # 投影无租约字段
        self.assertEqual(rva.fleet_ship_cards_seats(None), {})

    def test_verify_states_and_configurable_stale(self):
        fresh = rva.fleet_ship_card_of("t9ab", _orca())          # lastSeen 5s 前
        old = rva.fleet_ship_card_of(
            "t0cd", _orca("t0cd", status="verified", lastSeenAt=NOW - 301))
        self.assertEqual(rva.fleet_verify_of(fresh, NOW, 120), "probing")
        # verified 原样透出（准入态优先于 stale 派生）
        self.assertEqual(rva.fleet_verify_of(old, NOW, 120), "verified")
        maestro = rva.fleet_ship_card_of("0699", _maestro(spawnedAt=NOW - 30))
        self.assertEqual(rva.fleet_verify_of(maestro, NOW, 120), "—")
        stale = rva.fleet_ship_card_of(
            "t0ce", _orca("t0ce", status="active", lastSeenAt=NOW - 200))
        # 阈值可配：300s 阈值下 200s 前的 lastSeen 不算 stale
        self.assertEqual(rva.fleet_verify_of(stale, NOW, 300), "—")
        self.assertEqual(rva.fleet_verify_of(stale, NOW, 120), "stale")

    def test_last_seen_and_lease_labels(self):
        self.assertEqual(rva.fleet_last_seen_label(None, NOW), "—")
        self.assertEqual(rva.fleet_last_seen_label(NOW - 5, NOW), "5s前")
        self.assertEqual(rva.fleet_last_seen_label(NOW - 130, NOW), "2m前")
        self.assertEqual(rva.fleet_last_seen_label(NOW - 7300, NOW), "2h前")
        card = {"lease_until": NOW + 252, "lease_owner": "<orchestrator>"}
        self.assertEqual(rva.fleet_lease_label(card, NOW),
                         "⏳租约 <orchestrator> 还剩4m12s")
        self.assertIn("已过期", rva.fleet_lease_label(
            {"lease_until": NOW - 61, "lease_owner": None}, NOW))
        self.assertEqual(rva.fleet_lease_label({"lease_until": None}, NOW), "")

    def test_card_lines_segments_and_handover(self):
        card = rva.fleet_ship_card_of("0699", _maestro(
            owner="<orchestrator>", leaseExpiresAt="2026-08-28T18:00:00+00:00",
            spawnedAt="2026-08-28T17:05:11+00:00"))
        now = datetime_from_iso("2026-08-28T17:06:00+00:00")
        lines = rva.fleet_ship_card_lines(card, now)
        self.assertTrue(lines[0].startswith("0699 · active · maestro"))
        self.assertIn("term —", lines[1])
        self.assertIn("lastSeen 49s前", lines[1])
        self.assertIn("gw-002 · worker", lines[2])
        self.assertIn("⏳租约 <orchestrator> 还剩", "\n".join(lines))
        self.assertNotIn("换代中", "\n".join(lines))
        retire = rva.fleet_ship_card_of("t0cd", _orca("t0cd", retiring=True))
        self.assertIn("⟳ 换代中", "\n".join(
            rva.fleet_ship_card_lines(retire, NOW)))
        retire2 = rva.fleet_ship_card_of(
            "t0ce", _orca("t0ce", status="retiring"))
        self.assertIn("⟳ 换代中", "\n".join(
            rva.fleet_ship_card_lines(retire2, NOW)))

    def test_view_deterministic(self):
        doc = {"fleet": {"b9be": _maestro("b9be"),
                         "0699": _maestro(owner="o", leaseExpiresAt="x")}}
        cards = rva.fleet_ship_cards_doc(doc)
        v1 = rva.fleet_ship_view(cards, NOW)
        v2 = rva.fleet_ship_view(dict(cards), NOW)
        self.assertEqual(v1, v2)                        # 重放幂等键
        self.assertEqual([ln.split(" ")[0] for ln in v1 if ln][0], "0699")


def datetime_from_iso(s: str) -> float:
    import datetime as _dt
    return _dt.datetime.fromisoformat(s).timestamp()


class Tk003LinkTest(unittest.IsolatedAsyncioTestCase):
    """op=fleet 经网关透传往返（真网关 + 假 SSE 源 /op/fleet 路由）。"""

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

    async def test_fleet_full_roundtrip_through_gateway(self):
        gw = rt_gateway.VoiceGateway(port=0, token=TOKEN, head_provider=None,
                                     topic_sources=None)
        await gw.start()
        self.addAsyncCleanup(gw.stop)
        q: "queue.Queue[dict]" = queue.Queue()
        link = rva.ObserveLink(f"ws://127.0.0.1:{gw.port}/ws", TOKEN, q,
                               lambda s: None, pm_kinds=("fleet",))
        self.links.append(link)
        link.start()
        inbox = Inbox(q)
        await inbox.wait(
            lambda f: f.get("t") == "pm.res"
            and isinstance(f.get("data"), dict)
            and f["data"].get("subscribed") == ["fleet"],
            8, "订阅受理")
        link.send_request({"t": "pm.req", "id": "pmf-1",
                           "op": "fleet", "params": {}})
        res = await inbox.wait(
            lambda f: f.get("t") == "pm.res" and f.get("id") == "pmf-1",
            8, "op=fleet 回包")
        self.assertIn("data", res)
        self.assertEqual(res["data"]["count"], 2)
        cards = rva.fleet_ship_cards_seats(res["data"]["seats"])
        self.assertEqual(set(cards), {"0699", "b9be"})
        view = rva.fleet_ship_view(cards, NOW)
        self.assertIn("0699 · active · maestro", "\n".join(view))
        # 同快照重渲染逐字一致（全量重放幂等键）
        self.assertEqual(view, rva.fleet_ship_view(
            rva.fleet_ship_cards_seats(res["data"]["seats"]), NOW))


if __name__ == "__main__":
    unittest.main()
