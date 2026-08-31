#!/usr/bin/env python3
#
# SPDX-License-Identifier: BSD 2-Clause License
#
"""TK-005 · tk 端对齐 Android 修复（松键尾 2000ms 标定 + 按住中让位）.

- vad_tail_plan(2000,50) → 40 步、时刻单调、每步 1600B（32KB/s 真实码率）；
  VAD_TAIL_MS 标定常量 2000（Android 真机：endpointer 需 ≥~1.3s，600ms
  三次未闭句，2s 稳定 ~1.4s 闭句——引 <internal-repo> a931c19/AND4-5）
- worker 回合闸门（mock held_fn/yield_fn）：user_end / assistant_start /
  首个下行 binary 在按住中触发让位并开静音窗；未按住 no-op；mute 窗内
  未按住的下行照旧丢弃；user_start/interrupted 打断语义不变
- UI 让位幂等（_yield_held_mic，无 tkinter 壳）：held 才停麦+清尾+释放
  持有；松键先行 no-op；yield 后迟到松键无动作；再按 → 全新采集轮
"""

from __future__ import annotations

import queue
import sys
import types
import unittest
from pathlib import Path

POC_DIR = Path(__file__).resolve().parents[1]
if str(POC_DIR) not in sys.path:
    sys.path.insert(0, str(POC_DIR))

import rt_voice_app as rva  # noqa: E402


class VadTailPlanTest(unittest.TestCase):
    """尾静音投递表：2000ms/50ms → 40 步（TK-005 标定）。"""

    def test_2000ms_at_50ms_is_40_steps(self):
        plan = rva.vad_tail_plan(2000, 50)
        self.assertEqual(40, len(plan))
        self.assertEqual([(50 * (k + 1), 1600) for k in range(40)], plan)

    def test_times_monotonic_and_bytes_at_real_rate(self):
        plan = rva.vad_tail_plan(rva.VAD_TAIL_MS, rva.VAD_TAIL_STEP_MS)
        times = [t for t, _ in plan]
        self.assertEqual(sorted(times), times)
        self.assertTrue(
            all(b == 2 * rva.RATE * rva.VAD_TAIL_STEP_MS // 1000 for _, b in plan))

    def test_calibration_constant_is_2000(self):
        self.assertEqual(2000, rva.VAD_TAIL_MS)

    def test_nonpositive_is_empty(self):
        self.assertEqual([], rva.vad_tail_plan(0, 50))
        self.assertEqual([], rva.vad_tail_plan(2000, 0))


def make_link(held: bool):
    """VoiceLink + 持有态探针：held 可运行时翻转，yield 调用被记录。"""
    st = {"held": held}
    yields: list[int] = []
    link = rva.VoiceLink("ws://example/ws", "tok",
                         queue.Queue(), queue.Queue(), lambda _s: None,
                         held_fn=lambda: st["held"],
                         yield_fn=lambda: yields.append(1))
    return link, st, yields


class HeadTurnYieldTest(unittest.TestCase):
    """worker 回合闸门：按住中让位（对齐 Android autoYield）。"""

    def test_user_end_while_held_yields_and_unmutes(self):
        link, _st, yields = make_link(held=True)
        link._mute = True  # 按住中 head 已 user_start → mute 窗开着
        link._on_head_turn("user_end")
        self.assertEqual([1], yields)
        self.assertFalse(link._mute)

    def test_user_end_not_held_is_noop(self):
        link, _st, yields = make_link(held=False)
        link._mute = True
        link._on_head_turn("user_end")
        self.assertEqual([], yields)
        self.assertTrue(link._mute)  # 未按住：不动静音窗

    def test_assistant_start_while_held_yields(self):
        link, _st, yields = make_link(held=True)
        link._mute = True
        link._on_head_turn("assistant_start")
        self.assertEqual([1], yields)
        self.assertFalse(link._mute)

    def test_assistant_start_not_held_unmutes_without_yield(self):
        link, _st, yields = make_link(held=False)
        link._mute = True
        link._on_head_turn("assistant_start")
        self.assertEqual([], yields)
        self.assertFalse(link._mute)  # 既有语义：开窗

    def test_user_start_interrupt_keeps_mute_semantics(self):
        link, _st, yields = make_link(held=True)
        link._on_head_turn("user_start")
        self.assertTrue(link._mute)
        self.assertEqual([], yields)
        link._on_head_turn("interrupted")
        self.assertTrue(link._mute)
        self.assertEqual([], yields)


class DownlinkBinaryYieldTest(unittest.TestCase):
    """下行帧闸门：按住中首个 binary 即让位（判点照 mute 丢帧路径）。"""

    def test_first_binary_while_held_yields_and_passes(self):
        link, _st, yields = make_link(held=True)
        link._mute = True  # AND4-4 场景：mute 窗正吞回复
        link._on_downlink_binary(b"\x01\x02")
        self.assertEqual([1], yields)
        self.assertFalse(link._mute)
        self.assertEqual(1, link._play_q.qsize())  # 触发帧本身放行

    def test_binary_not_held_muted_drops_without_yield(self):
        link, _st, yields = make_link(held=False)
        link._mute = True
        link._on_downlink_binary(b"\x01")
        self.assertEqual([], yields)
        self.assertTrue(link._mute)
        self.assertEqual(0, link._play_q.qsize())

    def test_binary_not_held_unmuted_passes_without_yield(self):
        link, _st, yields = make_link(held=False)
        link._on_downlink_binary(b"\x01")
        self.assertEqual([], yields)
        self.assertEqual(1, link._play_q.qsize())


def bare_app(held: bool):
    """无 tkinter 的 App 壳：只为 _yield_held_mic 的持有态状态机。"""
    app = rva.App.__new__(rva.App)
    app.ptt = rva.PushToTalk()
    if held:
        app.ptt.press()
    app._tail_after = "after-1"
    cancelled: list[str] = []
    app.root = types.SimpleNamespace(after_cancel=lambda h: cancelled.append(h))
    stops: list[str] = []
    app._mic_stop = lambda: stops.append("mic_stop")
    logs: list[str] = []
    app.log_write = lambda msg: logs.append(msg)
    return app, cancelled, stops, logs


class YieldHeldMicTest(unittest.TestCase):
    """UI 让位幂等：松键先行 no-op；迟到松键无动作；再按新轮。"""

    def test_held_yields_stops_mic_and_cancels_tail(self):
        app, cancelled, stops, logs = bare_app(held=True)
        app._yield_held_mic()
        self.assertFalse(app.ptt.held)
        self.assertEqual(["after-1"], cancelled)
        self.assertEqual(["mic_stop"], stops)
        self.assertTrue(any("让位" in m for m in logs))

    def test_not_held_is_noop(self):
        app, cancelled, stops, _logs = bare_app(held=False)
        app._yield_held_mic()
        self.assertEqual([], cancelled)
        self.assertEqual([], stops)

    def test_late_release_after_yield_is_noop(self):
        app, _cancelled, stops, _logs = bare_app(held=True)
        app._yield_held_mic()
        self.assertIsNone(app.ptt.release())  # 迟到的松键：无动作
        self.assertEqual(["mic_stop"], stops)  # 不二次停麦

    def test_press_after_yield_starts_fresh_round(self):
        app, _cancelled, _stops, _logs = bare_app(held=True)
        app._yield_held_mic()
        self.assertEqual("start", app.ptt.press())  # 再按 → 新采集轮
        self.assertTrue(app.ptt.held)


if __name__ == "__main__":
    unittest.main()
