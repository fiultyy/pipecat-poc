#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Unit tests for the client-side transcript state machine."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "realtime-provider-poc"))

from rt_transcript import DEFAULT_MAX_TRANSCRIPT_BYTES, TranscriptState


def test_delta_accumulation_and_done_correction():
    st = TranscriptState()
    st.on_speech_started()
    st.on_input_delta("帮我查")
    st.on_input_delta("一下订单")  # same turn, no speech_started between
    assert st.snapshot() == [] or st.snapshot()[0].text == "帮我查一下订单"
    st.on_input_done("帮我查一下订单 ORD-1")  # authoritative replace
    assert st.entries[-1].text == "帮我查一下订单 ORD-1"


def test_new_entry_on_speech_started():
    st = TranscriptState()
    st.on_speech_started()
    st.on_input_delta("first")
    st.on_input_delta(" more")  # still same entry
    st.on_speech_started()  # new utterance
    st.on_input_delta("second")
    texts = [e.text for e in st.entries if e.role == "user"]
    assert texts == ["first more", "second"]


def test_roles_do_not_merge():
    st = TranscriptState()
    st.on_input_delta("user says")
    st.on_output_delta("bot replies")  # different role -> new entry
    assert [(e.role, e.text) for e in st.entries] == [
        ("user", "user says"),
        ("assistant", "bot replies"),
    ]


def test_handoff_input_dedupe():
    st = TranscriptState()
    st.on_handoff_input("调研 WebGPU")
    st.on_handoff_input("调研 WebGPU")  # duplicate suppressed
    assert len([e for e in st.entries if e.role == "user"]) == 1


def test_byte_budget_truncation():
    st = TranscriptState(max_bytes=200)
    st.seed("user", "x" * 150)
    st.seed("assistant", "y" * 150)  # pushes over budget -> oldest dropped
    assert len(st.entries) == 1
    assert st.entries[0].text.startswith("y" * 10)


def test_take_tail_drains():
    st = TranscriptState()
    st.seed("user", "hello")
    drained = asyncio.run(st.take_tail())
    assert [(e.role, e.text) for e in drained] == [("user", "hello")]
    assert st.entries == []
    # A second drain returns nothing: the swap is destructive.
    assert asyncio.run(st.take_tail()) == []


def test_oversized_single_entry_gets_prefixed():
    st = TranscriptState(max_bytes=100)
    st.seed("user", "z" * 400)
    assert st.entries[0].text.startswith("[truncated] ")
    assert len(st.entries[0].text.encode()) <= 100


def test_tiny_budget_does_not_reverse_slice():
    # budget < len("[truncated] "): the keep count must clamp to 0, not go
    # negative (a negative slice keeps the TAIL and bypasses truncation).
    st = TranscriptState(max_bytes=10)
    st.seed("user", "z" * 400)
    assert st.entries[0].text == "[truncated] "


def test_reseed_text_form():
    st = TranscriptState()
    st.seed("user", "查订单")
    st.seed("assistant", "好的 ORD-9")
    assert st.as_text() == "user: 查订单\nassistant: 好的 ORD-9"


def run_all():
    fns = [v for k, v in globals().items() if k.startswith("test_")]
    failed = 0
    for fn in fns:
        try:
            fn()
            print(f"  [PASS] {fn.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"  [FAIL] {fn.__name__}: {e}")
    return failed


if __name__ == "__main__":
    sys.exit(1 if run_all() else 0)
