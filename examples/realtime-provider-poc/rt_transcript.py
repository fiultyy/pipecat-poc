#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Client-side realtime transcript state machine.

Port of Codex's RealtimeTranscriptState (codex-rs/codex-api/src/endpoint/
realtime_websocket/methods.rs): the DashScope realtime session keeps its
context server-side and unreadable, so the client maintains its own
transcript — accumulated from deltas, corrected by authoritative
``.done`` texts, truncated to a byte budget — and hands the tail to the
orchestrator for re-seeding after a reconnect (the only way to reshape
context on DashScope) or as handoff context for backend agents.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

TRUNCATED_PREFIX = "[truncated] "

# Byte budget for the maintained transcript tail. Codex uses a similar cap
# so a runaway session can't grow the re-seed payload (and its token cost)
# without bound.
DEFAULT_MAX_TRANSCRIPT_BYTES = 24_000


@dataclass
class TranscriptEntry:
    """One contiguous stretch of speech/text attributed to a role."""

    role: str  # "user" | "assistant"
    text: str


def _entry_bytes(entry: TranscriptEntry) -> int:
    return len(entry.role.encode()) + len(entry.text.encode()) + 3


@dataclass
class TranscriptState:
    """Accumulates realtime transcript events into bounded entries.

    Feed it the parsed realtime events; call ``take_tail()`` when a
    handoff or re-seed needs the current conversation memory.
    """

    max_bytes: int = DEFAULT_MAX_TRANSCRIPT_BYTES
    entries: list[TranscriptEntry] = field(default_factory=list)
    _new_input: bool = False
    _new_output: bool = False
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    def on_speech_started(self) -> None:
        """input_audio_buffer.speech_started: the next input delta opens a new entry."""
        self._new_input = True

    def on_response_created(self) -> None:
        """response.created: the next output delta opens a new entry."""
        self._new_output = True

    def on_input_delta(self, delta: str) -> None:
        self._append_delta("user", delta, force_new=self._new_input)
        self._new_input = False

    def on_output_delta(self, delta: str) -> None:
        self._append_delta("assistant", delta, force_new=self._new_output)
        self._new_output = False

    def on_input_done(self, text: str) -> None:
        # ``.done`` is authoritative: it replaces the accumulated deltas.
        self._apply_done("user", text, force_new=self._new_input)
        self._new_input = False

    def on_output_done(self, text: str) -> None:
        self._apply_done("assistant", text, force_new=self._new_output)
        self._new_output = False

    def on_handoff_input(self, input_text: str) -> None:
        """A backend handoff intent the user never spoke aloud still belongs
        in the transcript as a user-side turn (Codex append_handoff_input)."""
        text = (input_text or "").strip()
        if not text:
            return
        if any(e.role == "user" and e.text == text for e in self.entries):
            return
        self.entries.append(TranscriptEntry("user", text))
        self._truncate()

    def seed(self, role: str, text: str) -> None:
        """Bulk-seed an entry (initial history replay)."""
        if text:
            self.entries.append(TranscriptEntry(role, text))
            self._truncate()

    async def take_tail(self) -> list[TranscriptEntry]:
        """Atomically drain the maintained transcript (handoff/re-seed memory)."""
        async with self._lock:
            out, self.entries = self.entries, []
            return out

    def snapshot(self) -> list[TranscriptEntry]:
        """Non-destructive view."""
        return list(self.entries)

    def as_text(self) -> str:
        return "\n".join(f"{e.role}: {e.text}" for e in self.entries)

    # -- internals --

    def _append_delta(self, role: str, delta: str, force_new: bool) -> None:
        if not delta:
            return
        if not force_new and self.entries and self.entries[-1].role == role:
            self.entries[-1].text += delta
        else:
            self.entries.append(TranscriptEntry(role, delta))
        self._truncate()

    def _apply_done(self, role: str, text: str, force_new: bool) -> None:
        if not text:
            return
        if not force_new and self.entries and self.entries[-1].role == role:
            self.entries[-1].text = text
        else:
            self.entries.append(TranscriptEntry(role, text))
        self._truncate()

    def _truncate(self) -> None:
        total = sum(_entry_bytes(e) for e in self.entries)
        while total > self.max_bytes and len(self.entries) > 1:
            total -= _entry_bytes(self.entries[0])
            self.entries.pop(0)
        if not self.entries:
            return
        entry = self.entries[0]
        budget = self.max_bytes - (len(entry.role.encode()) + 3)
        encoded = entry.text.encode()
        if len(encoded) <= budget:
            return
        keep = max(budget - len(TRUNCATED_PREFIX.encode()), 0)
        # Walk back to a char boundary.
        while keep > 0 and not (len(encoded[:keep].decode(errors="ignore")) == keep):
            keep -= 1
        entry.text = TRUNCATED_PREFIX + encoded[:keep].decode(errors="ignore")
