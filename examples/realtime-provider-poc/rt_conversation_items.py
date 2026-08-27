#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Conversation-item mirror + zero-LLM compaction for the realtime head.

The DashScope realtime conversation lives server-side; the client only sees
``conversation.item.*`` events as they happen. This module keeps a
client-side mirror of that item list (:class:`ConversationLog`) and plans a
deterministic compaction over it (:class:`ConversationCompactor`, kg/14
§2.5): past a size threshold, delete every non-pinned server item and
re-seed the conversation with a single mechanical ``state.snapshot`` user
item assembled from the session store plus the backend's running registry —
no summarizer LLM, no session rollover, doctrine untouched.

Wiring (gateway side): ``head.mirror_sink`` feeds the log (the Qwen
realtime service calls it on every conversation.item.added/.done with a
flat dict), and ``head.on_turn_idle`` runs the threshold check; deletes and
the snapshot injection are sent from the gateway under the final-injection
lock.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class MirroredItem:
    """One conversation item as mirrored from server events.

    Audio-bearing items are stored as text where the server provides a
    transcript; raw audio is never mirrored (the mirror exists to feed a
    text summarizer, and audio tokens are what compaction exists to shed).
    """

    item_id: str
    type: str  # message | function_call | function_call_output
    role: str | None = None
    text: str = ""
    name: str | None = None  # function_call: tool name
    call_id: str | None = None
    # Items kept verbatim through compaction regardless of policy
    # (e.g. pending function_call_output pairs still in flight).
    pinned: bool = False


def _field(item, name: str, default=None):
    """Read one field off a server item, attribute- or dict-shaped."""
    if isinstance(item, dict):
        return item.get(name, default)
    return getattr(item, name, default)


def _item_text(item) -> str:
    """Extract displayable text from a server item object (any shape)."""
    if isinstance(item, dict) and item.get("text"):
        return str(item["text"])  # pre-flattened (mirror_sink dict form)
    kind = _field(item, "type", "") or ""
    if kind == "function_call":
        return str(_field(item, "arguments", "") or "")
    if kind == "function_call_output":
        return str(_field(item, "output", "") or "")
    parts = []
    for c in _field(item, "content", None) or []:
        t = _field(c, "transcript", None) or _field(c, "text", None) or ""
        if t:
            parts.append(str(t))
    return "\n".join(parts)


class ConversationLog:
    """Client-side mirror of the server conversation item list.

    Fed from the head service's item-event taps (added/done both land here;
    order follows server events, duplicate item_ids update in place).
    """

    def __init__(self):
        self._items: list[MirroredItem] = []
        self._by_id: dict[str, MirroredItem] = {}

    def add(self, item_id: str, item) -> MirroredItem:
        """Record/refresh one item from a server event payload."""
        mi = MirroredItem(
            item_id=item_id,
            type=_field(item, "type", "message") or "message",
            role=_field(item, "role"),
            text=_item_text(item),
            name=_field(item, "name"),
            call_id=_field(item, "call_id"),
        )
        if item_id in self._by_id:
            self._by_id[item_id].__dict__.update(mi.__dict__)
        else:
            self._items.append(mi)
            self._by_id[item_id] = mi
        return mi

    def drop(self, item_id: str) -> MirroredItem | None:
        """Remove one mirrored item (compaction delete side-effect)."""
        mi = self._by_id.pop(item_id, None)
        if mi is not None:
            self._items.remove(mi)
        return mi

    def items(self) -> list[MirroredItem]:
        return list(self._items)

    def text_chars(self) -> int:
        """Rough size proxy for trigger thresholds."""
        return sum(len(i.text) for i in self._items)

    def to_summary_prompt(self) -> str:
        """Render the mirrored items into a summarizer-LLM prompt body.

        Function-call pairs render adjacently (call + output on two lines)
        so the summarizer sees tool round-trips whole, not as orphan halves.
        """
        lines = []
        for i in self._items:
            who = i.role or i.type
            if i.type == "function_call":
                lines.append(f"[tool_call {i.name}({i.call_id})] {i.text}")
            elif i.type == "function_call_output":
                lines.append(f"[tool_output {i.call_id}] {i.text}")
            else:
                lines.append(f"[{who}] {i.text}")
        return "\n".join(lines)

    def open_tool_pairs(self) -> list[tuple[MirroredItem, ...]]:
        """function_call items with no matching output yet.

        These MUST be carried verbatim (not summarized) across a compaction:
        the server pairs outputs by call_id, and a summarized call without
        its output breaks the pending function-call contract.
        """
        outs = {i.call_id for i in self._items
                if i.type == "function_call_output"}
        return [(c,) for c in self._items
                if c.type == "function_call" and c.call_id not in outs]


class ConversationCompactor:
    """Deterministic compaction planner over a :class:`ConversationLog`.

    Plan (kg/14 §2.5): items in open tool-call pairs stay verbatim on the
    server; everything else is deleted and replaced by one ``state.snapshot``
    user item mechanically joined from the store index and the backend run
    registry — zero LLM, zero session rollover.
    """

    def __init__(self, log: ConversationLog, *, trigger_chars: int = 20_000):
        self.log = log
        self.trigger_chars = trigger_chars

    def should_compact(self) -> bool:
        """Threshold check; ``trigger_chars`` of 0 disables compaction."""
        return self.trigger_chars > 0 and self.log.text_chars() >= self.trigger_chars

    def compact_plan(self) -> dict:
        """Deletion plan: every mirrored item except the pinned open pairs."""
        pinned_ids = {i.item_id for pair in self.log.open_tool_pairs() for i in pair}
        return {
            "delete_ids": [i.item_id for i in self.log.items()
                           if i.item_id not in pinned_ids],
            "pinned": len(pinned_ids),
            "before_chars": self.log.text_chars(),
        }

    def state_snapshot(self, store_rows: list[dict], running: list[dict],
                       *, now: float) -> dict:
        """Assemble the kg/14 §2.5 ``state.snapshot`` payload.

        Args:
            store_rows: ``store.list()`` index rows; the light fields
                (``no/ref/status/summary/chars``) survive, body never enters
                the head context.
            running: backend run-registry entries as ``{"ref", "ts"}`` dicts;
                refs already in the store join as their stored (terminal)
                status, the rest appear as ``running`` with ``elapsed_s``.
            now: reference epoch seconds for ``elapsed_s``/``ts``.

        Returns:
            The snapshot dict (``t``/``ts``/``tasks``/``counts``).
        """
        tasks = [
            {"no": r.get("no"), "ref": r.get("ref"), "status": r.get("status"),
             "summary": r.get("summary"), "chars": r.get("chars")}
            for r in store_rows
        ]
        known = {t["ref"] for t in tasks}
        for r in running:
            ref = r.get("ref")
            if not ref or ref in known:
                continue
            started = float(r.get("ts") or 0)
            tasks.append({"ref": ref, "status": "running",
                          "elapsed_s": max(0, int(now - started)) if started else 0})
        counts: dict[str, int] = {}
        for t in tasks:
            counts[t["status"]] = counts.get(t["status"], 0) + 1
        return {"t": "state.snapshot", "ts": now, "tasks": tasks, "counts": counts}
