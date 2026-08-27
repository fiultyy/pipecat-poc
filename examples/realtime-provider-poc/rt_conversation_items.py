#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Conversation-item mirror + compaction entry for the realtime head.

The DashScope realtime conversation lives server-side; the client only sees
``conversation.item.*`` events as they happen. This module keeps a client-side
mirror of that item list and renders it into a form a text LLM can summarize
— the two mechanical halves of a compaction pipeline.

Compaction policy (when to trigger, what to keep verbatim vs summarize, how
the rollover session is seeded) is deliberately NOT designed here: the entry
points are stubbed pending the design grill that follows the DSH compact
research.

Wiring (post-grill): the gateway head builder subscribes
``ConversationLog`` to the head service's ``on_conversation_item_created`` /
``on_conversation_item_updated`` event handlers; nothing else in the pipeline
changes.
"""

from __future__ import annotations

from dataclasses import dataclass, field


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


def _item_text(item) -> str:
    """Extract displayable text from a server item object (any shape)."""
    if getattr(item, "type", "") == "function_call":
        return getattr(item, "arguments", "") or ""
    if getattr(item, "type", "") == "function_call_output":
        return getattr(item, "output", "") or ""
    content = getattr(item, "content", None) or []
    parts = []
    for c in content:
        t = getattr(c, "transcript", None) or getattr(c, "text", None) or ""
        if t:
            parts.append(t)
    return "\n".join(parts)


class ConversationLog:
    """Client-side mirror of the server conversation item list.

    Fed from the head service's ``on_conversation_item_created`` /
    ``on_conversation_item_updated`` event handlers; order follows server
    events, duplicate item_ids update in place.
    """

    def __init__(self):
        self._items: list[MirroredItem] = []
        self._by_id: dict[str, MirroredItem] = {}

    def add(self, item_id: str, item) -> MirroredItem:
        """Record/refresh one item from a server event payload."""
        mi = MirroredItem(
            item_id=item_id,
            type=getattr(item, "type", "message"),
            role=getattr(item, "role", None),
            text=_item_text(item),
            name=getattr(item, "name", None),
            call_id=getattr(item, "call_id", None),
        )
        if item_id in self._by_id:
            self._by_id[item_id].__dict__.update(mi.__dict__)
        else:
            self._items.append(mi)
            self._by_id[item_id] = mi
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

        These MUST be carried verbatim (not summarized) across a rollover:
        the server pairs outputs by call_id, and a summarized call without
        its output breaks the pending function-call contract.
        """
        outs = {i.call_id for i in self._items
                if i.type == "function_call_output"}
        return [(c,) for c in self._items
                if c.type == "function_call" and c.call_id not in outs]


class ConversationCompactor:
    """Compaction policy entry — STUB, pending design grill.

    Open questions for the grill (informed by the DSH compact research):
    - trigger: item count vs text_chars vs server-reported context tokens?
    - summarizer: which model/endpoint, single-shot vs chunked?
    - rollover: seed new session with summary user item + verbatim pins?
      what does the user hear at the seam (if anything)?
    - DSH-style carve-outs worth porting: pinned items, recent-N verbatim,
      structured state (runs/refs) re-injected as facts?
    """

    def __init__(self, log: ConversationLog, *, trigger_chars: int = 20_000):
        self.log = log
        self.trigger_chars = trigger_chars

    def should_compact(self) -> bool:
        return self.log.text_chars() >= self.trigger_chars
