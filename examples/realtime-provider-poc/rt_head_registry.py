#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Multi-head profile registry with singleton activation (PR8).

Heads are configured as profiles in a JSON file — ``VOICE_HEAD_PROFILES``
or ``~/.config/voice-gateway/heads.json``. The gateway activates exactly
ONE profile at a time: a voice session is built from whichever profile is
active at build time, and switching (``head.switch`` control frame) only
repoints the selection for the NEXT session. The live head is never torn
down mid-call.

File shape::

    {
      "active": "nova",
      "profiles": [
        {"name": "nova", "label": "Nova·任务助手",
         "model": null, "voice": null,
         "doctrine": null, "doctrine_file": null,
         "turn_silence_ms": null, "turn_prefix_ms": null,
         "turn_threshold": null},
        {"name": "echo", "label": "Echo·副本占位"}
      ]
    }

Per-field precedence at head build (rt_gateway): profile value → legacy
env/built-in (``VOICE_HEAD_MODEL``/``VOICE_HEAD_VOICE``/``VOICE_HEAD_*``
VAD knobs/``DoctrineSource``). A ``null`` field therefore means "inherit
the legacy single-head behavior" — placeholder copies ship as all-null
profiles and are byte-equivalent to the default head.

Degradation: a missing, unreadable, or invalid file yields ONE
synthesized ``default`` profile with every field None — behavior
identical to the pre-registry gateway. Validation problems (duplicate
names, empty table, unknown active) also degrade the whole file rather
than partially trusting it; every problem is reported once via
``warnings`` so callers can surface it.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

ENV_PROFILES_PATH = "VOICE_HEAD_PROFILES"
ENV_ACTIVE_PIN = "VOICE_HEAD_PROFILE"
DEFAULT_PROFILES_PATH = Path.home() / ".config" / "voice-gateway" / "heads.json"

_KNOB_FIELDS = ("turn_silence_ms", "turn_prefix_ms", "turn_threshold")
_KNOB_CASTS = {"turn_silence_ms": int, "turn_prefix_ms": int,
               "turn_threshold": float}


@dataclass(frozen=True)
class HeadProfile:
    """One head agent configuration; None fields inherit legacy env/built-in."""

    name: str
    label: str = ""
    model: str | None = None
    voice: str | None = None
    doctrine: str | None = None          # inline text, wins over doctrine_file
    doctrine_file: str | None = None
    turn_silence_ms: int | None = None
    turn_prefix_ms: int | None = None
    turn_threshold: float | None = None


@dataclass
class HeadRegistry:
    """Loaded profile table + the singleton active selection.

    ``raw_profiles`` keeps the parsed file dicts so ``head.switch`` can
    persist the new active selection back without rewriting profile
    content (atomic write by the gateway).
    """

    profiles: list[HeadProfile]
    active: str
    path: Path | None = None              # None = synthesized single-head mode
    env_pinned: bool = False              # VOICE_HEAD_PROFILE wins at load
    warnings: list[str] = field(default_factory=list)

    @property
    def file_backed(self) -> bool:
        return self.path is not None

    def active_profile(self) -> HeadProfile:
        return next((p for p in self.profiles if p.name == self.active),
                    self.profiles[0])

    def describe(self) -> list[dict]:
        """Compact listing for the ``head.list`` control frame."""
        return [{"name": p.name, "label": p.label, "active": p.name == self.active,
                 "model": p.model, "voice": p.voice} for p in self.profiles]


def _default_registry(warnings: list[str]) -> HeadRegistry:
    return HeadRegistry(profiles=[HeadProfile(name="default")], active="default",
                        path=None, warnings=warnings)


def load_head_registry(env=None) -> HeadRegistry:
    """Load the profile table; never raises — problems degrade to default."""
    env = env if env is not None else os.environ
    warn = lambda msg: print(f"rt_head_registry: {msg}", file=sys.stderr)  # noqa: E731
    warnings: list[str] = []

    raw_path = (env.get(ENV_PROFILES_PATH) or "").strip()
    path = Path(raw_path) if raw_path else DEFAULT_PROFILES_PATH
    try:
        with open(path, encoding="utf-8") as fh:
            doc = json.load(fh)
    except OSError as e:
        warnings.append(f"{path} unreadable ({e}); single-head mode")
        reg = _default_registry(warnings)
        for w in warnings:
            warn(w)
        return reg
    except ValueError as e:
        warnings.append(f"{path} invalid JSON ({e}); single-head mode")
        reg = _default_registry(warnings)
        for w in warnings:
            warn(w)
        return reg

    raw_profiles = doc.get("profiles") if isinstance(doc, dict) else None
    if not isinstance(raw_profiles, list) or not raw_profiles:
        warnings.append(f"{path} has no profiles table; single-head mode")
        reg = _default_registry(warnings)
        for w in warnings:
            warn(w)
        return reg

    profiles: list[HeadProfile] = []
    names: set[str] = set()
    for i, item in enumerate(raw_profiles):
        if not isinstance(item, dict) or not isinstance(item.get("name"), str) \
                or not item["name"].strip():
            warnings.append(f"profiles[{i}] lacks a non-empty name; single-head mode")
            reg = _default_registry(warnings)
            for w in warnings:
                warn(w)
            return reg
        name = item["name"].strip()
        if name in names:
            warnings.append(f"duplicate profile name {name!r}; single-head mode")
            reg = _default_registry(warnings)
            for w in warnings:
                warn(w)
            return reg
        names.add(name)
        kwargs: dict = {"label": str(item.get("label") or "")}
        for key in ("model", "voice", "doctrine", "doctrine_file"):
            val = item.get(key)
            kwargs[key] = val.strip() if isinstance(val, str) and val.strip() else None
        for key in _KNOB_FIELDS:
            val = item.get(key)
            if val is None:
                kwargs[key] = None
                continue
            try:
                kwargs[key] = _KNOB_CASTS[key](val)
            except (TypeError, ValueError):
                warnings.append(f"profile {name!r} {key}={val!r} unparseable; ignored")
                kwargs[key] = None
        profiles.append(HeadProfile(name=name, **kwargs))

    pin = (env.get(ENV_ACTIVE_PIN) or "").strip()
    if pin:
        if pin not in names:
            warnings.append(f"{ENV_ACTIVE_PIN}={pin} unknown; using file active")
        env_pinned = pin in names
        active = pin if pin in names else str(doc.get("active") or "")
    else:
        env_pinned = False
        active = str(doc.get("active") or "")
    if active not in names:
        warnings.append(f"active {active!r} unknown; using first profile")
        active = profiles[0].name

    reg = HeadRegistry(profiles=profiles, active=active, path=path,
                       env_pinned=env_pinned, warnings=warnings)
    for w in warnings:
        warn(w)
    return reg
