#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Unit tests for the head profile registry (PR8: multi-head config,
singleton activation)."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "realtime-provider-poc"))

from rt_head_registry import (  # noqa: E402
    DEFAULT_PROFILES_PATH,
    ENV_ACTIVE_PIN,
    ENV_PROFILES_PATH,
    HeadProfile,
    load_head_registry,
)


def write_heads(tmp_path, doc: dict, name: str = "heads.json") -> Path:
    p = tmp_path / name
    p.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
    return p


def two_profile_doc() -> dict:
    return {
        "active": "alpha",
        "profiles": [
            {"name": "alpha", "label": "甲", "model": "m-a", "voice": "v-a",
             "turn_silence_ms": 800, "turn_threshold": 0.6},
            {"name": "beta", "label": "乙"},
        ],
    }


def test_load_happy_path(tmp_path):
    p = write_heads(tmp_path, two_profile_doc())
    reg = load_head_registry(env={ENV_PROFILES_PATH: str(p)})
    assert reg.file_backed and reg.path == p and not reg.env_pinned
    assert [pr.name for pr in reg.profiles] == ["alpha", "beta"]
    assert reg.active == "alpha"
    alpha = reg.active_profile()
    assert alpha.model == "m-a" and alpha.voice == "v-a"
    assert alpha.turn_silence_ms == 800 and abs(alpha.turn_threshold - 0.6) < 1e-9
    assert alpha.turn_prefix_ms is None
    beta = reg.profiles[1]
    assert beta.model is None and beta.label == "乙"
    assert reg.describe() == [
        {"name": "alpha", "label": "甲", "active": True,
         "model": "m-a", "voice": "v-a"},
        {"name": "beta", "label": "乙", "active": False,
         "model": None, "voice": None},
    ]


def test_active_pin_via_env(tmp_path):
    p = write_heads(tmp_path, two_profile_doc())
    reg = load_head_registry(env={ENV_PROFILES_PATH: str(p),
                                  ENV_ACTIVE_PIN: "beta"})
    assert reg.active == "beta" and reg.env_pinned
    # 钉在不存在的名字：告警 + 回落文件 active
    reg2 = load_head_registry(env={ENV_PROFILES_PATH: str(p),
                                   ENV_ACTIVE_PIN: "gamma"})
    assert reg2.active == "alpha" and not reg2.env_pinned
    assert any("gamma" in w for w in reg2.warnings)


def test_missing_file_degrades_to_single_head(tmp_path):
    reg = load_head_registry(env={ENV_PROFILES_PATH: str(tmp_path / "nope.json")})
    assert not reg.file_backed and reg.path is None
    assert [p.name for p in reg.profiles] == ["default"]
    d = reg.active_profile()
    assert all(getattr(d, f) is None for f in
               ("model", "voice", "doctrine", "doctrine_file",
                "turn_silence_ms", "turn_prefix_ms", "turn_threshold"))
    assert reg.warnings and "unreadable" in reg.warnings[0]


@pytest.mark.parametrize("doc,frag", [
    ({}, "no profiles"),
    ({"profiles": []}, "no profiles"),
    ({"profiles": [{"label": "x"}]}, "name"),
    ({"profiles": [{"name": "a"}, {"name": "a"}]}, "duplicate"),
])
def test_invalid_docs_degrade_whole_file(tmp_path, doc, frag):
    p = write_heads(tmp_path, doc)
    reg = load_head_registry(env={ENV_PROFILES_PATH: str(p)})
    assert not reg.file_backed
    assert [pr.name for pr in reg.profiles] == ["default"]
    assert any(frag in w for w in reg.warnings)


def test_unknown_active_falls_back_to_first(tmp_path):
    doc = two_profile_doc()
    doc["active"] = "ghost"
    p = write_heads(tmp_path, doc)
    reg = load_head_registry(env={ENV_PROFILES_PATH: str(p)})
    assert reg.active == "alpha"
    assert any("ghost" in w for w in reg.warnings)


def test_bad_knob_values_are_dropped_with_warning(tmp_path):
    doc = {"active": "a", "profiles": [
        {"name": "a", "turn_silence_ms": "fast", "turn_threshold": "0.5"}]}
    p = write_heads(tmp_path, doc)
    reg = load_head_registry(env={ENV_PROFILES_PATH: str(p)})
    a = reg.active_profile()
    assert a.turn_silence_ms is None and abs(a.turn_threshold - 0.5) < 1e-9
    assert any("turn_silence_ms" in w for w in reg.warnings)


def test_default_path_constant():
    # 缺省位形 = 用户配置目录（服务部署面），不随仓库走
    assert DEFAULT_PROFILES_PATH.name == "heads.json"
    assert ".config" in str(DEFAULT_PROFILES_PATH)


def test_head_profile_is_plain_data():
    p = HeadProfile(name="x")
    assert p.label == "" and p.doctrine is None
