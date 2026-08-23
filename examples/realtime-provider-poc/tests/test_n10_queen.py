#!/usr/bin/env python3
#
# SPDX-License-Identifier: BSD 2-Clause License
#
"""N10-T2 · queen 派生协议测试（OF-013 python 投影器侧）.

零网络零 GLM：只测纯函数、模板文本与参数构造。不实例化 Projector
（那需要 GLM 凭据）、不发 incubate RPC（RPC 消费面由插件票 N10-T3 负责，
本票只验参数构造）。三门用 pipecat-poc examples 的真实实现导入——
路径缺失则显式失败，不跳过。
"""

from __future__ import annotations

import ast
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
POC_DIR = REPO  # pipecat-poc 布局：三门与投影器同目录（tests/ 与其同级）

# 过三门用的最小合规骨架（§4 全章节 + 灾难底线行；生产路径中这些来自
# GLM 投影基底，此处仅用于验证 queen doctrine 追加后仍全绿）。
SKELETON = """# AGENTS.md

## Agent Behavior
测试骨架。

### Mission
- 骨架

### How you work
- 骨架

### MUST
- 骨架

### MUST NOT
- 不得删除生产数据、不得泄露敏感凭据（灾难底线）。

### Style
- 骨架

### Output
- 骨架
"""


WIZARD_PY = Path("~/.agents/skills/incubation-wizard/wizard.py").expanduser()


def _load_wizard():
    if not WIZARD_PY.exists():
        raise AssertionError(f"向导安装点缺失：{WIZARD_PY}（回流前先拷 wizard.py+SKILL.md）")
    spec = importlib.util.spec_from_file_location(
        "incubation_wizard_under_test", WIZARD_PY
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Base(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        gates_py = POC_DIR / "rt_projection_gates.py"
        if not gates_py.exists():
            raise AssertionError(
                f"三门真实实现缺失：{gates_py} 不在位（只读参考被移动/删除？"
                "本测试显式失败，不跳过）"
            )
        for p in (str(POC_DIR),):
            if p not in sys.path:
                sys.path.insert(0, p)
        import rt_projector  # noqa: F401  （导入本身零网络零凭据副作用）
        import rt_projection_gates  # noqa: F401

        cls.rp = sys.modules["rt_projector"]
        cls.gates = sys.modules["rt_projection_gates"]


class QueenTemplateTests(_Base):
    def test_template_nonempty_and_gate1_clean(self):
        tpl = self.rp.ROLE_TEMPLATES.get("queen", "")
        self.assertTrue(tpl.strip(), "queen doctrine 模板缺失或为空")
        self.assertEqual(
            self.gates.gate1_terminology(tpl), [],
            "queen 模板自身触犯术语零暴露（gate1）",
        )

    def test_template_passes_full_gates_on_skeleton(self):
        rep = self.gates.run_gates(SKELETON)
        self.assertTrue(rep.passed, f"对照骨架自身未过三门: {rep.violations}")
        rep2 = self.gates.run_gates(SKELETON.rstrip() + "\n\n" + self.rp.ROLE_TEMPLATES["queen"])
        self.assertTrue(rep2.passed, f"骨架+queen 未过三门: {rep2.violations}")

    def test_template_carries_ironclauses(self):
        tpl = self.rp.ROLE_TEMPLATES["queen"]
        for phrase in ("人在环", "derived-by", "parent", "不直接孵化"):
            self.assertIn(phrase, tpl, f"queen 铁条款缺关键词 {phrase!r}")

    def test_project_accepts_queen_role(self):
        # role 白名单含 queen（不实跑投影，只验入口校验逻辑的接受面）
        self.assertIn("queen", self.rp.ROLE_TEMPLATES)


class GrillDimensionsTests(_Base):
    def test_exactly_18_with_required_fields(self):
        dims = self.rp.GRILL_DIMENSIONS
        self.assertEqual(len(dims), 18, f"grill 清单须恰 18 维，实际 {len(dims)}")
        for d in dims:
            self.assertTrue(d.get("key"), f"缺 key: {d!r}")
            self.assertTrue(d.get("question", "").strip(), f"缺 question: {d!r}")
            self.assertTrue(d.get("default_hint", "").strip(), f"缺 default_hint: {d!r}")
        keys = [d["key"] for d in dims]
        self.assertEqual(len(keys), len(set(keys)), "grill 维度 key 重复")

    def test_contains_agent_role_excludes_template_version(self):
        keys = {d["key"] for d in self.rp.GRILL_DIMENSIONS}
        self.assertIn("agent_role", keys, "grill 清单须含 agent_role")
        self.assertNotIn("template_version", keys, "机械维 template_version 不得入清单")

    def test_checklist_suggested_nonempty(self):
        cl = self.rp.grill_checklist("写代码的agent")
        self.assertEqual(len(cl), 18)
        for item in cl:
            self.assertEqual(set(item), {"key", "question", "suggested"})
            self.assertTrue(str(item["suggested"]).strip(),
                            f"[{item['key']}] 建议值为空")
        self.assertEqual([i["key"] for i in cl],
                         [d["key"] for d in self.rp.GRILL_DIMENSIONS])

    def test_checklist_priors_and_role_keywords(self):
        cl = self.rp.grill_checklist("管理一个发布上线的运维团队")
        by_key = {i["key"]: i["suggested"] for i in cl}
        self.assertIn("manager", by_key["agent_role"], "管理关键词应建议 manager")
        self.assertIn("release", by_key["D1"], "上线关键词应命中 release 预设先验")
        cl2 = self.rp.grill_checklist("一个没有任何信号的纯空白描述")
        self.assertTrue(all(i["suggested"].strip() for i in cl2), "兜底先验也应给出建议值")


class MustacheSanitizeTests(_Base):
    def test_sanitize_rewrites_double_braces(self):
        self.assertEqual(self.rp.sanitize_mustache("{{x}}"), "{ {x}}")
        self.assertEqual(self.rp.sanitize_mustache("a{{b}}c{{d}}"), "a{ {b}}c{ {d}}")

    def test_sanitize_noop_on_plain_text(self):
        for text in ("普通文本", "单花括号 { ok }", "", "x = '{ {'"):
            self.assertEqual(self.rp.sanitize_mustache(text), text)

    def test_assert_no_mustache_raises(self):
        with self.assertRaises(ValueError):
            self.rp.assert_no_mustache("bad {{x}} text")

    def test_assert_no_mustache_accepts_clean(self):
        self.rp.assert_no_mustache("clean text")
        self.rp.assert_no_mustache("已消毒 { {x}} 段")


class WizardArgsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.w = _load_wizard()

    def test_roles_contain_queen(self):
        self.assertIn("queen", self.w.ROLES)

    def test_role_queen_parseable(self):
        args = self.w.parse_args(
            ["--scenario", "s", "--name", "n", "--role", "queen"]
        )
        self.assertEqual(args.role, "queen")
        self.assertFalse(args.derive)
        self.assertEqual(args.answers, {})

    def test_derive_without_answers_file_errors(self):
        err = io.StringIO()
        with redirect_stderr(err):
            with self.assertRaises(SystemExit) as ctx:
                self.w.parse_args(["--scenario", "s", "--name", "n", "--derive"])
        self.assertEqual(ctx.exception.code, 2)
        self.assertIn("--answers-file", err.getvalue())

    def test_answers_file_json_loaded(self):
        payload = {"agent_role": "manager", "D1": "高", "M3": "长期"}
        with tempfile.NamedTemporaryFile(
            "w", suffix=".json", delete=False, encoding="utf-8"
        ) as fh:
            json.dump(payload, fh, ensure_ascii=False)
            path = fh.name
        try:
            args = self.w.parse_args([
                "--scenario", "s", "--name", "n",
                "--derive", "--answers-file", path, "--parent", "p1",
            ])
        finally:
            Path(path).unlink(missing_ok=True)
        self.assertTrue(args.derive)
        self.assertEqual(args.answers, payload)
        self.assertEqual(args.parent, "p1")

    def test_classic_arg_surface_unchanged(self):
        args = self.w.parse_args([
            "--scenario", "调研 WebGPU", "--name", "research-webgpu",
            "--role", "worker", "--targets", "dry", "--model", "glm-5-turbo",
        ])
        self.assertEqual(
            (args.scenario, args.name, args.role, args.targets, args.model,
             args.derive, args.parent, args.answers),
            ("调研 WebGPU", "research-webgpu", "worker", ["dry"], "glm-5-turbo",
             False, "", {}),
        )


class SyntaxTests(unittest.TestCase):
    def test_py_files_parse(self):
        # 回流变体：rt_projector 在 REPO，wizard 在技能安装点
        for label, path in (("rt_projector.py", REPO / "rt_projector.py"),
                            ("wizard.py", WIZARD_PY)):
            ast.parse(path.read_text(encoding="utf-8"), filename=label)  # SyntaxError 即测试失败


if __name__ == "__main__":
    unittest.main()
