#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Environment wiring for the voice-orchestration PoC entries.

Credential chain (single source of truth, see docs/kg/05-contracts.md §4):

1. ``.env`` next to the entry script (repo-local overrides win);
2. ``~/.dsh/zhipu.env`` — GLM coding-plan key (never override existing).

DashScope (DASHSCOPE_API_KEY) is intentionally NOT sourced anywhere: until
the key is provided (open question Q2) live voice runs fall back to the
GLM text-mode smoke path.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

ZHIPU_ENV_PATH = Path("~/.dsh/zhipu.env").expanduser()

KEYS_OF_INTEREST = (
    "ZHIPU_CODING_PLAN_API_KEY",
    "GLM_BASE_URL",
    "GLM_FORMATTER_MODEL",
    "DASHSCOPE_API_KEY",
    "VOICE_GATEWAY_TOKEN",
)


def _parse_env_file(path: Path) -> dict[str, str]:
    """Minimal KEY=VALUE parser (the tests stub dotenv out; see tests/conftest.py)."""
    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        out[k.strip()] = v.strip().strip("'\"")
    return out


def load_env() -> dict[str, bool]:
    """Load the credential chain; return presence map for diagnostics."""
    load_dotenv()
    parsed = _parse_env_file(ZHIPU_ENV_PATH) if ZHIPU_ENV_PATH.exists() else {}
    for k, v in parsed.items():
        os.environ.setdefault(k, v)  # same precedence as load_dotenv(override=False)
    return {k: bool(os.environ.get(k)) for k in KEYS_OF_INTEREST}


def glm_credentials() -> tuple[str, str]:
    """Return (api_key, base_url) for the GLM formatter/orchestrator client.

    Loads the credential chain on first use, so any entry point (script,
    pytest) can call this directly.

    Raises:
        RuntimeError: when the coding-plan key is absent from the chain.
    """
    if not os.environ.get("ZHIPU_CODING_PLAN_API_KEY"):
        load_env()
    key = os.environ.get("ZHIPU_CODING_PLAN_API_KEY", "")
    if not key:
        raise RuntimeError(
            "ZHIPU_CODING_PLAN_API_KEY missing: expected in ~/.dsh/zhipu.env (see rt_env)"
        )
    base_url = os.environ.get(
        "GLM_BASE_URL", "https://open.bigmodel.cn/api/coding/paas/v4"
    )
    return key, base_url
