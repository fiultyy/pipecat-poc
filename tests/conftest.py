#
# Copyright (c) 2024-2026, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Shared pytest configuration for the Pipecat test suite."""

import dotenv
import pytest
from contextlib import ExitStack

from pipecat.utils.deprecation import _warned_read_sites

from live_lock import DEFAULT_DOMAIN, LiveLockUnavailable, live_lease


def pytest_configure(config):
    """Keep a developer's ``.env`` out of the test session.

    Modules that call ``load_dotenv()`` at import scope would otherwise pull the
    repository's ``.env`` into ``os.environ`` while pytest collects the suite,
    before any test runs. Tests would then see whichever variables that
    developer happens to have set, and behave differently than they do in CI.

    Collection imports test modules after this hook, so a module binding the
    name with ``from dotenv import load_dotenv`` picks up the stub.
    """
    dotenv.load_dotenv = lambda *args, **kwargs: False
    config.addinivalue_line(
        "markers",
        "live([domains]): 占用真实宿主资源（dais 总线/orca 宿主等）的用例；"
        "conftest 自动按声明域取跨进程 flock 租约（tests/live_lock.py，"
        "DSH_LIVE_LOCK=skip 时拿不到即跳过让路）",
    )


_LEASES: pytest.StashKey[ExitStack] = pytest.StashKey()


def pytest_runtest_setup(item):
    """Acquire the declared live leases before a marked case runs.

    ``@pytest.mark.live`` / ``@pytest.mark.live("dais-bus", "orca-host")`` /
    ``@pytest.mark.live(domains=[...])`` — domains default to the shared dais
    bus. Leases are acquired in sorted order (deadlock-free) and released in
    ``pytest_runtest_teardown``. With ``DSH_LIVE_LOCK=skip`` a held lease
    skips the case with a trace instead of waiting.
    """
    marker = item.get_closest_marker("live")
    if marker is None:
        return
    domains = sorted(set(marker.args) or set(marker.kwargs.get("domains", ())) or {DEFAULT_DOMAIN})
    stack = ExitStack()
    try:
        for domain in domains:
            stack.enter_context(live_lease(domain))
    except LiveLockUnavailable as exc:
        stack.close()
        pytest.skip(f"live lease unavailable (DSH_LIVE_LOCK=skip): {exc}")
    item.stash[_LEASES] = stack


def pytest_runtest_teardown(item):
    """Release the live leases held for this case (setup-to-teardown span)."""
    stack = item.stash.get(_LEASES, None)
    if stack is not None:
        item.stash[_LEASES] = None
        stack.close()


@pytest.fixture(autouse=True)
def reset_deprecated_read_warnings():
    """Let every test see a deprecated field read for the first time.

    ``warn_deprecated_read()`` warns once per call site for the life of the
    process. Tests that assert on those warnings reach them through shared
    source lines — a fixture processor's ``setup()``, say — so each test starts
    from an empty record.
    """
    _warned_read_sites.clear()
