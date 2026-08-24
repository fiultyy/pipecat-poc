"""flock-based leases that serialize live tests across concurrent processes.

Two pytest runs sharing the dais bus (or the same mailbox) trample each
other, so live cases declare their resource domains via ``@pytest.mark.live``
and ``tests/conftest.py`` acquires one lease per domain around each case.
The default mode blocks until the lease is free and warns loudly instead of
waiting in silence; ``DSH_LIVE_LOCK=skip`` downgrades to a non-blocking
attempt that raises :class:`LiveLockUnavailable` so the caller can skip.

Budget doctrine anchor (docs/plans/impl-specs.md, gate G6): live budget
constants follow ``预算 ≥ 实测 P95 × 2``; the wait here never exceeds that
budget silently — ``warn_after_s`` mirrors the same measured-P95 basis as
the per-case timeouts recorded alongside it.
"""

from __future__ import annotations

import errno
import fcntl
import os
import re
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

DEFAULT_DOMAIN = "dais-bus"
LOCK_ENV = "DSH_LIVE_LOCK"
BLOCK = "block"
SKIP = "skip"


class LiveLockUnavailable(RuntimeError):
    """Raised when skip-mode acquisition finds the lease held elsewhere."""


def lease_path(domain: str) -> Path:
    """Return the lock file path for one resource domain.

    Domains are arbitrary strings (``dais-bus``, ``orca-host``, a mailbox
    name); they are sanitized into one lock file per domain under a
    per-uid directory in the system temp dir.
    """
    safe = re.sub(r"[^a-z0-9._-]+", "-", domain.lower()).strip("-") or "domain"
    lock_dir = Path(tempfile.gettempdir()) / f"dsh-live-locks-{os.getuid()}"
    lock_dir.mkdir(parents=True, exist_ok=True)
    return lock_dir / f"{safe}.lock"


@contextmanager
def live_lease(
    domain: str,
    *,
    mode: str | None = None,
    warn_after_s: float = 60.0,
    poll_s: float = 0.5,
) -> Iterator[Path]:
    """Hold an exclusive cross-process lease on one resource domain.

    Args:
        domain: resource domain identifier; one lock file per domain.
        mode: ``block`` (default) waits until free, ``skip`` raises
            :class:`LiveLockUnavailable` instead of waiting. ``None`` reads
            ``DSH_LIVE_LOCK`` from the environment (default ``block``).
        warn_after_s: once the wait exceeds this, emit one stderr warning
            naming the domain and keep waiting — never silent.
        poll_s: retry interval while blocking.

    Yields:
        the lease file path (holder pid is written inside).
    """
    resolved = (mode or os.environ.get(LOCK_ENV, BLOCK)).strip().lower()
    if resolved not in (BLOCK, SKIP):
        raise ValueError(f"unknown live-lease mode {resolved!r} (want block|skip)")
    path = lease_path(domain)
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
    warned = False
    warn_at = time.monotonic() + warn_after_s
    try:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError as exc:
                if exc.errno not in (errno.EACCES, errno.EAGAIN):
                    raise
                if resolved == SKIP:
                    raise LiveLockUnavailable(
                        f"live lease '{domain}' held elsewhere ({path})"
                    ) from exc
                if not warned and time.monotonic() > warn_at:
                    warned = True
                    print(
                        f"WARNING: live_lease '{domain}' still waiting after "
                        f"{warn_after_s:.0f}s (concurrent live run holds it); "
                        "continuing to wait",
                        file=sys.stderr,
                    )
                time.sleep(poll_s)
        os.truncate(fd, 0)
        os.write(fd, f"{os.getpid()} {time.time():.0f}\n".encode())
        try:
            yield path
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)
