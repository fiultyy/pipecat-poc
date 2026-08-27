#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""rt_session_store: 编排任务台账（SQLite；docs/kg/14-unified-callback-split.md §2.1）.

单表 ``sessions``、主键 ref、LRU 500、WAL。同步 API——网关侧读写均以
``asyncio.to_thread`` 包裹；同进程单连接（``check_same_thread=False``）+
``threading.Lock`` 串行化全部访问。

- ``SessionStore.put``    — orch.dispatch 受理落库（幂等；no 受理时分配）
- ``SessionStore.update`` — done/failed/cancelled/conv_id 回填等局部更新
- ``SessionStore.get``    — 正文全文取出（body.get → body.item）
- ``SessionStore.list``   — 最近 N 条索引（无 body；topic_cache 回放）

字段对齐设计文档 JSON：ref/no/status/title/summary/body/chars/credentials_json/
run_id/conv_id/ts/updated_ts。credentials 落库为 JSON 字符串、API 出入口转
list；chars 恒由 body 长度派生（不采信调用方给的值）；no 取自 AUTOINCREMENT
序列表——高水位永不回退，删行/清库后不复用旧号，跨进程重启稳定。

写入面的失败隔离在调用方（``main._bridge`` try/except 吞并 + stderr 告警）：
台账故障不得影响派发链路。
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

ENV_DB_PATH = "VOICE_STORE_DB"
DEFAULT_DB_PATH = "~/.local/state/voice-gateway/store.db"
DEFAULT_MAX_ROWS = 500   # LRU 上限（KG 14 §2.1）
DEFAULT_LIST_LIMIT = 50  # 索引回放条数（topic_cache 最近 50 条）

# status 全集（裁决 #10：对齐 orch.done 命名，去 kind 双字段）
STATUSES = ("accepted", "running", "done", "failed", "cancelled", "timeout")

_COLUMNS = (
    "ref", "no", "status", "title", "summary", "body", "chars",
    "credentials_json", "run_id", "conv_id", "ts", "updated_ts",
)
# update() 可改字段；ref/no/updated_ts 由 store 自管
_UPDATABLE = ("status", "title", "summary", "body", "credentials", "run_id", "conv_id")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    ref              TEXT PRIMARY KEY,
    no               INTEGER NOT NULL,
    status           TEXT NOT NULL DEFAULT 'accepted',
    title            TEXT NOT NULL DEFAULT '',
    summary          TEXT NOT NULL DEFAULT '',
    body             TEXT NOT NULL DEFAULT '',
    chars            INTEGER NOT NULL DEFAULT 0,
    credentials_json TEXT NOT NULL DEFAULT '[]',
    run_id           TEXT,
    conv_id          TEXT,
    ts               REAL NOT NULL,
    updated_ts       REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS no_seq (
    id INTEGER PRIMARY KEY AUTOINCREMENT
);
"""


class SessionStore:
    """编排台账唯一持久面（裁决 #8：写入面 = main._bridge，backend 不感知 store）。"""

    def __init__(self, path: str | os.PathLike[str] | None = None) -> None:
        """打开（必要时创建）台账库。

        Args:
            path: 库文件路径；None 时依次取 env ``VOICE_STORE_DB``、缺省
                ``~/.local/state/voice-gateway/store.db``（目录自动建）。
        """
        if path is None:
            path = os.environ.get(ENV_DB_PATH) or DEFAULT_DB_PATH
        self._path = Path(path).expanduser()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    @property
    def path(self) -> Path:
        """库文件实际路径（诊断/测试用）。"""
        return self._path

    def close(self) -> None:
        """关闭连接。"""
        with self._lock:
            self._conn.close()

    # ---- 写入 ----

    def put(self, entry: dict) -> dict:
        """幂等落库一条任务（INSERT OR REPLACE，同 ref 整行覆写）。

        Args:
            entry: 至少含 ``ref``。``status`` 缺省 accepted；``ts`` 缺省当前
                时刻；``no`` 缺省由序列表分配，ref 已存在或显式给定时沿用
                ——重复事件不烧号，no 跨重连稳定。

        Returns:
            落库后的完整条目（含 body、credentials 已转 list）。

        Raises:
            ValueError: 缺 ref，或 status 不在 :data:`STATUSES` 内。
            TypeError: credentials 非 list/tuple/None。
        """
        ref = entry.get("ref")
        if not ref:
            raise ValueError("put: entry['ref'] is required")
        status = entry.get("status") or "accepted"
        if status not in STATUSES:
            raise ValueError(f"put: unknown status {status!r}; valid: {list(STATUSES)}")
        body = entry.get("body") or ""
        ts = float(entry.get("ts") or time.time())
        updated_ts = float(entry.get("updated_ts") or ts)
        creds_json = self._creds_to_json(entry.get("credentials"))
        with self._lock:
            try:
                row = self._conn.execute(
                    "SELECT no FROM sessions WHERE ref = ?", (ref,)
                ).fetchone()
                if row is not None:
                    no = row["no"]
                elif entry.get("no") is not None:
                    no = int(entry["no"])
                else:
                    no = self._next_no()
                self._conn.execute(
                    "INSERT OR REPLACE INTO sessions"
                    " (ref, no, status, title, summary, body, chars,"
                    " credentials_json, run_id, conv_id, ts, updated_ts)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        ref, no, status,
                        entry.get("title") or "", entry.get("summary") or "",
                        body, len(body), creds_json,
                        entry.get("run_id"), entry.get("conv_id"), ts, updated_ts,
                    ),
                )
                self._enforce_lru()
                self._conn.commit()
                return self._row_to_dict(self._fetch(ref))
            except Exception:
                self._conn.rollback()
                raise

    def update(self, ref: str, **fields: Any) -> dict | None:
        """按 ref 局部更新；updated_ts 恒刷新为当前时刻。

        Args:
            ref: 目标条目。
            **fields: 仅接受 ``status/title/summary/body/credentials/run_id/
                conv_id``；body 变更时 chars 同步重派生。

        Returns:
            更新后的完整条目；ref 不存在返回 None。

        Raises:
            ValueError: 字段不可更新，或 status 不在 :data:`STATUSES` 内。
        """
        unknown = [k for k in fields if k not in _UPDATABLE]
        if unknown:
            raise ValueError(
                f"update: non-updatable fields {unknown}; updatable: {list(_UPDATABLE)}"
            )
        if "status" in fields and fields["status"] not in STATUSES:
            raise ValueError(
                f"update: unknown status {fields['status']!r}; valid: {list(STATUSES)}"
            )
        creds_json = self._creds_to_json(fields["credentials"]) if "credentials" in fields else None
        with self._lock:
            try:
                if self._fetch(ref) is None:
                    return None
                sets: list[str] = []
                vals: list[Any] = []
                for key, value in fields.items():
                    if key == "credentials":
                        sets.append("credentials_json = ?")
                        vals.append(creds_json)
                    elif key == "body":
                        body = value or ""
                        sets += ["body = ?", "chars = ?"]
                        vals += [body, len(body)]
                    else:
                        sets.append(f"{key} = ?")
                        vals.append(value)
                sets.append("updated_ts = ?")
                vals.append(time.time())
                vals.append(ref)
                self._conn.execute(f"UPDATE sessions SET {', '.join(sets)} WHERE ref = ?", vals)
                self._enforce_lru()
                self._conn.commit()
                return self._row_to_dict(self._fetch(ref))
            except Exception:
                self._conn.rollback()
                raise

    # ---- 读取 ----

    def get(self, ref: str) -> dict | None:
        """取单条完整条目（含 body、credentials 已转 list）；miss 返回 None。"""
        with self._lock:
            row = self._fetch(ref)
            return self._row_to_dict(row) if row is not None else None

    def list(self, limit: int = DEFAULT_LIST_LIMIT) -> list[dict]:
        """最近 ``limit`` 条索引（按 no 升序、去 body；回放/快照拼装用）。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM sessions ORDER BY no DESC LIMIT ?", (limit,)
            ).fetchall()
        return [self._row_to_dict(row, with_body=False) for row in reversed(rows)]

    def count(self) -> int:
        """当前条数（LRU 生效后上限为 :data:`DEFAULT_MAX_ROWS`）。"""
        with self._lock:
            return int(self._conn.execute("SELECT COUNT(*) AS n FROM sessions").fetchone()["n"])

    # ---- 内部（调用方已持 ``self._lock``）----

    def _fetch(self, ref: str) -> sqlite3.Row | None:
        return self._conn.execute("SELECT * FROM sessions WHERE ref = ?", (ref,)).fetchone()

    def _next_no(self) -> int:
        # AUTOINCREMENT 高水位：sqlite_sequence 不随删行回退，旧号不复用
        cur = self._conn.execute("INSERT INTO no_seq DEFAULT VALUES")
        no = int(cur.lastrowid)
        self._conn.execute("DELETE FROM no_seq")
        return no

    def _enforce_lru(self, max_rows: int = DEFAULT_MAX_ROWS) -> None:
        # 超上限时删最旧（按 ts，次序 no 定序）
        n = self._conn.execute("SELECT COUNT(*) AS n FROM sessions").fetchone()["n"]
        if n > max_rows:
            self._conn.execute(
                "DELETE FROM sessions WHERE ref IN ("
                " SELECT ref FROM sessions ORDER BY ts ASC, no ASC LIMIT ?)",
                (n - max_rows,),
            )

    @staticmethod
    def _creds_to_json(credentials: Any) -> str:
        if credentials is None:
            return "[]"
        if not isinstance(credentials, (list, tuple)):
            raise TypeError(
                f"credentials must be list/tuple/None, got {type(credentials).__name__}"
            )
        return json.dumps(list(credentials), ensure_ascii=False)

    @staticmethod
    def _row_to_dict(row: sqlite3.Row, with_body: bool = True) -> dict:
        entry = {k: row[k] for k in _COLUMNS if with_body or k != "body"}
        entry["credentials"] = json.loads(entry.pop("credentials_json"))
        return entry
