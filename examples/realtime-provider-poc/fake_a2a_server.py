#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Fake A2A server (internal-contract subset) for offline development.

Mirrors the wire contract the real plugin (``~/.dsh/plugins/a2a-profile-
server``, docs/kg/02-ws2-a2a-profile.md §2) will expose, so ``A2aClient``
(N1 lane A) and conformance tests can be built against it before the
cordis plugin exists:

- ``GET /.well-known/agent-card.json``
- ``POST /`` JSON-RPC 2.0: ``message/send`` / ``tasks/get`` / ``tasks/cancel``

Fake behavior: every dispatched task auto-completes after ``complete_s``
with an echo artifact carrying the credential convention
(``【凭证…】``, docs/kg/05-contracts.md §2). Run standalone on :8791.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid

from aiohttp import web

INTERNAL_VERSION = "internal-1"


class FakeTaskStore:
    """In-memory task table with the internal state machine.

    submitted → working → completed | failed | canceled
    """

    def __init__(self, complete_s: float = 0.2):
        self._tasks: dict[str, dict] = {}
        self._complete_s = complete_s

    def create(self, raw_intent: str, ref: str = "-") -> dict:
        task_id = "t_" + uuid.uuid4().hex[:8]
        task = {
            "id": task_id,
            "state": "submitted",
            "intent": raw_intent,
            "ref": ref,
            "created_at": time.time(),
            "artifacts": [],
            "error": None,
        }
        self._tasks[task_id] = task
        return task

    def get(self, task_id: str) -> dict | None:
        return self._tasks.get(task_id)

    async def drive(self, task_id: str) -> None:
        """Advance a task through working → completed (fake worker)."""
        task = self._tasks[task_id]
        await asyncio.sleep(self._complete_s)
        task["state"] = "working"
        await asyncio.sleep(self._complete_s)
        credential = f"【凭证FAKE-{task_id}】"
        task["artifacts"] = [{
            "type": "done-body",
            "content": f"\"Agent Final Message\":\n\n{task['intent']} 已完成 {credential}",
        }]
        task["state"] = "completed"

    def cancel(self, task_id: str) -> dict | None:
        task = self._tasks.get(task_id)
        if task and task["state"] in ("submitted", "working"):
            task["state"] = "canceled"
            return task
        return None


def _rpc_error(req_id, code: int, message: str) -> dict:
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}


def build_app(store: FakeTaskStore) -> web.Application:
    async def agent_card(_req: web.Request) -> web.Response:
        return web.json_response({
            "name": "voice-head-orchestrator",
            "description": "fake a2a endpoint for offline dev",
            "skills": [
                {"id": "dispatch", "description": "fan out a raw intent"},
                {"id": "query", "description": "task status"},
            ],
            "url": "http://127.0.0.1:8791/",
            "version": INTERNAL_VERSION,
        })

    async def rpc(req: web.Request) -> web.Response:
        payload = await req.json()
        method, params = payload.get("method"), payload.get("params") or {}
        req_id = payload.get("id")

        if method == "message/send":
            parts = params.get("message", {}).get("parts", [])
            raw_intent = next((p.get("text", "") for p in parts if p.get("type") == "text"), "")
            if not raw_intent:
                return web.json_response(_rpc_error(req_id, -32602, "empty text part"))
            task = store.create(raw_intent, params.get("context", {}).get("ref", "-"))
            asyncio.create_task(store.drive(task["id"]))
            return web.json_response({"jsonrpc": "2.0", "id": req_id, "result": {"task": {"id": task["id"], "state": task["state"]}}})

        if method == "tasks/get":
            task = store.get(params.get("taskId", ""))
            if not task:
                return web.json_response(_rpc_error(req_id, -32602, "unknown taskId"))
            return web.json_response({
                "jsonrpc": "2.0", "id": req_id,
                "result": {"task": {k: task[k] for k in ("id", "state", "artifacts", "error")}},
            })

        if method == "tasks/cancel":
            task = store.cancel(params.get("taskId", ""))
            if not task:
                return web.json_response(_rpc_error(req_id, -32602, "unknown/uncancelable taskId"))
            return web.json_response({"jsonrpc": "2.0", "id": req_id, "result": {"task": {"id": task["id"], "state": task["state"]}}})

        return web.json_response(_rpc_error(req_id, -32601, f"method not found: {method}"))

    app = web.Application()
    app.router.add_get("/.well-known/agent-card.json", agent_card)
    app.router.add_post("/", rpc)
    return app


def main(port: int = 8791) -> None:
    store = FakeTaskStore()
    web.run_app(build_app(store), host="127.0.0.1", port=port, print=None)


if __name__ == "__main__":
    main()
