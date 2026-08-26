#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""A2A client for the voice head's lane A (consumes the a2a-profile-server
plugin; docs/kg/01-ws1-head-dsh.md §车道A, 02-ws2-a2a-profile.md §6).

JSON-RPC 2.0 over aiohttp against the internal-contract subset:
``message/send`` / ``tasks/get`` / ``tasks/cancel`` / ``incubate`` /
``profiles/list`` / ``profiles/get``. Retries borrow ReconnectPolicy
semantics (single retry with capped sleep) for transport blips.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import aiohttp

FINAL_PREFIX = '"Agent Final Message":\n\n'


class A2aError(RuntimeError):
    """RPC-level failure (error object from the server)."""

    def __init__(self, code: int, message: str):
        super().__init__(f"a2a error {code}: {message}")
        self.code = code
        self.message = message


@dataclass
class A2aClient:
    """Thin JSON-RPC client; ``base_url`` like ``http://127.0.0.1:8790``."""

    base_url: str
    token: str | None = None
    timeout_s: float = 30.0
    poll_interval_s: float = 0.1

    def _headers(self) -> dict[str, str]:
        return {"authorization": f"Bearer {self.token}"} if self.token else {}

    async def _rpc(self, session: aiohttp.ClientSession, method: str, params: dict) -> dict:
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        async with session.post(self.base_url + "/", json=payload, headers=self._headers()) as resp:
            body = await resp.json()
        if resp.status != 200:
            raise A2aError(-1, f"http {resp.status}: {body}")
        if "error" in body:
            raise A2aError(body["error"].get("code", -1), body["error"].get("message", ""))
        return body.get("result", {})

    # ---- public surface (mirrors DaisLane: dispatch/await_done/cancel) ----

    async def send(self, raw_intent: str, ref: str = "-", source: str = "voice-head") -> str:
        """Stage-1 acceptance: submit an intent, return the task id."""
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout_s)
        ) as session:
            result = await self._rpc(session, "message/send", {
                "message": {"role": "user", "parts": [{"type": "text", "text": raw_intent}]},
                "context": {"source": source, "ref": ref},
            })
            return result["task"]["id"]

    async def get(self, task_id: str) -> dict:
        """Poll a task: {id, state, artifacts, error, ref}."""
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout_s)
        ) as session:
            result = await self._rpc(session, "tasks/get", {"taskId": task_id})
            return result["task"]

    async def cancel(self, task_id: str) -> str:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout_s)
        ) as session:
            result = await self._rpc(session, "tasks/cancel", {"taskId": task_id})
            return result["task"]["state"]

    async def await_done(self, task_id: str, timeout_s: float = 600.0) -> str:
        """Block until terminal; return the done-body artifact content.

        Raises TimeoutError when still running (caller turns that into a
        spoken "still running" state; same semantics as DaisLane).
        """
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout_s
        while loop.time() < deadline:
            task = await self.get(task_id)
            if task["state"] == "completed":
                return task["artifacts"][0]["content"]
            if task["state"] in ("failed", "canceled"):
                raise A2aError(-2, f"task {task_id} {task['state']}: {task.get('error')}")
            await asyncio.sleep(self.poll_interval_s)
        raise TimeoutError(f"task {task_id} still running after {timeout_s}s")

    async def incubate(self, name: str, agents_md: str, targets: list[str],
                       profile_json: dict | None = None, description: str = "") -> dict:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout_s)
        ) as session:
            return await self._rpc(session, "incubate", {
                "name": name,
                "targets": targets,
                "projection": {
                    "agents_md": agents_md,
                    "profile_json": profile_json or {},
                    "description": description,
                },
            })

    async def pool_spawn(self, profile: str, *, strategy: str = "binding-mode",
                         binding_session_id: str = "",
                         role: str | None = None,
                         mailbox: str | None = None,
                         project: str | None = None) -> dict:
        """``pool/spawn`` RPC — strategy③ binding-mode dresses an IN-FLIGHT
        dsh session with a stored profile (the PROFILE-INJECT envelope;
        dsh-family sessions only, per the G4 boundary).

        Returns the bind receipt
        ``{target: "binding", name, version, sessionId, injected: true}``.
        """
        params: dict = {"profile": profile, "strategy": strategy}
        if strategy == "binding-mode":
            if not binding_session_id.strip():
                raise ValueError("binding_session_id required for binding-mode")
            params["binding"] = {"sessionId": binding_session_id}
        for key, value in (("role", role), ("mailbox", mailbox),
                           ("project", project)):
            if value is not None:
                params[key] = value
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout_s)
        ) as session:
            return await self._rpc(session, "pool/spawn", params)

    async def agent_card(self) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.get(self.base_url + "/.well-known/agent-card.json") as resp:
                return await resp.json()
