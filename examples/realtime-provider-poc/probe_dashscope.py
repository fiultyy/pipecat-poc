#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Live DashScope Realtime protocol probe (needs a real API key).

Verifies, against the actual cloud, each protocol assumption the
QwenOmniRealtimeLLMService rewrite is built on — independent of pipecat:

  1. connect        wss://dashscope[|-intl].aliyuncs.com/api-ws/v1/realtime
  2. session.update our flattened dialect accepted -> session.updated
  3. text roundtrip conversation.item.create + response.create(modalities
                     text) -> response.text.delta* / response.done
  4. tool call      session.update(tools) -> model calls it ->
                     conversation.item.create(function_call_output) ->
                     response.create -> final text
  5. finish         session.finish -> session.finished

Usage:
    DASHSCOPE_API_KEY=sk-... .venv/bin/python probe_dashscope.py \
        [--model qwen3.5-omni-flash-realtime] [--intl] [--tool-test]

Audio input is not probed (no TTS on this box to synthesize speech); add
``--wav file.wav`` to also stream 16k/24k mono PCM16 chunks into
input_audio_buffer.append and watch server VAD events.
"""

import argparse
import asyncio
import base64
import json
import os
import sys
import uuid
import wave

import websockets

RESULTS = []


def log(step: str, ok: bool, detail: str = ""):
    RESULTS.append((step, ok, detail))
    mark = "PASS" if ok else "FAIL"
    print(f"[{mark}] {step}" + (f" — {detail}" if detail else ""), flush=True)


def eid() -> str:
    return "event_" + uuid.uuid4().hex


async def recv_until(ws, wanted: set[str], timeout: float = 15.0) -> list[dict]:
    """Collect server events until one of `wanted` types (or timeout)."""
    got: list[dict] = []
    try:
        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            evt = json.loads(raw)
            got.append(evt)
            print(f"    <- {evt.get('type')}", flush=True)
            if evt.get("type") in wanted:
                return got
            if evt.get("type") == "error":
                return got
    except (asyncio.TimeoutError, websockets.ConnectionClosed):
        return got


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default="qwen3.5-omni-flash-realtime")
    parser.add_argument("--intl", action="store_true", help="Singapore endpoint")
    parser.add_argument("--tool-test", action="store_true", help="run step 4 (tool call)")
    parser.add_argument("--wav", help="16k/24k mono PCM16 wav to stream (audio/VAD probe)")
    args = parser.parse_args()

    key = os.environ.get("DASHSCOPE_API_KEY")
    if not key:
        sys.exit("set DASHSCOPE_API_KEY first")

    host = "dashscope-intl.aliyuncs.com" if args.intl else "dashscope.aliyuncs.com"
    url = f"wss://{host}/api-ws/v1/realtime?model={args.model}"

    print(f"== probing {url} ==")
    try:
        ws = await websockets.connect(url, additional_headers={"Authorization": f"Bearer {key}"})
    except Exception as e:
        log("connect", False, str(e))
        for step, _, _ in RESULTS:
            print(f"ABORTED at {step}")
        return
    log("connect", True)

    try:
        # -- 2. session.update in our flattened DashScope dialect --
        await ws.send(json.dumps({
            "event_id": eid(), "type": "session.update",
            "session": {
                "modalities": ["text"],
                "instructions": "你是协议测试助手。回答务必简短。",
            },
        }))
        evts = await recv_until(ws, {"session.updated"})
        ok = any(e.get("type") == "session.updated" for e in evts)
        log("session.update(flat dialect)", ok,
            "" if ok else next((json.dumps(e, ensure_ascii=False) for e in evts if e.get("type") == "error"), "no reply"))

        # -- 3. text roundtrip --
        await ws.send(json.dumps({
            "event_id": eid(), "type": "conversation.item.create",
            "item": {"type": "message", "role": "user",
                     "content": [{"type": "input_text", "text": "只回复两个字：收到"}]},
        }))
        await recv_until(ws, {"conversation.item.created", "conversation.item.done"})
        await ws.send(json.dumps({
            "event_id": eid(), "type": "response.create",
            "response": {"modalities": ["text"]},
        }))
        evts = await recv_until(ws, {"response.done"})
        text = "".join(e.get("delta", "") for e in evts if e.get("type") == "response.text.delta")
        done = any(e.get("type") == "response.done" for e in evts)
        log("text roundtrip (response.text.delta)", done and bool(text), f'回复: {text!r}')
        event_names = {e.get("type") for e in evts}
        print(f"    observed events: {sorted(event_names)}")

        # -- 4. tool call roundtrip --
        if args.tool_test:
            await ws.send(json.dumps({
                "event_id": eid(), "type": "session.update",
                "session": {
                    "modalities": ["text"],
                    "tools": [{
                        "type": "function",
                        "name": "get_weather",
                        "description": "查询城市天气",
                        "parameters": {"type": "object", "properties": {
                            "city": {"type": "string"}}, "required": ["city"]},
                    }],
                },
            }))
            await recv_until(ws, {"session.updated"})
            await ws.send(json.dumps({
                "event_id": eid(), "type": "conversation.item.create",
                "item": {"type": "message", "role": "user",
                         "content": [{"type": "input_text", "text": "查一下杭州天气"}]},
            }))
            await recv_until(ws, {"conversation.item.created", "conversation.item.done"})
            await ws.send(json.dumps({
                "event_id": eid(), "type": "response.create",
                "response": {"modalities": ["text"]},
            }))
            evts = await recv_until(ws, {"response.done"})
            names = {e.get("type") for e in evts}
            print(f"    tool-phase events: {sorted(names)}")
            fcd = next((e for e in evts if e.get("type") == "response.function_call_arguments.done"), None)
            item_done = [e for e in evts if e.get("type") == "conversation.item.done"]
            call = None
            for e in item_done:
                it = e.get("item", {})
                if it.get("type") == "function_call":
                    call = it
            if fcd or call:
                call = call or {}
                call_id = call.get("call_id") or fcd.get("call_id")
                log("tool call event names", True,
                    f"observed: {sorted(n for n in names if 'function' in n or 'call' in n)}")
                await ws.send(json.dumps({
                    "event_id": eid(), "type": "conversation.item.create",
                    "item": {"type": "function_call_output", "call_id": call_id,
                             "output": json.dumps({"weather": "晴 30C"})},
                }))
                await recv_until(ws, {"conversation.item.created", "conversation.item.done"})
                await ws.send(json.dumps({
                    "event_id": eid(), "type": "response.create",
                    "response": {"modalities": ["text"]},
                }))
                evts = await recv_until(ws, {"response.done"})
                text = "".join(e.get("delta", "") for e in evts if e.get("type") == "response.text.delta")
                log("tool result -> final text", bool(text), f"回复: {text!r}")
            else:
                log("tool call event names", False, f"no function_call events; got {sorted(names)}")

        # -- optional audio/VAD probe --
        if args.wav:
            with wave.open(args.wav, "rb") as w:
                rate = w.getframerate()
                chunk = w.readframes(int(rate * 0.1))  # 100ms
                while chunk:
                    await ws.send(json.dumps({
                        "event_id": eid(), "type": "input_audio_buffer.append",
                        "audio": base64.b64encode(chunk).decode(),
                    }))
                    chunk = w.readframes(int(rate * 0.1))
                    await asyncio.sleep(0.1)
            evts = await recv_until(ws, {"input_audio_buffer.speech_started"}, timeout=10)
            names = {e.get("type") for e in evts}
            log("audio append + server VAD", "input_audio_buffer.speech_started" in names,
                f"events: {sorted(names)}")

        # -- 5. graceful finish --
        await ws.send(json.dumps({"event_id": eid(), "type": "session.finish"}))
        evts = await recv_until(ws, {"session.finished"}, timeout=10)
        log("session.finish -> session.finished",
            any(e.get("type") == "session.finished" for e in evts))
    finally:
        await ws.close()

    print("\n== summary ==")
    for step, ok, detail in RESULTS:
        print(f"  {'✓' if ok else '✗'} {step}")


if __name__ == "__main__":
    asyncio.run(main())
