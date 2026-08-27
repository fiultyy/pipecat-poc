#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Qwen-Omni-Realtime LLM service implementation (DashScope Realtime WS API)."""

import asyncio
import json
from typing import Callable

from loguru import logger

from pipecat.frames.frames import LLMFullResponseStartFrame
from pipecat.services.openai import realtime as openai_realtime
from pipecat.services.openai.realtime.llm import OpenAIRealtimeLLMService
from pipecat.utils.types import is_given

DASHSCOPE_REALTIME_BASE_URL_CN = "wss://dashscope.aliyuncs.com/api-ws/v1/realtime"
DASHSCOPE_REALTIME_BASE_URL_INTL = "wss://dashscope-intl.aliyuncs.com/api-ws/v1/realtime"

DEFAULT_QWEN_OMNI_REALTIME_MODEL = "qwen3.5-omni-flash-realtime"

# Server event names emitted by DashScope that differ from OpenAI's. Maps the
# DashScope name to the OpenAI name the parent service dispatches on.
# Sources: dashscope SDK 1.27.0 omni_realtime.py + Model Studio docs
# (help.aliyun.com/zh/model-studio/server-events) + live probing.
# - response.audio.delta / .done          (audio-modality output)
# - response.audio_transcript.delta/.done (text transcript of spoken output)
# - response.text.delta / .done           (text-only modality output)
#
# NOTE on input transcription: qwen3.5-omni models are end-to-end — the
# separate input_audio_transcription pipeline (which qwen3-omni /
# qwen-omni-turbo support via session.input_audio_transcription) does not
# emit transcript content on these endpoints (docs: "各环节无法独立")， so
# no user-voice TranscriptionFrames should be expected from them. The
# aliases below are kept for the older models that do.
_SERVER_EVENT_ALIASES = {
    "response.audio.delta": "response.output_audio.delta",
    "response.audio_transcript.delta": "response.output_audio_transcript.delta",
    "response.audio.done": "response.output_audio.done",
    "response.audio_transcript.done": "response.output_audio_transcript.done",
    "response.text.delta": "response.output_text.delta",
    "response.text.done": "response.output_text.done",
    # DashScope acknowledges seeded conversation items with
    # conversation.item.created (probe-verified 2026-08-22); the parent's
    # dispatch handles the equivalent conversation.item.added.
    "conversation.item.created": "conversation.item.added",
}

# Events the parent's whitelist parser would reject (killing the receive
# loop) that carry nothing the parent acts on — dropped instead.
# - response.function_call_arguments.delta: the parent only acts on .done.
# - session.finished: sent by some DashScope endpoints on teardown; the
#   socket closes right after, ending the receive loop naturally.
_SERVER_EVENT_DROPS = {
    "response.function_call_arguments.delta",
    "session.finished",
}


def _mirror_item_text(item) -> str:
    """Extract the displayable text of one realtime conversation item."""
    if item.type == "function_call":
        return item.arguments or ""
    if item.type == "function_call_output":
        return item.output or ""
    parts = []
    for c in item.content or []:
        t = getattr(c, "transcript", None) or getattr(c, "text", None) or ""
        if t:
            parts.append(t)
    return "\n".join(parts)


def _mirror_item_dict(item) -> dict:
    """Flatten one conversation item into the mirror tap's dict form."""
    return {
        "item_id": item.id,
        "type": item.type,
        "role": item.role,
        "text": _mirror_item_text(item),
        "name": item.name,
        "call_id": item.call_id,
    }


class QwenOmniRealtimeLLMService(OpenAIRealtimeLLMService):
    """Realtime voice service for Alibaba DashScope's Qwen-Omni-Realtime models.

    Works like :class:`OpenAIRealtimeLLMService` (server VAD turn proposals,
    streaming audio/text responses, function calling, transcription) against
    the DashScope endpoint. See the module docstring for the adapted protocol
    differences.

    Example::

        from pipecat.services.qwen.realtime import QwenOmniRealtimeLLMService
        from pipecat.services.openai.realtime import events

        service = QwenOmniRealtimeLLMService(
            api_key=os.environ["DASHSCOPE_API_KEY"],
            settings=QwenOmniRealtimeLLMService.Settings(
                model="qwen3.5-omni-flash-realtime",
                session_properties=events.SessionProperties(
                    instructions="You are a helpful voice assistant.",
                ),
            ),
        )
    """

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = DASHSCOPE_REALTIME_BASE_URL_CN,
        workspace_id: str | None = None,
        settings: OpenAIRealtimeLLMService.Settings | None = None,
        **kwargs,
    ):
        """Initialize the Qwen-Omni-Realtime service.

        Args:
            api_key: DashScope API key (Bearer token).
            base_url: WebSocket base URL. Defaults to the China (Beijing)
                endpoint; pass ``DASHSCOPE_REALTIME_BASE_URL_INTL`` for the
                international (Singapore) site.
            workspace_id: Optional DashScope workspace ID, sent as the
                ``X-DashScope-WorkSpace`` header.
            settings: Runtime-updatable settings. The model defaults to
                ``qwen3.5-omni-flash-realtime``.
            **kwargs: Additional arguments passed to the parent service.
        """
        settings = settings or self.Settings()
        if not is_given(settings.model):
            settings.model = DEFAULT_QWEN_OMNI_REALTIME_MODEL
        super().__init__(
            api_key=api_key,
            base_url=base_url,
            settings=settings,
            **kwargs,
        )
        self._workspace_id = workspace_id
        # Protocol-level turn state: cleared on ``response.created``, set on
        # ``response.done``. Lets callers queue work until the turn ends
        # instead of racing ``response.create`` into the single
        # active-response slot — a losing race the server drops silently.
        self.turn_idle: asyncio.Event = asyncio.Event()
        self.turn_idle.set()
        # Optional taps for a client-side conversation mirror and compaction
        # (kg/14 §2.5). Both default to None — unset, the service behaves
        # exactly as before.
        # - mirror_sink: fed one flat dict per conversation.item.added/.done
        #   handler pass (see ``_mirror_item_dict`` for the field set).
        # - on_turn_idle: invoked synchronously right after response.done
        #   settles the turn, in the receive-loop context — schedule async
        #   work from it, never block.
        self.mirror_sink: Callable[[dict], None] | None = None
        self.on_turn_idle: Callable[[], None] | None = None

    def _track_turn_state(self, evt) -> None:
        """Mirror protocol response lifecycle onto :attr:`turn_idle`."""
        if evt.type == "response.created":
            self.turn_idle.clear()
        elif evt.type == "response.done":
            self.turn_idle.set()
            if self.on_turn_idle is not None:
                try:
                    self.on_turn_idle()
                except Exception as e:
                    logger.debug(f"{self} on_turn_idle callback failed: {e}")

    async def _connect(self):
        try:
            if self._websocket:
                return
            headers = {"Authorization": f"Bearer {self.api_key}"}
            if self._workspace_id:
                headers["X-DashScope-WorkSpace"] = self._workspace_id
            # Model must ride the URI query (probe-verified 2026-08-26):
            # without it the endpoint accepts session.update and even
            # transcribes appended audio, but never emits VAD events
            # (input_audio_buffer.speech_started/stopped) nor the
            # auto-committed response — the session is silently half-alive.
            uri = self.base_url
            if "?" not in uri:
                uri = f"{uri}?model={self._settings.model}"
            self._websocket = await openai_realtime.llm.websocket_connect(
                uri=uri,
                additional_headers=headers,
            )
            self._receive_task = self.create_task(self._receive_task_handler())
        except Exception as e:
            await self.push_error(error_msg=f"Error connecting: {e}", exception=e)
            self._websocket = None

    async def _disconnect(self):
        try:
            # Teardown for qwen3.5-omni endpoints is a plain socket close:
            # session.finish is rejected there ("Invalid value", probe-verified
            # 2026-08-22) even after a completed turn, and the error reply it
            # provokes would surface as a spurious ErrorFrame. The Model
            # Studio docs allow direct disconnect for Qwen-Omni-Realtime.
            await super()._disconnect()
        except Exception as e:
            await self.push_error(error_msg=f"Error disconnecting: {e}", exception=e)

    async def send_client_event(self, event: openai_realtime.events.ClientEvent):
        """Send a client event, rewriting dialect differences to DashScope's form.

        ``session.update``: fields flattened (see ``_rewrite_session_update``).
        ``response.create``: ``response.output_modalities`` renamed to
        ``modalities`` (same difference as in session.update; without this
        the server ignores the modality override).

        Args:
            event: The client event (Pydantic model) to send.
        """
        if isinstance(event, openai_realtime.events.SessionUpdateEvent):
            payload = event.model_dump(exclude_none=True)
            self._rewrite_session_update(payload)
            await self._ws_send(payload)
        elif isinstance(event, openai_realtime.events.ResponseCreateEvent):
            payload = event.model_dump(exclude_none=True)
            response = payload.get("response")
            if isinstance(response, dict) and "output_modalities" in response:
                response["modalities"] = response.pop("output_modalities")
            await self._ws_send(payload)
        else:
            await super().send_client_event(event)

    @staticmethod
    def _rewrite_session_update(payload: dict) -> None:
        """Normalize an OpenAI ``session.update`` payload to DashScope's dialect, in place.

        OpenAI nests voice under ``audio.output``, turn detection and input
        transcription under ``audio.input``; DashScope expects them at the
        top level of the session object, and calls output modalities
        ``modalities``. Unknown OpenAI-only bookkeeping fields are dropped.
        """
        session = payload.get("session")
        if not isinstance(session, dict):
            return

        if "output_modalities" in session:
            session["modalities"] = session.pop("output_modalities")

        audio = session.pop("audio", None)
        if isinstance(audio, dict):
            audio_in = audio.get("input") or {}
            audio_out = audio.get("output") or {}
            if "voice" in audio_out:
                session["voice"] = audio_out["voice"]
            if "turn_detection" in audio_in:
                session["turn_detection"] = audio_in["turn_detection"]
            if "transcription" in audio_in:
                session["input_audio_transcription"] = audio_in["transcription"]
            # audio.input/output.format keeps the same nested position in the
            # DashScope new-style structure; keep it if present.
            if "format" in audio_in or "format" in audio_out:
                session["audio"] = {
                    k: v for k, v in (("input", audio_in.get("format")), ("output", audio_out.get("format"))) if v
                }

        # DashScope uses flat legacy names for the audio codecs unless the
        # nested audio.format structure is used.
        if "audio" not in session:
            if isinstance(session.get("input_audio_format"), dict):
                session["input_audio_format"] = session["input_audio_format"].get("type")
            if isinstance(session.get("output_audio_format"), dict):
                session["output_audio_format"] = session["output_audio_format"].get("type")

        for field in ("type", "object", "id", "expires_at", "tracing", "prompt", "include"):
            session.pop(field, None)

        # Tools: OpenAI nests the declaration under a "function" key; DashScope
        # takes it flat (probe-verified 2026-08-22: flat declarations are
        # accepted and invoked, nested ones are silently ignored).
        tools = session.get("tools")
        if isinstance(tools, list):
            session["tools"] = [QwenOmniRealtimeLLMService._flatten_tool(t) for t in tools]

    @staticmethod
    def _flatten_tool(tool: dict) -> dict:
        """Convert an OpenAI-style tool declaration to DashScope's flat form."""
        fn = tool.get("function")
        if isinstance(fn, dict):
            flat = {"type": "function", **fn}
            return flat
        return tool

    async def _create_response(self):
        # Same as the parent, except the session update (carrying tools) is
        # sent BEFORE seeding conversation items: DashScope only honors
        # tools registered before the items exist (probe-verified 2026-08-22;
        # updates after seeding leave the model text-mimicking the call).
        if not self._api_session_ready:
            self._run_llm_when_api_session_ready = True
            return

        assert self._context is not None

        adapter = self.get_llm_adapter()

        if self._llm_needs_conversation_setup:
            logger.debug(
                f"Setting up conversation on {self} with initial messages: "
                f"{adapter.get_messages_for_logging(self._context)}"
            )

            # Send new settings (incl. tools) first — see method docstring.
            await self._send_session_update()

            # Then seed the initial messages.
            llm_invocation_params = adapter.get_llm_invocation_params(self._context)
            messages = llm_invocation_params["messages"]
            for item in messages:
                evt = openai_realtime.events.ConversationItemCreateEvent(item=item)
                self._messages_added_manually[evt.item.id] = True
                await self.send_client_event(evt)

            self._llm_needs_conversation_setup = False

        logger.debug("Creating response")

        await self.push_frame(LLMFullResponseStartFrame())
        await self.start_processing_metrics()
        await self.start_ttfb_metrics()
        await self.send_client_event(
            openai_realtime.events.ResponseCreateEvent(
                response=openai_realtime.events.ResponseProperties(
                    output_modalities=self._get_enabled_modalities()
                )
            )
        )

    async def _truncate_current_audio_response(self):
        # DashScope has no conversation.item.truncate event; response.cancel
        # (sent on speech_started) is the supported way to stop playback, so
        # only drop the local tracking state.
        self._current_audio_response = None

    def _feed_mirror(self, item) -> None:
        """Feed :attr:`mirror_sink` one item as a flat dict, if subscribed.

        Sink exceptions are contained at debug level: the mirror is an
        observation tap and must never break the receive path.
        """
        if self.mirror_sink is None:
            return
        try:
            self.mirror_sink(_mirror_item_dict(item))
        except Exception as e:
            logger.debug(f"{self} mirror_sink failed: {e}")

    async def _handle_evt_conversation_item_added(self, evt):
        # Mirror before the parent: client-seeded items (gateway final/
        # snapshot injections) must land in the mirror too, and the parent
        # returns early on them.
        self._feed_mirror(evt.item)
        await super()._handle_evt_conversation_item_added(evt)

    async def _handle_evt_conversation_item_done(self, evt):
        self._feed_mirror(evt.item)
        await super()._handle_evt_conversation_item_done(evt)

    async def delete_conversation_item(self, item_id: str) -> None:
        """Delete one server-side conversation item by id.

        Compaction entry (kg/14 §2.5): the client-side plan deletes every
        non-pinned item so the server-side context shrinks without a session
        rollover. The event needs no DashScope rewriting.
        """
        await self.send_client_event(
            openai_realtime.events.ConversationItemDeleteEvent(item_id=item_id)
        )

    async def _receive_task_handler(self):
        # Copied from OpenAIRealtimeLLMService with three DashScope-specific
        # guards: names are normalized/dropped before parsing, a parse failure
        # on an unknown event logs and continues, and a handler exception on
        # one event is contained instead of killing the receive loop (a dead
        # loop silently stops serving every following turn).
        assert self._websocket is not None

        async for message in self._websocket:
            normalized = self._normalize_server_message(message)
            if normalized is None:
                continue
            try:
                evt = openai_realtime.events.parse_server_event(normalized)
            except Exception as e:
                logger.warning(f"{self} skipped unparseable server event: {e}")
                continue
            self._track_turn_state(evt)
            try:
                if await self._dispatch_server_event(evt) == "fatal":
                    return
            except Exception as e:
                logger.error(f"{self} handler failed on {evt.type}, continuing: {e}")

    async def _dispatch_server_event(self, evt):
            if evt.type == "session.created":
                await self._handle_evt_session_created(evt)
            elif evt.type == "session.updated":
                await self._handle_evt_session_updated(evt)
            elif evt.type == "response.output_audio.delta":
                await self._handle_evt_audio_delta(evt)
            elif evt.type == "conversation.item.added":
                await self._handle_evt_conversation_item_added(evt)
            elif evt.type == "conversation.item.done":
                await self._handle_evt_conversation_item_done(evt)
            elif evt.type == "conversation.item.input_audio_transcription.delta":
                await self._handle_evt_input_audio_transcription_delta(evt)
            elif evt.type == "conversation.item.input_audio_transcription.completed":
                await self.handle_evt_input_audio_transcription_completed(evt)
            elif evt.type == "conversation.item.retrieved":
                await self._handle_conversation_item_retrieved(evt)
            elif evt.type == "response.done":
                await self._handle_evt_response_done(evt)
            elif evt.type == "input_audio_buffer.speech_started":
                # Barge-in: cancel the in-flight response before the usual
                # truncate path. DashScope keeps streaming the old response's
                # audio deltas unless explicitly cancelled; the OpenAI parent
                # only cancels in manual turn-detection mode.
                try:
                    await self.send_client_event(
                        openai_realtime.events.ResponseCancelEvent()
                    )
                except Exception as e:
                    logger.debug(f"{self} response.cancel on barge-in: {e}")
                await self._handle_evt_speech_started(evt)
            elif evt.type == "input_audio_buffer.speech_stopped":
                await self._handle_evt_speech_stopped(evt)
            elif evt.type == "response.output_text.delta":
                await self._handle_evt_text_delta(evt)
            elif evt.type == "response.output_audio_transcript.delta":
                await self._handle_evt_audio_transcript_delta(evt)
            elif evt.type == "response.function_call_arguments.done":
                await self._handle_evt_function_call_arguments_done(evt)
            elif evt.type == "error":
                if not await self._maybe_handle_evt_retrieve_conversation_item_error(evt):
                    if self._is_recoverable_response_error(evt):
                        logger.debug(f"{self} {evt.error.message}")
                    else:
                        await self._handle_evt_error(evt)
                        return "fatal"

    @staticmethod
    def _is_recoverable_response_error(evt) -> bool:
        """Whether a server error is a benign turn-state race, not fatal.

        DashScope sometimes reports these with an empty ``code`` and the
        semantics only in ``message``, so both are matched: a
        ``response.create`` racing the server-VAD auto-commit (``Conversation
        already has an active response``) and a cancel landing after the
        response finished (``response_cancel_not_active``). Killing the
        receive loop on either would drop the in-flight response's
        ``response.done`` and with it the assistant turn-end frames.
        """
        code = evt.error.code or ""
        message = evt.error.message or ""
        return (
            code in ("response_cancel_not_active", "conversation_already_has_active_response")
            or "already has an active response" in message
            or "no active response" in message.lower()
        )

    @staticmethod
    def _normalize_server_message(message: str | bytes) -> str | None:
        """Rewrite DashScope server event names to OpenAI's; None to drop.

        Events in ``_SERVER_EVENT_DROPS`` return None (the receive loop
        skips them); aliased names are rewritten in place.
        """
        try:
            data = json.loads(message)
        except (TypeError, json.JSONDecodeError):
            return message if isinstance(message, str) else None
        if isinstance(data, dict):
            evt_type = data.get("type")
            if evt_type in _SERVER_EVENT_DROPS:
                return None
            alias = _SERVER_EVENT_ALIASES.get(evt_type)
            if alias:
                data["type"] = alias
            # Input transcription delta: DashScope carries the running preview
            # in ``stash`` and leaves the OpenAI ``delta`` field absent. The
            # parent's model requires ``delta``; synthesize it from text+stash.
            # Interim frames are replace-style, so a full preview is the
            # correct value.
            if evt_type == "conversation.item.input_audio_transcription.delta":
                if not data.get("delta"):
                    data["delta"] = (data.get("text") or "") + (data.get("stash") or "")
            # response.created/.done: DashScope names usage details with a
            # trailing s and omits status_details/output; rename/fill so the
            # parent's pydantic model validates. Usage may also arrive with
            # no detail sub-objects at all, which must be backfilled or the
            # whole turn-end event fails validation.
            if evt_type in ("response.created", "response.done") and isinstance(
                data.get("response"), dict
            ):
                resp = data["response"]
                resp.setdefault("status_details", {})
                resp.setdefault("output", [])
                usage = resp.get("usage")
                if isinstance(usage, dict):
                    for src, dst in (
                        ("input_tokens_details", "input_token_details"),
                        ("output_tokens_details", "output_token_details"),
                    ):
                        if src in usage:
                            usage[dst] = usage.pop(src)
                    usage.setdefault("input_token_details", {})
                    usage.setdefault("output_token_details", {})
        return json.dumps(data)
