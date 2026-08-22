#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Qwen-Omni-Realtime LLM service implementation (DashScope Realtime WS API)."""

import json

from loguru import logger

from pipecat.services.openai import realtime as openai_realtime
from pipecat.services.openai.realtime.llm import OpenAIRealtimeLLMService
from pipecat.utils.types import is_given

DASHSCOPE_REALTIME_BASE_URL_CN = "wss://dashscope.aliyuncs.com/api-ws/v1/realtime"
DASHSCOPE_REALTIME_BASE_URL_INTL = "wss://dashscope-intl.aliyuncs.com/api-ws/v1/realtime"

DEFAULT_QWEN_OMNI_REALTIME_MODEL = "qwen3.5-omni-flash-realtime"

# Server event names emitted by DashScope that differ from OpenAI's. Maps the
# DashScope name to the OpenAI name the parent service dispatches on.
# Sources: dashscope SDK 1.27.0 omni_realtime.py + Model Studio docs
# (help.aliyun.com/zh/model-studio/server-events):
# - response.audio.delta / .done          (audio-modality output)
# - response.audio_transcript.delta/.done (text transcript of spoken output)
# - response.text.delta / .done           (text-only modality output)
_SERVER_EVENT_ALIASES = {
    "response.audio.delta": "response.output_audio.delta",
    "response.audio_transcript.delta": "response.output_audio_transcript.delta",
    "response.audio.done": "response.output_audio.done",
    "response.audio_transcript.done": "response.output_audio_transcript.done",
    "response.text.delta": "response.output_text.delta",
    "response.text.done": "response.output_text.done",
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

    async def _connect(self):
        try:
            if self._websocket:
                return
            headers = {"Authorization": f"Bearer {self.api_key}"}
            if self._workspace_id:
                headers["X-DashScope-WorkSpace"] = self._workspace_id
            self._websocket = await openai_realtime.llm.websocket_connect(
                uri=self.base_url,
                additional_headers=headers,
            )
            self._receive_task = self.create_task(self._receive_task_handler())
        except Exception as e:
            await self.push_error(error_msg=f"Error connecting: {e}", exception=e)
            self._websocket = None

    async def _disconnect(self):
        try:
            # Graceful teardown: ask the server to finish the session before
            # closing the socket. Best-effort; teardown must not hang.
            if self._websocket and not self._disconnecting:
                try:
                    await self._websocket.send(
                        json.dumps({"event_id": "event_session_finish", "type": "session.finish"})
                    )
                except Exception:
                    pass
            await super()._disconnect()
        except Exception as e:
            await self.push_error(error_msg=f"Error disconnecting: {e}", exception=e)

    async def send_client_event(self, event: openai_realtime.events.ClientEvent):
        """Send a client event, rewriting ``session.update`` to the DashScope dialect.

        Args:
            event: The client event (Pydantic model) to send.
        """
        if isinstance(event, openai_realtime.events.SessionUpdateEvent):
            payload = event.model_dump(exclude_none=True)
            self._rewrite_session_update(payload)
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

    async def _receive_task_handler(self):
        # Copied from OpenAIRealtimeLLMService: the parent loop has no
        # per-message hook, and DashScope renames the audio delta events and
        # adds session.finished. Event names are normalized before parsing so
        # the parent handlers are reused as-is.
        assert self._websocket is not None

        async for message in self._websocket:
            normalized = self._normalize_server_message(message)
            evt = openai_realtime.events.parse_server_event(normalized)
            if evt.type == "session.created":
                await self._handle_evt_session_created(evt)
            elif evt.type == "session.updated":
                await self._handle_evt_session_updated(evt)
            elif evt.type == "session.finished":
                logger.info(f"{self} session finished")
                return
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
                    if evt.error.code in (
                        "response_cancel_not_active",
                        "conversation_already_has_active_response",
                    ):
                        logger.debug(f"{self} {evt.error.message}")
                    else:
                        await self._handle_evt_error(evt)
                        return

    @staticmethod
    def _normalize_server_message(message: str | bytes) -> str:
        """Rewrite DashScope server event names to OpenAI's, when they differ."""
        try:
            data = json.loads(message)
        except (TypeError, json.JSONDecodeError):
            return message
        if isinstance(data, dict):
            alias = _SERVER_EVENT_ALIASES.get(data.get("type"))
            if alias:
                data["type"] = alias
        return json.dumps(data)
