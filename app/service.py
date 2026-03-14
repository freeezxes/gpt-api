from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from openai import OpenAI

from app.analytics_toolkit import AnalyticsToolkit
from app.config import Settings
from app.schemas import ObjectChatRequest, ObjectChatResponse
from app.tracker_client import TrackerClient


class ConfigurationError(RuntimeError):
    """Raised when required local configuration is missing."""


class ObjectNotFoundError(RuntimeError):
    """Retained for compatibility with HTTP layer mappings."""


class ObjectChatService:
    def __init__(
        self,
        settings: Settings,
        tracker_client: TrackerClient | None = None,
        openai_client: OpenAI | None = None,
    ) -> None:
        self.settings = settings
        self.tracker_client = tracker_client or TrackerClient(
            base_url=settings.tracker_api_base_url,
            timeout_seconds=settings.request_timeout_seconds,
        )
        self.openai_client = openai_client

    def answer_question(self, payload: ObjectChatRequest) -> ObjectChatResponse:
        if not self.settings.openai_api_key:
            raise ConfigurationError("OPENAI_API_KEY is not set")
        if self.openai_client is None:
            self.openai_client = OpenAI(api_key=self.settings.openai_api_key)

        selected_model = payload.model or self.settings.openai_model
        toolkit = AnalyticsToolkit(request=payload, tracker_client=self.tracker_client)
        response = self._create_response(
            model=selected_model,
            instructions=self._build_instructions(payload),
            input_payload=self._build_user_input(payload),
            tools=toolkit.tool_definitions(),
            previous_response_id=payload.previous_response_id,
        )

        for _ in range(self.settings.openai_max_tool_rounds):
            function_calls = self._extract_function_calls(response)
            if not function_calls:
                break

            tool_outputs = []
            for function_call in function_calls:
                output = toolkit.execute(
                    name=function_call["name"], arguments=function_call["arguments"]
                )
                output = self._auto_retry_tool_if_possible(
                    function_name=function_call["name"],
                    arguments=function_call["arguments"],
                    output=output,
                    toolkit=toolkit,
                )
                tool_outputs.append(
                    {
                        "type": "function_call_output",
                        "call_id": function_call["call_id"],
                        "output": json.dumps(output, ensure_ascii=False),
                    }
                )

            response = self._create_response(
                model=selected_model,
                instructions=self._build_instructions(payload),
                input_payload=tool_outputs,
                tools=toolkit.tool_definitions(),
                previous_response_id=self._read_field(response, "id"),
            )
        else:
            raise RuntimeError("OpenAI tool loop exceeded the configured max number of rounds")

        answer = self._extract_output_text(response)
        if not answer:
            raise RuntimeError("OpenAI returned an empty answer")

        resolved_model = self._read_field(response, "model") or selected_model
        response_id = self._read_field(response, "id")
        return ObjectChatResponse(
            answer=answer,
            model=resolved_model,
            response_id=response_id,
            context=toolkit.build_context(),
        )

    def _create_response(
        self,
        *,
        model: str,
        instructions: str,
        input_payload: str | list[dict[str, Any]],
        tools: list[dict[str, Any]],
        previous_response_id: str | None,
    ):
        request_payload: dict[str, Any] = {
            "model": model,
            "instructions": instructions,
            "input": input_payload,
            "tools": tools,
            "max_output_tokens": self.settings.openai_max_output_tokens,
            "store": True,
        }
        if model.startswith("gpt-5"):
            request_payload["reasoning"] = {
                "effort": self.settings.openai_reasoning_effort
            }
        if previous_response_id:
            request_payload["previous_response_id"] = previous_response_id

        try:
            return self.openai_client.responses.create(**request_payload)
        except Exception as exc:
            raise RuntimeError(f"OpenAI request failed: {exc}") from exc

    @staticmethod
    def _build_user_input(payload: ObjectChatRequest) -> str:
        request_context = {
            "store_id": payload.store_id,
            "selected_object_id": payload.object_id,
            "default_time_window": {
                "start_time": payload.start_time.isoformat() if payload.start_time else None,
                "end_time": payload.end_time.isoformat() if payload.end_time else None,
            },
            "timezone": payload.timezone,
            "previous_response_id": payload.previous_response_id,
        }
        return (
            f"User question: {payload.question}\n\n"
            "Current request context JSON:\n"
            f"{json.dumps(request_context, ensure_ascii=False)}"
        )

    @staticmethod
    def _build_instructions(payload: ObjectChatRequest) -> str:
        now_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        selected_object_line = (
            f"Current selected object id: {payload.object_id}. "
            "Use it only when the user is clearly asking about the current object."
            if payload.object_id is not None
            else "There is no selected object in the current request."
        )
        default_window_line = (
            f"Default request interval: {payload.start_time.isoformat()} to {payload.end_time.isoformat()}."
            if payload.start_time and payload.end_time
            else "There is no default interval in the current request."
        )
        return (
            "Formatting re-enabled.\n"
            "You are an analytics agent for Myrza Tracker.\n"
            "Your job is to decide which backend tools are needed, call them, and then answer.\n"
            "Never invent analytics when a tool can verify them.\n"
            "Use the minimum number of tools needed for a reliable answer.\n"
            "Use `get_daily_counts` for questions about daily dynamics, trends, maxima by day, "
            "or phrases like 'за последний месяц', 'по дням', 'в какой день'.\n"
            "Use `get_interval_counts` for one-interval questions, rankings, comparisons inside one "
            "period, or phrases like 'за этот интервал'.\n"
            "If the user is asking about the whole store and does not explicitly focus on a single "
            "object or zone, do not pass `object_id` to tools.\n"
            "If there is no selected object and no object name in the question, default to store-wide analytics.\n"
            "If the user names an object but you only have text, call `list_store_objects` first to "
            "find the correct id.\n"
            "If a tool returns `error.retry_hint`, follow that hint and retry the tool automatically "
            "instead of asking the user, unless the missing information truly cannot be inferred.\n"
            f"{selected_object_line}\n"
            f"{default_window_line}\n"
            f"Default timezone: {payload.timezone}. Current UTC time: {now_utc}.\n"
            "Interpret relative periods as follows unless the user says otherwise:\n"
            "- 'сегодня' and 'вчера' are local calendar days in the request timezone.\n"
            "- 'за последний N дней/недель/месяц' is a rolling interval ending now.\n"
            "- 'в прошлом месяце' is the previous calendar month.\n"
            "Important metric caveat: `points_inside` and `points_around` are zone interaction counts, "
            "not guaranteed unique visitors or customers. If the user asks about clients or visitors, "
            "answer using a proxy metric and say so explicitly instead of asking for confirmation by "
            "default. For broad traffic questions about the whole store, prefer `points_combined` as "
            "the default proxy when it is available from the tools.\n"
            "If the necessary period is missing and cannot be inferred from the current request, say "
            "what is missing instead of guessing.\n"
            "Always answer in Russian, concise and factual."
        )

    @staticmethod
    def _extract_function_calls(response: Any) -> list[dict[str, str]]:
        calls: list[dict[str, str]] = []
        for item in ObjectChatService._iter_output_items(response):
            if ObjectChatService._read_field(item, "type") != "function_call":
                continue
            calls.append(
                {
                    "name": str(ObjectChatService._read_field(item, "name")),
                    "call_id": str(ObjectChatService._read_field(item, "call_id")),
                    "arguments": str(ObjectChatService._read_field(item, "arguments") or "{}"),
                }
            )
        return calls

    @staticmethod
    def _extract_output_text(response: Any) -> str:
        direct_text = ObjectChatService._read_field(response, "output_text")
        if isinstance(direct_text, str) and direct_text.strip():
            return direct_text.strip()

        chunks: list[str] = []
        for item in ObjectChatService._iter_output_items(response):
            if ObjectChatService._read_field(item, "type") != "message":
                continue
            content_items = ObjectChatService._read_field(item, "content") or []
            for content in content_items:
                if ObjectChatService._read_field(content, "type") == "output_text":
                    text = ObjectChatService._read_field(content, "text")
                    if isinstance(text, str) and text:
                        chunks.append(text)
        return "\n".join(chunks).strip()

    @staticmethod
    def _iter_output_items(response: Any) -> list[Any]:
        output = ObjectChatService._read_field(response, "output")
        if isinstance(output, list):
            return output
        return []

    @staticmethod
    def _read_field(value: Any, field_name: str) -> Any:
        if isinstance(value, dict):
            return value.get(field_name)
        return getattr(value, field_name, None)

    @staticmethod
    def _auto_retry_tool_if_possible(
        *,
        function_name: str,
        arguments: str,
        output: dict[str, Any],
        toolkit: AnalyticsToolkit,
    ) -> dict[str, Any]:
        error = output.get("error") if isinstance(output, dict) else None
        if not isinstance(error, dict):
            return output

        retry_hint = error.get("retry_hint")
        if retry_hint != "retry_without_object_id":
            return output

        try:
            patched_arguments = json.loads(arguments or "{}")
        except json.JSONDecodeError:
            return output

        if not isinstance(patched_arguments, dict):
            return output
        patched_arguments.pop("object_id", None)
        return toolkit.execute(function_name, patched_arguments)
