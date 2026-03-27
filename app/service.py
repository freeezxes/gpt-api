from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

from openai import OpenAI

from app.analytics_toolkit import AnalyticsToolkit
from app.config import Settings
from app.question_scope import question_mentions_demographics, question_mentions_entry_traffic
from app.schemas import ObjectChatRequest, ObjectChatResponse
from app.store_analytics_client import StoreAnalyticsClient
from app.tracker_client import TrackerClient

OFF_TOPIC_ANSWER = (
    "Я отвечаю только по магазину, зонам, объектам и их метрикам. "
    "Спроси про период, объект, зону, трафик, очередь или сравнение."
)

STRONG_DOMAIN_KEYWORDS = (
    "объект",
    "зона",
    "касс",
    "очеред",
    "трафик",
    "посет",
    "клиент",
    "примероч",
    "полк",
    "вешал",
    "rack",
    "checkout",
    "queue",
    "traffic",
    "inside",
    "around",
    "зал",
    "вход",
    "выход",
    "камера",
    "сесс",
    "демограф",
    "возраст",
    "мужч",
    "женщ",
    "конверси",
    "выручк",
    "чек",
    "продаж",
    "примерк",
    "heatmap",
    "уведомлен",
    "alert",
    "ритейл",
    "операцион",
    "эффектив",
    "улучш",
    "оптим",
    "слаб",
    "риск",
    "проблем",
    "товар",
    "sku",
    "ассортимент",
    "витрин",
    "выкладк",
    "мерч",
    "обслужив",
    "сервис",
    "персонал",
)
WEAK_DOMAIN_KEYWORDS = ("магазин", "store", "shop", "бутик", "точка", "филиал", "локация")
OFF_TOPIC_KEYWORDS = (
    "рецепт",
    "пельмен",
    "борщ",
    "суп",
    "котлет",
    "паста",
    "пирог",
    "кулинар",
    "еда",
    "погода",
    "доллар",
    "евро",
    "курс валют",
    "анекдот",
    "шутк",
    "стих",
    "песня",
    "поздрав",
    "переведи",
    "перевод",
    "реферат",
    "сочинени",
    "домашк",
    "python",
    "javascript",
    "java",
    "typescript",
    "sql",
    "regex",
    "регекс",
    "алгоритм",
    "leetcode",
    "фильм",
    "сериал",
    "книга",
    "гороскоп",
    "новост",
    "полит",
    "президент",
    "любов",
    "отношени",
    "как дела",
    "кто такой",
    "что такое",
)
SELECTED_OBJECT_REFERENCE_PHRASES = (
    "этот объект",
    "эта зона",
    "эта касса",
    "по этому объекту",
    "по этой зоне",
    "по нему",
    "по ней",
    "у него",
    "у нее",
    "здесь",
    "тут",
)
OBJECT_NAME_STOPWORDS = {"object", "left", "right", "center", "setup"}


class ConfigurationError(RuntimeError):
    """Raised when required local configuration is missing."""


class ObjectNotFoundError(RuntimeError):
    """Retained for compatibility with HTTP layer mappings."""


class ObjectChatService:
    def __init__(
        self,
        settings: Settings,
        tracker_client: TrackerClient | None = None,
        store_analytics_client: StoreAnalyticsClient | None = None,
        openai_client: OpenAI | None = None,
    ) -> None:
        self.settings = settings
        self.tracker_client = tracker_client or TrackerClient(
            base_url=settings.tracker_api_base_url,
            timeout_seconds=settings.request_timeout_seconds,
        )
        self.store_analytics_client = store_analytics_client or StoreAnalyticsClient(
            database_url=settings.analytics_database_url,
            timeout_seconds=settings.request_timeout_seconds,
        )
        self.openai_client = openai_client

    def answer_question(self, payload: ObjectChatRequest) -> ObjectChatResponse:
        selected_model = payload.model or self.settings.openai_model
        toolkit = AnalyticsToolkit(
            request=payload,
            tracker_client=self.tracker_client,
            store_analytics_client=self.store_analytics_client,
        )

        guardrail_answer = self._guardrail_answer(payload)
        if guardrail_answer is not None:
            return ObjectChatResponse(
                answer=guardrail_answer,
                model="guardrail",
                response_id=None,
                context=toolkit.build_context(),
            )

        if not self.settings.openai_api_key:
            raise ConfigurationError("OPENAI_API_KEY is not set")
        if self.openai_client is None:
            self.openai_client = OpenAI(api_key=self.settings.openai_api_key)

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
            answer=self._normalize_answer(answer),
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
            "inferred_metric_family": ObjectChatService._infer_metric_family(payload.question),
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
            "You are an analytics agent for Myrza Tracker.\n"
            "Your job is to decide which backend tools are needed, call them, and then answer.\n"
            "Treat any question that is even loosely related to the store, retail operations, "
            "sales, traffic, conversion, assortment, service, staffing, merchandising, queues, "
            "zones, objects, customer behavior, or performance as in-scope.\n"
            "Only refuse clearly unrelated topics like recipes, weather, coding help, politics, "
            "or general chit-chat.\n"
            "Never invent analytics when a tool can verify them.\n"
            "Use the minimum number of tools needed for a reliable answer.\n"
            "Use `get_daily_counts` for questions about daily dynamics, trends, maxima by day, "
            "or phrases like 'за последний месяц', 'по дням', 'в какой день'.\n"
            "Use `get_interval_counts` for one-interval questions, rankings, comparisons inside one "
            "period, or phrases like 'за этот интервал'.\n"
            "Use `get_daily_entry_traffic` for questions specifically about store entries/exits by day, "
            "daily entry comparisons, door traffic, or phrases like 'входы', 'выходы', 'вошло', 'зашло'.\n"
            "Use `get_entry_interval_traffic` for interval questions specifically about entries/exits or "
            "door traffic for one period.\n"
            "Use `get_daily_demographics` for questions about men vs women, gender split by day, "
            "age dynamics, or phrases like 'кто больше приходит мужчины или женщины за неделю'.\n"
            "Use `get_demographics_interval` for one-period questions about gender split, age split, "
            "or store demographics in one interval.\n"
            "If the user is asking about the whole store and does not explicitly focus on a single "
            "object or zone, do not pass `object_id` to tools.\n"
            "If the question is about entries/exits, always prefer the entry traffic tools and do not "
            "answer with `points_inside`, `points_around`, or `points_combined`. Entry/exit traffic is "
            "store-level, not object-level.\n"
            "If the question is about gender, demographics, men, women, or age, always prefer the "
            "demographics tools and do not answer with `points_inside`, `points_around`, or "
            "`points_combined`. Demographics are store-level, not object-level.\n"
            "If there is no selected object and no object name in the question, default to store-wide analytics.\n"
            "If the user names an object but you only have text, call `list_store_objects` first to "
            "find the correct id.\n"
            "If a tool returns `error.retry_hint`, follow that hint and retry the tool automatically "
            "instead of asking the user, unless the missing information truly cannot be inferred.\n"
            "If the question is outside store analytics, retail operations, objects, zones, queues, "
            f"or metrics, do not call tools and answer exactly: {OFF_TOPIC_ANSWER}\n"
            "If the question is related to the store or retail operations but the exact metric is not "
            "available in tools, still answer usefully in Russian. Be explicit about what is known, "
            "what is not directly available, and what available data would be the closest proxy.\n"
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
            "Important demographic caveat: demographic tools return analytics aggregates from "
            "`person_traffic_aggregate`. Treat them as store-level demographic counts or session proxies, "
            "not guaranteed unique shoppers.\n"
            "If the necessary period is missing and cannot be inferred from the current request, say "
            "what is missing instead of guessing.\n"
            "Final answer rules:\n"
            "- Always answer in Russian.\n"
            "- Start with one direct sentence that answers the question.\n"
            "- If details are useful, add at most 3 short bullet points.\n"
            "- Do not restate the question. Do not dump raw data. Do not explain your internal reasoning.\n"
            "- If one date, zone, rank, or number already answers the question, stop after 1-2 short sentences.\n"
            "- If a metric caveat is needed, keep it to one short final sentence starting with 'Метрика proxy:'.\n"
            "- Keep the answer compact and easy to scan."
        )

    @staticmethod
    def _infer_metric_family(question: str) -> str:
        if question_mentions_entry_traffic(question):
            return "entry_traffic"
        if question_mentions_demographics(question):
            return "demographics"
        return "object_activity"

    def _guardrail_answer(self, payload: ObjectChatRequest) -> str | None:
        if self._is_question_in_scope(payload):
            return None
        return OFF_TOPIC_ANSWER

    def _is_question_in_scope(self, payload: ObjectChatRequest) -> bool:
        question = self._normalize_text(payload.question)
        if not question:
            return False

        strong_hits = self._keyword_hits(question, STRONG_DOMAIN_KEYWORDS)
        weak_hits = self._keyword_hits(question, WEAK_DOMAIN_KEYWORDS)
        off_topic_hits = self._keyword_hits(question, OFF_TOPIC_KEYWORDS)

        if payload.object_id is not None and any(
            phrase in question for phrase in SELECTED_OBJECT_REFERENCE_PHRASES
        ):
            strong_hits += 1

        if off_topic_hits == 0:
            return True

        if strong_hits == 0:
            strong_hits += self._object_name_signal(payload.store_id, question)

        if strong_hits > 0:
            return strong_hits > off_topic_hits

        return weak_hits > off_topic_hits

    def _object_name_signal(self, store_id: int, normalized_question: str) -> int:
        try:
            objects = self.tracker_client.list_objects(store_id)
        except Exception:
            return 0

        signal = 0
        for obj in objects:
            normalized_name = self._normalize_text(obj.name)
            if normalized_name and normalized_name in normalized_question:
                return 2
            for token in re.findall(r"[a-zа-я0-9]+", normalized_name):
                if len(token) < 4 or token in OBJECT_NAME_STOPWORDS:
                    continue
                if token in normalized_question:
                    signal = 1
        return signal

    @staticmethod
    def _normalize_text(value: str) -> str:
        return re.sub(r"\s+", " ", value.lower().replace("ё", "е")).strip()

    @staticmethod
    def _keyword_hits(text: str, keywords: tuple[str, ...]) -> int:
        return sum(1 for keyword in keywords if keyword in text)

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
    def _normalize_answer(answer: str) -> str:
        normalized_lines: list[str] = []
        previous_blank = False

        for raw_line in answer.replace("\r\n", "\n").split("\n"):
            stripped = raw_line.strip()
            if not stripped:
                if previous_blank:
                    continue
                normalized_lines.append("")
                previous_blank = True
                continue

            normalized_lines.append(re.sub(r"\s{2,}", " ", stripped))
            previous_blank = False

        while normalized_lines and normalized_lines[0] == "":
            normalized_lines.pop(0)
        while normalized_lines and normalized_lines[-1] == "":
            normalized_lines.pop()

        return "\n".join(normalized_lines)

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
