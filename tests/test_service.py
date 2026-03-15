from __future__ import annotations

import json
from types import SimpleNamespace

from app.analytics_toolkit import AnalyticsToolkit
from app.config import Settings
from app.schemas import ObjectChatRequest, TrackerCounts, TrackerObject
from app.service import OFF_TOPIC_ANSWER, ObjectChatService

DAILY_COUNTS = {
    "2026-03-01": {
        3: (10, 5),
        8: (12, 1),
    },
    "2026-03-02": {
        3: (27, 70),
        8: (40, 0),
    },
    "2026-03-03": {
        3: (5, 2),
        8: (8, 1),
    },
}


class FakeTrackerClient:
    def list_objects(self, store_id: int) -> list[TrackerObject]:
        return [
            TrackerObject(
                id=3,
                store_id=store_id,
                name="Object 3 (Center Setup)",
                polygon_points=[(1.0, 1.0), (2.0, 2.0), (3.0, 1.0)],
                buffer_polygon_points=[(0.0, 0.0), (4.0, 0.0), (4.0, 3.0)],
                buffer_radius=30,
            ),
            TrackerObject(
                id=8,
                store_id=store_id,
                name="касса",
                polygon_points=[(10.0, 10.0), (12.0, 10.0), (12.0, 12.0)],
                buffer_polygon_points=None,
                buffer_radius=30,
            ),
        ]

    def get_store_counts(self, store_id, start_time, end_time) -> list[TrackerCounts]:
        values = DAILY_COUNTS.get(start_time.date().isoformat(), {})
        return [
            TrackerCounts(
                id=3,
                name="Object 3 (Center Setup)",
                store_id=store_id,
                points_inside=values.get(3, (0, 0))[0],
                points_around=values.get(3, (0, 0))[1],
            ),
            TrackerCounts(
                id=8,
                name="касса",
                store_id=store_id,
                points_inside=values.get(8, (0, 0))[0],
                points_around=values.get(8, (0, 0))[1],
            ),
        ]


class FakeResponsesAPI:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        if len(self.calls) == 1:
            return SimpleNamespace(
                id="resp_step_1",
                model=kwargs["model"],
                output_text="",
                output=[
                    SimpleNamespace(
                        type="function_call",
                        name="get_daily_counts",
                        call_id="call_daily_1",
                        arguments=json.dumps(
                            {
                                "object_id": 8,
                                "start_date": "2026-03-01",
                                "end_date": "2026-03-03",
                            }
                        ),
                    )
                ],
            )

        return SimpleNamespace(
            id="resp_final_1",
            model=kwargs["model"],
            output_text=(
                "Если считать по proxy-метрике zone interaction counts, пик по кассе был "
                "2026-03-02: 40 inside и 0 around."
            ),
            output=[],
        )


class FakeOpenAIClient:
    def __init__(self) -> None:
        self.responses = FakeResponsesAPI()


class RetryingResponsesAPI(FakeResponsesAPI):
    def create(self, **kwargs):
        self.calls.append(kwargs)
        if len(self.calls) == 1:
            return SimpleNamespace(
                id="resp_retry_step_1",
                model=kwargs["model"],
                output_text="",
                output=[
                    SimpleNamespace(
                        type="function_call",
                        name="get_daily_counts",
                        call_id="call_daily_retry_1",
                        arguments=json.dumps(
                            {
                                "object_id": 3,
                                "start_date": "2026-03-01",
                                "end_date": "2026-03-03",
                            }
                        ),
                    )
                ],
            )

        return SimpleNamespace(
            id="resp_retry_final_1",
            model=kwargs["model"],
            output_text="Пик по магазину был 2026-03-02.",
            output=[],
        )


class RetryingOpenAIClient:
    def __init__(self) -> None:
        self.responses = RetryingResponsesAPI()


class DirectResponsesAPI:
    def __init__(self, output_text: str) -> None:
        self.calls: list[dict] = []
        self.output_text = output_text

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(
            id="resp_direct_1",
            model=kwargs["model"],
            output_text=self.output_text,
            output=[],
        )


class DirectOpenAIClient:
    def __init__(self, output_text: str) -> None:
        self.responses = DirectResponsesAPI(output_text)


def make_settings() -> Settings:
    return Settings(
        openai_api_key="test-key",
        openai_model="gpt-5-mini",
        openai_max_output_tokens=300,
        openai_reasoning_effort="low",
        openai_max_tool_rounds=4,
        request_timeout_seconds=10,
        tracker_api_base_url="http://unused",
    )


def make_service() -> tuple[ObjectChatService, FakeOpenAIClient]:
    openai_client = FakeOpenAIClient()
    service = ObjectChatService(
        settings=make_settings(),
        tracker_client=FakeTrackerClient(),
        openai_client=openai_client,
    )
    return service, openai_client


def make_retrying_service() -> tuple[ObjectChatService, RetryingOpenAIClient]:
    openai_client = RetryingOpenAIClient()
    service = ObjectChatService(
        settings=make_settings(),
        tracker_client=FakeTrackerClient(),
        openai_client=openai_client,
    )
    return service, openai_client


def test_daily_counts_aggregates_store_and_finds_peak_day():
    request = ObjectChatRequest(
        store_id=5,
        question="В какой день был пик?",
        timezone="UTC",
    )
    toolkit = AnalyticsToolkit(request=request, tracker_client=FakeTrackerClient())

    result = toolkit.execute(
        "get_daily_counts",
        {"start_date": "2026-03-01", "end_date": "2026-03-03"},
    )

    assert result["best_days"]["by_inside"]["date"] == "2026-03-02"
    assert result["best_days"]["by_inside"]["points_inside"] == 67
    assert result["days"][1]["points_combined"] == 137


def test_store_wide_question_rejects_accidental_object_scope():
    request = ObjectChatRequest(
        store_id=5,
        question="В какой день за последний месяц было больше всего клиентов?",
        timezone="UTC",
    )
    toolkit = AnalyticsToolkit(request=request, tracker_client=FakeTrackerClient())

    result = toolkit.execute(
        "get_daily_counts",
        {"object_id": 3, "start_date": "2026-03-01", "end_date": "2026-03-03"},
    )

    assert "error" in result
    assert "does not identify a specific object" in result["error"]["message"]
    assert result["error"]["retry_hint"] == "retry_without_object_id"


def test_answer_question_runs_tool_loop_and_returns_context():
    service, openai_client = make_service()
    request = ObjectChatRequest(
        store_id=5,
        object_id=8,
        question="В какой день за последние 3 дня было больше всего клиентов у этого объекта?",
        timezone="UTC",
        previous_response_id="resp_prev_user_1",
    )

    response = service.answer_question(request)

    assert response.answer.startswith("Если считать по proxy-метрике")
    assert response.model == "gpt-5-mini"
    assert response.response_id == "resp_final_1"
    assert response.context.store_id == 5
    assert response.context.object_id == 8
    assert response.context.object_name == "касса"
    assert response.context.tools_used == ["get_daily_counts"]

    first_call = openai_client.responses.calls[0]
    assert first_call["previous_response_id"] == "resp_prev_user_1"
    assert first_call["store"] is True
    assert first_call["reasoning"] == {"effort": "low"}
    assert any(tool["name"] == "get_daily_counts" for tool in first_call["tools"])

    second_call = openai_client.responses.calls[1]
    assert second_call["previous_response_id"] == "resp_step_1"
    tool_output = json.loads(second_call["input"][0]["output"])
    assert tool_output["best_days"]["by_inside"]["date"] == "2026-03-02"
    assert tool_output["object_name"] == "касса"


def test_service_auto_retries_without_object_id_for_store_scope():
    service, openai_client = make_retrying_service()
    request = ObjectChatRequest(
        store_id=5,
        question="В какой день за последний месяц было больше всего клиентов?",
        timezone="UTC",
    )

    response = service.answer_question(request)

    assert response.answer == "Пик по магазину был 2026-03-02."
    second_call = openai_client.responses.calls[1]
    tool_output = json.loads(second_call["input"][0]["output"])
    assert tool_output["scope"] == "store"
    assert tool_output["best_days"]["by_inside"]["date"] == "2026-03-02"


def test_offtopic_question_returns_guardrail_without_openai_call():
    service, openai_client = make_service()
    request = ObjectChatRequest(
        store_id=5,
        question="Дай рецепт пельменей",
        timezone="UTC",
    )

    response = service.answer_question(request)

    assert response.answer == OFF_TOPIC_ANSWER
    assert response.model == "guardrail"
    assert response.response_id is None
    assert response.context.tools_used == []
    assert openai_client.responses.calls == []


def test_output_is_normalized_for_readability():
    openai_client = DirectOpenAIClient("Пик был 14 марта.  \n\n\nМетрика proxy: points_* считаем как proxy.   ")
    service = ObjectChatService(
        settings=make_settings(),
        tracker_client=FakeTrackerClient(),
        openai_client=openai_client,
    )
    request = ObjectChatRequest(
        store_id=5,
        question="Какой день был самым активным?",
        timezone="UTC",
    )

    response = service.answer_question(request)

    assert response.answer == "Пик был 14 марта.\n\nМетрика proxy: points_* считаем как proxy."
    assert len(openai_client.responses.calls) == 1


def test_instructions_require_short_answers_and_offtopic_refusal():
    request = ObjectChatRequest(
        store_id=5,
        question="Что происходит по магазину?",
        timezone="UTC",
    )

    instructions = ObjectChatService._build_instructions(request)

    assert "Start with one direct sentence" in instructions
    assert OFF_TOPIC_ANSWER in instructions
    assert "Keep the answer compact and easy to scan" in instructions
