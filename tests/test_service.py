from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

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

ENTRY_DAILY = {
    "2026-03-13": (55, 49),
    "2026-03-14": (102, 91),
}

DEMOGRAPHICS_DAILY = {
    "2026-03-10": {
        "male_count": 12,
        "female_count": 9,
        "unknown_gender_count": 1,
        "total_detections": 24,
        "unique_sessions": 18,
    },
    "2026-03-11": {
        "male_count": 18,
        "female_count": 7,
        "unknown_gender_count": 0,
        "total_detections": 29,
        "unique_sessions": 23,
    },
    "2026-03-12": {
        "male_count": 10,
        "female_count": 15,
        "unknown_gender_count": 0,
        "total_detections": 28,
        "unique_sessions": 21,
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


class FakeStoreAnalyticsClient:
    configured = True

    def get_entry_traffic_interval(self, store_id: int, start_time: datetime, end_time: datetime):
        return {
            "scope": "store_entry_traffic",
            "store_id": store_id,
            "time_window": {
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
            },
            "metric_note": "Counts come from entry/exit door-counter events, not object interaction points.",
            "traffic": {
                "entries_in": 157,
                "exits_out": 140,
                "net_flow": 17,
            },
            "by_door_counter": [
                {
                    "door_counter_id": 4,
                    "door_counter_name": "Вход/Выход",
                    "entries_in": 157,
                    "exits_out": 140,
                    "net_flow": 17,
                }
            ],
        }

    def get_daily_entry_traffic(
        self,
        store_id: int,
        start_date: date,
        end_date: date,
        timezone_name: str,
    ):
        tz = ZoneInfo(timezone_name)
        days = []
        current_date = start_date
        while current_date <= end_date:
            entries_in, exits_out = ENTRY_DAILY.get(current_date.isoformat(), (0, 0))
            local_start = datetime.combine(current_date, time.min, tzinfo=tz)
            local_end = local_start + timedelta(days=1)
            days.append(
                {
                    "date": current_date.isoformat(),
                    "start_time": local_start.astimezone(timezone.utc).isoformat(),
                    "end_time": local_end.astimezone(timezone.utc).isoformat(),
                    "entries_in": entries_in,
                    "exits_out": exits_out,
                    "net_flow": entries_in - exits_out,
                    "by_door_counter": [
                        {
                            "door_counter_id": 4,
                            "door_counter_name": "Вход/Выход",
                            "entries_in": entries_in,
                            "exits_out": exits_out,
                            "net_flow": entries_in - exits_out,
                        }
                    ],
                }
            )
            current_date += timedelta(days=1)
        return {
            "scope": "store_entry_traffic",
            "store_id": store_id,
            "timezone": timezone_name,
            "date_window": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
            "metric_note": "Counts come from entry/exit door-counter events, not object interaction points.",
            "best_days": {
                "by_entries_in": max(days, key=lambda item: (item["entries_in"], item["date"])),
                "by_exits_out": max(days, key=lambda item: (item["exits_out"], item["date"])),
            },
            "days": days,
        }

    def get_demographics_interval(self, store_id: int, start_time: datetime, end_time: datetime):
        male_count = sum(item["male_count"] for item in DEMOGRAPHICS_DAILY.values())
        female_count = sum(item["female_count"] for item in DEMOGRAPHICS_DAILY.values())
        unknown_gender_count = sum(
            item["unknown_gender_count"] for item in DEMOGRAPHICS_DAILY.values()
        )
        total_detections = sum(item["total_detections"] for item in DEMOGRAPHICS_DAILY.values())
        unique_sessions = sum(item["unique_sessions"] for item in DEMOGRAPHICS_DAILY.values())
        known_total = male_count + female_count
        return {
            "scope": "store_demographics",
            "store_id": store_id,
            "time_window": {
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
            },
            "metric_note": (
                "Demographics come from person_traffic_aggregate store-level rows "
                "(zone_id is null). These are analytics aggregates, not guaranteed unique customers."
            ),
            "summary": {
                "total_detections": total_detections,
                "unique_sessions": unique_sessions,
                "gender": {
                    "male_count": male_count,
                    "female_count": female_count,
                    "unknown_gender_count": unknown_gender_count,
                    "known_total": known_total,
                    "male_share": round((male_count / known_total) * 100, 1),
                    "female_share": round((female_count / known_total) * 100, 1),
                },
                "top_gender": {
                    "label": "male",
                    "count": male_count,
                    "margin": male_count - female_count,
                },
                "age_breakdown": {
                    "age0_17": 1,
                    "age18_24": 7,
                    "age25_34": 19,
                    "age35_44": 12,
                    "age45_54": 5,
                    "age55_plus": 3,
                },
            },
        }

    def get_daily_demographics(
        self,
        store_id: int,
        start_date: date,
        end_date: date,
        timezone_name: str,
    ):
        tz = ZoneInfo(timezone_name)
        days = []
        current_date = start_date
        while current_date <= end_date:
            values = DEMOGRAPHICS_DAILY.get(
                current_date.isoformat(),
                {
                    "male_count": 0,
                    "female_count": 0,
                    "unknown_gender_count": 0,
                    "total_detections": 0,
                    "unique_sessions": 0,
                },
            )
            known_total = values["male_count"] + values["female_count"]
            local_start = datetime.combine(current_date, time.min, tzinfo=tz)
            local_end = local_start + timedelta(days=1)
            days.append(
                {
                    "date": current_date.isoformat(),
                    "start_time": local_start.astimezone(timezone.utc).isoformat(),
                    "end_time": local_end.astimezone(timezone.utc).isoformat(),
                    "total_detections": values["total_detections"],
                    "unique_sessions": values["unique_sessions"],
                    "gender": {
                        "male_count": values["male_count"],
                        "female_count": values["female_count"],
                        "unknown_gender_count": values["unknown_gender_count"],
                        "known_total": known_total,
                        "male_share": round((values["male_count"] / known_total) * 100, 1)
                        if known_total
                        else 0.0,
                        "female_share": round((values["female_count"] / known_total) * 100, 1)
                        if known_total
                        else 0.0,
                    },
                    "top_gender": (
                        {
                            "label": "male",
                            "count": values["male_count"],
                            "margin": values["male_count"] - values["female_count"],
                        }
                        if values["male_count"] >= values["female_count"] and known_total
                        else (
                            {
                                "label": "female",
                                "count": values["female_count"],
                                "margin": values["female_count"] - values["male_count"],
                            }
                            if known_total
                            else None
                        )
                    ),
                    "age_breakdown": {
                        "age0_17": 0,
                        "age18_24": 0,
                        "age25_34": 0,
                        "age35_44": 0,
                        "age45_54": 0,
                        "age55_plus": 0,
                    },
                }
            )
            current_date += timedelta(days=1)
        return {
            "scope": "store_demographics",
            "store_id": store_id,
            "timezone": timezone_name,
            "date_window": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
            "metric_note": (
                "Demographics come from person_traffic_aggregate store-level rows "
                "(zone_id is null). These are analytics aggregates, not guaranteed unique customers."
            ),
            "best_days": {
                "by_male_count": max(
                    days,
                    key=lambda item: (
                        item["gender"]["male_count"],
                        item["gender"]["female_count"],
                        item["date"],
                    ),
                ),
                "by_female_count": max(
                    days,
                    key=lambda item: (
                        item["gender"]["female_count"],
                        item["gender"]["male_count"],
                        item["date"],
                    ),
                ),
            },
            "days": days,
        }


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


class EntryResponsesAPI:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        if len(self.calls) == 1:
            return SimpleNamespace(
                id="resp_entry_step_1",
                model=kwargs["model"],
                output_text="",
                output=[
                    SimpleNamespace(
                        type="function_call",
                        name="get_daily_entry_traffic",
                        call_id="call_entry_daily_1",
                        arguments=json.dumps(
                            {
                                "start_date": "2026-03-13",
                                "end_date": "2026-03-14",
                                "timezone": "Asia/Almaty",
                            }
                        ),
                    )
                ],
            )

        return SimpleNamespace(
            id="resp_entry_final_1",
            model=kwargs["model"],
            output_text="Вчера было 102 входа против 55 позавчера. Рост: 85.5%.",
            output=[],
        )


class EntryOpenAIClient:
    def __init__(self) -> None:
        self.responses = EntryResponsesAPI()


class DemographicsResponsesAPI:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        if len(self.calls) == 1:
            return SimpleNamespace(
                id="resp_demo_step_1",
                model=kwargs["model"],
                output_text="",
                output=[
                    SimpleNamespace(
                        type="function_call",
                        name="get_daily_demographics",
                        call_id="call_demo_daily_1",
                        arguments=json.dumps(
                            {
                                "start_date": "2026-03-10",
                                "end_date": "2026-03-12",
                                "timezone": "Asia/Almaty",
                            }
                        ),
                    )
                ],
            )

        return SimpleNamespace(
            id="resp_demo_final_1",
            model=kwargs["model"],
            output_text="За неделю мужчин больше: 40 против 31 у женщин.",
            output=[],
        )


class DemographicsOpenAIClient:
    def __init__(self) -> None:
        self.responses = DemographicsResponsesAPI()


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


def make_entry_service() -> tuple[ObjectChatService, EntryOpenAIClient]:
    openai_client = EntryOpenAIClient()
    service = ObjectChatService(
        settings=make_settings(),
        tracker_client=FakeTrackerClient(),
        store_analytics_client=FakeStoreAnalyticsClient(),
        openai_client=openai_client,
    )
    return service, openai_client


def make_demographics_service() -> tuple[ObjectChatService, DemographicsOpenAIClient]:
    openai_client = DemographicsOpenAIClient()
    service = ObjectChatService(
        settings=make_settings(),
        tracker_client=FakeTrackerClient(),
        store_analytics_client=FakeStoreAnalyticsClient(),
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


def test_entry_question_rejects_object_counts_tool():
    request = ObjectChatRequest(
        store_id=5,
        question="Сравни вчера и позавчера по входам",
        timezone="Asia/Almaty",
    )
    toolkit = AnalyticsToolkit(
        request=request,
        tracker_client=FakeTrackerClient(),
        store_analytics_client=FakeStoreAnalyticsClient(),
    )

    result = toolkit.execute(
        "get_daily_counts",
        {"start_date": "2026-03-13", "end_date": "2026-03-14"},
    )

    assert "error" in result
    assert "entry/exit traffic" in result["error"]["message"]
    assert result["error"]["retry_hint"] == "use_entry_traffic_tools"


def test_demographics_question_rejects_object_counts_tool():
    request = ObjectChatRequest(
        store_id=5,
        question="Кто больше приходит в магазин, мужчины или женщины за неделю?",
        timezone="Asia/Almaty",
    )
    toolkit = AnalyticsToolkit(
        request=request,
        tracker_client=FakeTrackerClient(),
        store_analytics_client=FakeStoreAnalyticsClient(),
    )

    result = toolkit.execute(
        "get_daily_counts",
        {"start_date": "2026-03-10", "end_date": "2026-03-12"},
    )

    assert "error" in result
    assert "demographics" in result["error"]["message"]
    assert result["error"]["retry_hint"] == "use_demographics_tools"


def test_daily_entry_traffic_returns_real_entry_metric_shape():
    request = ObjectChatRequest(
        store_id=5,
        question="Сравни вчера и позавчера по входам",
        timezone="Asia/Almaty",
    )
    toolkit = AnalyticsToolkit(
        request=request,
        tracker_client=FakeTrackerClient(),
        store_analytics_client=FakeStoreAnalyticsClient(),
    )

    result = toolkit.execute(
        "get_daily_entry_traffic",
        {"start_date": "2026-03-13", "end_date": "2026-03-14", "timezone": "Asia/Almaty"},
    )

    assert result["best_days"]["by_entries_in"]["date"] == "2026-03-14"
    assert result["best_days"]["by_entries_in"]["entries_in"] == 102
    assert result["days"][0]["entries_in"] == 55
    assert result["days"][1]["exits_out"] == 91


def test_daily_demographics_returns_gender_breakdown():
    request = ObjectChatRequest(
        store_id=5,
        question="Кто больше приходит в магазин, мужчины или женщины за неделю?",
        timezone="Asia/Almaty",
    )
    toolkit = AnalyticsToolkit(
        request=request,
        tracker_client=FakeTrackerClient(),
        store_analytics_client=FakeStoreAnalyticsClient(),
    )

    result = toolkit.execute(
        "get_daily_demographics",
        {"start_date": "2026-03-10", "end_date": "2026-03-12", "timezone": "Asia/Almaty"},
    )

    assert result["best_days"]["by_male_count"]["date"] == "2026-03-11"
    assert result["best_days"]["by_female_count"]["date"] == "2026-03-12"
    assert result["days"][0]["gender"]["male_count"] == 12
    assert result["days"][2]["gender"]["female_count"] == 15


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


def test_entry_question_uses_entry_tool_loop():
    service, openai_client = make_entry_service()
    request = ObjectChatRequest(
        store_id=5,
        question="Сравни вчера и позавчера по входам",
        timezone="Asia/Almaty",
    )

    response = service.answer_question(request)

    assert response.answer == "Вчера было 102 входа против 55 позавчера. Рост: 85.5%."
    assert response.context.tools_used == ["get_daily_entry_traffic"]
    assert response.model == "gpt-5-mini"
    second_call = openai_client.responses.calls[1]
    tool_output = json.loads(second_call["input"][0]["output"])
    assert tool_output["best_days"]["by_entries_in"]["entries_in"] == 102


def test_demographics_question_uses_demographics_tool_loop():
    service, openai_client = make_demographics_service()
    request = ObjectChatRequest(
        store_id=5,
        question="Кто больше приходит в магазин, мужчины или женщины за неделю?",
        timezone="Asia/Almaty",
    )

    response = service.answer_question(request)

    assert response.answer == "За неделю мужчин больше: 40 против 31 у женщин."
    assert response.context.tools_used == ["get_daily_demographics"]
    second_call = openai_client.responses.calls[1]
    tool_output = json.loads(second_call["input"][0]["output"])
    assert tool_output["best_days"]["by_male_count"]["gender"]["male_count"] == 18


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


def test_instructions_require_short_answers_and_entry_metric_rules():
    request = ObjectChatRequest(
        store_id=5,
        question="Что по входам за вчера?",
        timezone="UTC",
    )

    instructions = ObjectChatService._build_instructions(request)
    user_input = ObjectChatService._build_user_input(request)

    assert "get_daily_entry_traffic" in instructions
    assert "do not answer with `points_inside`, `points_around`, or `points_combined`" in instructions
    assert "Start with one direct sentence" in instructions
    assert OFF_TOPIC_ANSWER in instructions
    assert '"inferred_metric_family": "entry_traffic"' in user_input


def test_instructions_require_demographics_tools_for_gender_questions():
    request = ObjectChatRequest(
        store_id=5,
        question="Кто больше приходит, мужчины или женщины?",
        timezone="UTC",
    )

    instructions = ObjectChatService._build_instructions(request)
    user_input = ObjectChatService._build_user_input(request)

    assert "get_daily_demographics" in instructions
    assert "always prefer the demographics tools" in instructions
    assert '"inferred_metric_family": "demographics"' in user_input
