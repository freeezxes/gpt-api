from __future__ import annotations

import json
import re
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from app.question_scope import question_mentions_demographics, question_mentions_entry_traffic
from app.schemas import ObjectChatContext, ObjectChatRequest, TrackerCounts, TrackerObject
from app.store_analytics_client import StoreAnalyticsClient
from app.tracker_client import TrackerClient

MAX_DAILY_RANGE_DAYS = 120


class ToolExecutionError(RuntimeError):
    """Raised when a tool call is invalid or missing required parameters."""


class AnalyticsToolkit:
    def __init__(
        self,
        request: ObjectChatRequest,
        tracker_client: TrackerClient,
        store_analytics_client: StoreAnalyticsClient | None = None,
    ) -> None:
        self.request = request
        self.tracker_client = tracker_client
        self.store_analytics_client = store_analytics_client
        self.tools_used: list[str] = []
        self.resolved_object_id: int | None = request.object_id
        self.object_scope_used = False
        self.object_name: str | None = None
        self.store_object_count: int | None = None
        self.points_inside: int | None = None
        self.points_around: int | None = None
        self.rank_by_inside: int | None = None
        self.rank_by_around: int | None = None
        self.last_timezone: str = request.timezone
        self.last_start_time: datetime | None = request.start_time
        self.last_end_time: datetime | None = request.end_time
        self._objects_cache: dict[int, list[TrackerObject]] = {}
        self._store_counts_cache: dict[tuple[int, str, str], list[TrackerCounts]] = {}

    @staticmethod
    def tool_definitions() -> list[dict[str, Any]]:
        return [
            {
                "type": "function",
                "name": "list_store_objects",
                "description": (
                    "List store objects and their ids. Use this when the user names a zone "
                    "in text and you need to identify or disambiguate it. If store_id is omitted, "
                    "use the current request store_id."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "store_id": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "Store id. Omit to use the current request store_id.",
                        }
                    },
                    "additionalProperties": False,
                },
            },
            {
                "type": "function",
                "name": "get_interval_counts",
                "description": (
                    "Get analytics for a single interval for the whole store or one object. "
                    "Use this for questions about one period, interval comparisons, rankings, "
                    "or phrases like 'for this interval'. Metrics are zone interaction counts "
                    "(points_inside and points_around), not unique visitors."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "store_id": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "Store id. Omit to use the current request store_id.",
                        },
                        "object_id": {
                            "type": "integer",
                            "minimum": 1,
                            "description": (
                                "Object id. Provide it only when the user is explicitly asking "
                                "about one object or zone. Omit it for store-wide questions."
                            ),
                        },
                        "start_time": {
                            "type": "string",
                            "description": (
                                "ISO 8601 datetime with timezone. Omit only if the current request "
                                "already includes a default interval."
                            ),
                        },
                        "end_time": {
                            "type": "string",
                            "description": (
                                "ISO 8601 datetime with timezone. Omit only if the current request "
                                "already includes a default interval."
                            ),
                        },
                    },
                    "additionalProperties": False,
                },
            },
            {
                "type": "function",
                "name": "get_daily_counts",
                "description": (
                    "Get daily analytics across a date range for the whole store or one object. "
                    "Use this for questions like 'which day', 'daily', 'trend', 'dynamics', "
                    "'last month', or 'per day'. Day buckets are local to the provided timezone "
                    "or the current request timezone. Metrics are zone interaction counts, not "
                    "unique visitors."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "store_id": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "Store id. Omit to use the current request store_id.",
                        },
                        "object_id": {
                            "type": "integer",
                            "minimum": 1,
                            "description": (
                                "Object id. Provide it only when the user is explicitly asking "
                                "about one object or zone. Omit it for store-wide daily analytics."
                            ),
                        },
                        "start_date": {
                            "type": "string",
                            "description": (
                                "Inclusive local start date in YYYY-MM-DD. Omit only if the current "
                                "request interval should be reused as the date window."
                            ),
                        },
                        "end_date": {
                            "type": "string",
                            "description": (
                                "Inclusive local end date in YYYY-MM-DD. Omit only if the current "
                                "request interval should be reused as the date window."
                            ),
                        },
                        "timezone": {
                            "type": "string",
                            "description": "IANA timezone name. Omit to use the current request timezone.",
                        },
                    },
                    "additionalProperties": False,
                },
            },
            {
                "type": "function",
                "name": "get_entry_interval_traffic",
                "description": (
                    "Get store entry/exit traffic for a single interval from door-counter events. "
                    "Use this for questions specifically about входы, выходы, вошло, вышло, "
                    "entry traffic, exit traffic, or door traffic for one period."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "store_id": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "Store id. Omit to use the current request store_id.",
                        },
                        "start_time": {
                            "type": "string",
                            "description": (
                                "ISO 8601 datetime with timezone. Omit only if the current request "
                                "already includes a default interval."
                            ),
                        },
                        "end_time": {
                            "type": "string",
                            "description": (
                                "ISO 8601 datetime with timezone. Omit only if the current request "
                                "already includes a default interval."
                            ),
                        },
                    },
                    "additionalProperties": False,
                },
            },
            {
                "type": "function",
                "name": "get_daily_entry_traffic",
                "description": (
                    "Get daily store entry/exit traffic from door-counter events. "
                    "Use this for questions like 'сравни вчера и позавчера по входам', "
                    "'в какой день было больше всего входов', or daily entry/exit trends."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "store_id": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "Store id. Omit to use the current request store_id.",
                        },
                        "start_date": {
                            "type": "string",
                            "description": (
                                "Inclusive local start date in YYYY-MM-DD. Omit only if the current "
                                "request interval should be reused as the date window."
                            ),
                        },
                        "end_date": {
                            "type": "string",
                            "description": (
                                "Inclusive local end date in YYYY-MM-DD. Omit only if the current "
                                "request interval should be reused as the date window."
                            ),
                        },
                        "timezone": {
                            "type": "string",
                            "description": "IANA timezone name. Omit to use the current request timezone.",
                        },
                    },
                    "additionalProperties": False,
                },
            },
            {
                "type": "function",
                "name": "get_demographics_interval",
                "description": (
                    "Get store-level demographics for one interval from person_traffic_aggregate. "
                    "Use this for questions about men vs women, gender split, age split, "
                    "or store demographics for one period such as a week."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "store_id": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "Store id. Omit to use the current request store_id.",
                        },
                        "start_time": {
                            "type": "string",
                            "description": (
                                "ISO 8601 datetime with timezone. Omit only if the current request "
                                "already includes a default interval."
                            ),
                        },
                        "end_time": {
                            "type": "string",
                            "description": (
                                "ISO 8601 datetime with timezone. Omit only if the current request "
                                "already includes a default interval."
                            ),
                        },
                    },
                    "additionalProperties": False,
                },
            },
            {
                "type": "function",
                "name": "get_daily_demographics",
                "description": (
                    "Get daily store-level demographics from person_traffic_aggregate. "
                    "Use this for questions like who dominated by gender over the last week, "
                    "daily gender trend, or age dynamics by day."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "store_id": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "Store id. Omit to use the current request store_id.",
                        },
                        "start_date": {
                            "type": "string",
                            "description": (
                                "Inclusive local start date in YYYY-MM-DD. Omit only if the current "
                                "request interval should be reused as the date window."
                            ),
                        },
                        "end_date": {
                            "type": "string",
                            "description": (
                                "Inclusive local end date in YYYY-MM-DD. Omit only if the current "
                                "request interval should be reused as the date window."
                            ),
                        },
                        "timezone": {
                            "type": "string",
                            "description": "IANA timezone name. Omit to use the current request timezone.",
                        },
                    },
                    "additionalProperties": False,
                },
            },
        ]

    def execute(self, name: str, arguments: str | dict[str, Any] | None) -> dict[str, Any]:
        try:
            parsed_args = self._parse_arguments(arguments)
            handlers = {
                "list_store_objects": self._list_store_objects,
                "get_interval_counts": self._get_interval_counts,
                "get_daily_counts": self._get_daily_counts,
                "get_entry_interval_traffic": self._get_entry_interval_traffic,
                "get_daily_entry_traffic": self._get_daily_entry_traffic,
                "get_demographics_interval": self._get_demographics_interval,
                "get_daily_demographics": self._get_daily_demographics,
            }
            handler = handlers.get(name)
            if handler is None:
                raise ToolExecutionError(f"Unknown tool: {name}")
            if name not in self.tools_used:
                self.tools_used.append(name)
            return handler(parsed_args)
        except ToolExecutionError as exc:
            return {
                "error": {
                    "message": str(exc),
                    "retry_hint": self._retry_hint_for_message(str(exc)),
                }
            }
        except Exception as exc:
            return {"error": {"message": str(exc), "retry_hint": None}}

    def build_context(self) -> ObjectChatContext:
        object_id = self.resolved_object_id if self.object_scope_used else self.request.object_id
        object_name = self.object_name if object_id is not None else None
        points_inside = self.points_inside if self.object_scope_used else None
        points_around = self.points_around if self.object_scope_used else None
        rank_by_inside = self.rank_by_inside if self.object_scope_used else None
        rank_by_around = self.rank_by_around if self.object_scope_used else None
        return ObjectChatContext(
            store_id=self.request.store_id,
            object_id=object_id,
            object_name=object_name,
            store_object_count=self.store_object_count,
            timezone=self.last_timezone,
            start_time=self.last_start_time,
            end_time=self.last_end_time,
            points_inside=points_inside,
            points_around=points_around,
            rank_by_inside=rank_by_inside,
            rank_by_around=rank_by_around,
            tools_used=self.tools_used,
        )

    def _get_entry_interval_traffic(self, args: dict[str, Any]) -> dict[str, Any]:
        client = self._require_store_analytics_client()
        store_id = self._resolve_store_id(args)
        start_time, end_time = self._resolve_time_window(args)
        self.last_start_time = start_time
        self.last_end_time = end_time
        return client.get_entry_traffic_interval(
            store_id=store_id,
            start_time=start_time,
            end_time=end_time,
        )

    def _get_daily_entry_traffic(self, args: dict[str, Any]) -> dict[str, Any]:
        client = self._require_store_analytics_client()
        store_id = self._resolve_store_id(args)
        tz_name = str(args.get("timezone") or self.request.timezone)
        tz = ZoneInfo(tz_name)
        start_date, end_date = self._resolve_date_range(args, tz)
        total_days = (end_date - start_date).days + 1
        if total_days > MAX_DAILY_RANGE_DAYS:
            raise ToolExecutionError(
                f"Daily date range is limited to {MAX_DAILY_RANGE_DAYS} days per call"
            )
        self.last_timezone = tz_name
        self.last_start_time = datetime.combine(start_date, time.min, tzinfo=tz).astimezone(
            timezone.utc
        )
        self.last_end_time = (
            datetime.combine(end_date, time.min, tzinfo=tz) + timedelta(days=1)
        ).astimezone(timezone.utc)
        return client.get_daily_entry_traffic(
            store_id=store_id,
            start_date=start_date,
            end_date=end_date,
            timezone_name=tz_name,
        )

    def _get_demographics_interval(self, args: dict[str, Any]) -> dict[str, Any]:
        client = self._require_store_analytics_client()
        store_id = self._resolve_store_id(args)
        start_time, end_time = self._resolve_time_window(args)
        self.last_start_time = start_time
        self.last_end_time = end_time
        return client.get_demographics_interval(
            store_id=store_id,
            start_time=start_time,
            end_time=end_time,
        )

    def _get_daily_demographics(self, args: dict[str, Any]) -> dict[str, Any]:
        client = self._require_store_analytics_client()
        store_id = self._resolve_store_id(args)
        tz_name = str(args.get("timezone") or self.request.timezone)
        tz = ZoneInfo(tz_name)
        start_date, end_date = self._resolve_date_range(args, tz)
        total_days = (end_date - start_date).days + 1
        if total_days > MAX_DAILY_RANGE_DAYS:
            raise ToolExecutionError(
                f"Daily date range is limited to {MAX_DAILY_RANGE_DAYS} days per call"
            )
        self.last_timezone = tz_name
        self.last_start_time = datetime.combine(start_date, time.min, tzinfo=tz).astimezone(
            timezone.utc
        )
        self.last_end_time = (
            datetime.combine(end_date, time.min, tzinfo=tz) + timedelta(days=1)
        ).astimezone(timezone.utc)
        return client.get_daily_demographics(
            store_id=store_id,
            start_date=start_date,
            end_date=end_date,
            timezone_name=tz_name,
        )

    def _require_store_analytics_client(self) -> StoreAnalyticsClient:
        if self.store_analytics_client is None or not self.store_analytics_client.configured:
            raise ToolExecutionError(
                "Entry/exit metrics are not configured for this service"
            )
        return self.store_analytics_client

    def _question_mentions_entry_traffic(self) -> bool:
        return question_mentions_entry_traffic(self.request.question)

    def _question_mentions_demographics(self) -> bool:
        return question_mentions_demographics(self.request.question)

    def _list_store_objects(self, args: dict[str, Any]) -> dict[str, Any]:
        store_id = self._resolve_store_id(args)
        objects = self._get_objects(store_id)
        if self.request.object_id is not None:
            selected = next((item for item in objects if item.id == self.request.object_id), None)
            if selected is not None:
                self.object_name = selected.name
        return {
            "store_id": store_id,
            "object_count": len(objects),
            "objects": [item.model_dump(mode="json") for item in objects],
        }

    def _get_interval_counts(self, args: dict[str, Any]) -> dict[str, Any]:
        if self._question_mentions_entry_traffic():
            raise ToolExecutionError(
                "This question is about entry/exit traffic. Use the entry traffic tools instead of object interaction counts."
            )
        if self._question_mentions_demographics():
            raise ToolExecutionError(
                "This question is about demographics. Use the demographics tools instead of object interaction counts."
            )
        store_id = self._resolve_store_id(args)
        object_id = self._resolve_object_id(args, store_id)
        start_time, end_time = self._resolve_time_window(args)
        counts = self._get_store_counts(store_id, start_time, end_time)
        ranking_by_inside = self._sorted_counts(
            counts, key_name="points_inside", tie_breaker="points_around"
        )
        ranking_by_around = self._sorted_counts(
            counts, key_name="points_around", tie_breaker="points_inside"
        )
        totals = self._store_totals(counts)
        self.store_object_count = len(counts)
        self.last_start_time = start_time
        self.last_end_time = end_time

        if object_id is None:
            return {
                "scope": "store",
                "store_id": store_id,
                "time_window": self._time_window_payload(start_time, end_time),
                "metric_note": (
                    "Counts are zone interaction counts from tracker objects, not unique visitors."
                ),
                "store_totals": totals,
                "counts_by_object": [item.model_dump(mode="json") for item in counts],
                "leaders": {
                    "by_inside": [
                        item.model_dump(mode="json") for item in ranking_by_inside[:5]
                    ],
                    "by_around": [
                        item.model_dump(mode="json") for item in ranking_by_around[:5]
                    ],
                },
            }

        current_counts = next((item for item in counts if item.id == object_id), None)
        object_name = self._resolve_object_name(store_id, object_id, current_counts)
        rank_by_inside = self._rank_of_object(ranking_by_inside, object_id)
        rank_by_around = self._rank_of_object(ranking_by_around, object_id)
        self.object_scope_used = True
        self.resolved_object_id = object_id
        self.object_name = object_name
        self.points_inside = current_counts.points_inside if current_counts else 0
        self.points_around = current_counts.points_around if current_counts else 0
        self.rank_by_inside = rank_by_inside
        self.rank_by_around = rank_by_around

        return {
            "scope": "object",
            "store_id": store_id,
            "object": {
                "id": object_id,
                "name": object_name,
                "points_inside": self.points_inside,
                "points_around": self.points_around,
                "points_combined": self.points_inside + self.points_around,
                "rank_by_inside": rank_by_inside,
                "rank_by_around": rank_by_around,
            },
            "time_window": self._time_window_payload(start_time, end_time),
            "metric_note": (
                "Counts are zone interaction counts from tracker objects, not unique visitors."
            ),
            "store_totals": totals,
            "leaders": {
                "by_inside": [item.model_dump(mode="json") for item in ranking_by_inside[:5]],
                "by_around": [item.model_dump(mode="json") for item in ranking_by_around[:5]],
            },
        }

    def _get_daily_counts(self, args: dict[str, Any]) -> dict[str, Any]:
        if self._question_mentions_entry_traffic():
            raise ToolExecutionError(
                "This question is about entry/exit traffic. Use the entry traffic tools instead of object interaction counts."
            )
        if self._question_mentions_demographics():
            raise ToolExecutionError(
                "This question is about demographics. Use the demographics tools instead of object interaction counts."
            )
        store_id = self._resolve_store_id(args)
        object_id = self._resolve_object_id(args, store_id)
        tz_name = str(args.get("timezone") or self.request.timezone)
        tz = ZoneInfo(tz_name)
        start_date, end_date = self._resolve_date_range(args, tz)
        total_days = (end_date - start_date).days + 1
        if total_days > MAX_DAILY_RANGE_DAYS:
            raise ToolExecutionError(
                f"Daily date range is limited to {MAX_DAILY_RANGE_DAYS} days per call"
            )

        rows: list[dict[str, Any]] = []
        current_date = start_date
        while current_date <= end_date:
            local_start = datetime.combine(current_date, time.min, tzinfo=tz)
            local_end = local_start + timedelta(days=1)
            start_utc = local_start.astimezone(timezone.utc)
            end_utc = local_end.astimezone(timezone.utc)
            counts = self._get_store_counts(store_id, start_utc, end_utc)
            ranking_by_inside = self._sorted_counts(
                counts, key_name="points_inside", tie_breaker="points_around"
            )
            ranking_by_around = self._sorted_counts(
                counts, key_name="points_around", tie_breaker="points_inside"
            )

            if object_id is None:
                totals = self._store_totals(counts)
                rows.append(
                    {
                        "date": current_date.isoformat(),
                        "start_time": start_utc.isoformat(),
                        "end_time": end_utc.isoformat(),
                        **totals,
                        "top_object_by_inside": self._serialize_count(ranking_by_inside[0])
                        if ranking_by_inside
                        else None,
                        "top_object_by_around": self._serialize_count(ranking_by_around[0])
                        if ranking_by_around
                        else None,
                    }
                )
            else:
                current_counts = next((item for item in counts if item.id == object_id), None)
                object_name = self._resolve_object_name(store_id, object_id, current_counts)
                points_inside = current_counts.points_inside if current_counts else 0
                points_around = current_counts.points_around if current_counts else 0
                rows.append(
                    {
                        "date": current_date.isoformat(),
                        "start_time": start_utc.isoformat(),
                        "end_time": end_utc.isoformat(),
                        "points_inside": points_inside,
                        "points_around": points_around,
                        "points_combined": points_inside + points_around,
                        "rank_by_inside": self._rank_of_object(ranking_by_inside, object_id),
                        "rank_by_around": self._rank_of_object(ranking_by_around, object_id),
                        "object_name": object_name,
                    }
                )
                self.object_scope_used = True
                self.resolved_object_id = object_id
                self.object_name = object_name

            current_date += timedelta(days=1)

        self.last_timezone = tz_name
        self.last_start_time = datetime.combine(start_date, time.min, tzinfo=tz).astimezone(
            timezone.utc
        )
        self.last_end_time = (
            datetime.combine(end_date, time.min, tzinfo=tz) + timedelta(days=1)
        ).astimezone(timezone.utc)

        if object_id is None:
            return {
                "scope": "store",
                "store_id": store_id,
                "timezone": tz_name,
                "date_window": {
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                },
                "metric_note": (
                    "Counts are zone interaction counts from tracker objects, not unique visitors."
                ),
                "best_days": self._best_days(rows),
                "days": rows,
            }

        best_days = self._best_days(rows)
        best_inside = best_days.get("by_inside")
        if best_inside is not None:
            self.points_inside = best_inside.get("points_inside")
            self.points_around = best_inside.get("points_around")
            self.rank_by_inside = best_inside.get("rank_by_inside")
            self.rank_by_around = best_inside.get("rank_by_around")

        return {
            "scope": "object",
            "store_id": store_id,
            "object_id": object_id,
            "object_name": self.object_name,
            "timezone": tz_name,
            "date_window": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
            "metric_note": (
                "Counts are zone interaction counts from tracker objects, not unique visitors."
            ),
            "best_days": best_days,
            "days": rows,
        }

    def _resolve_store_id(self, args: dict[str, Any]) -> int:
        raw_value = args.get("store_id", self.request.store_id)
        try:
            store_id = int(raw_value)
        except (TypeError, ValueError) as exc:
            raise ToolExecutionError("store_id must be an integer") from exc
        if store_id < 1:
            raise ToolExecutionError("store_id must be greater than 0")
        return store_id

    def _resolve_object_id(self, args: dict[str, Any], store_id: int) -> int | None:
        raw_value = args.get("object_id")
        if raw_value is None:
            return None
        try:
            object_id = int(raw_value)
        except (TypeError, ValueError) as exc:
            raise ToolExecutionError("object_id must be an integer") from exc
        if object_id < 1:
            raise ToolExecutionError("object_id must be greater than 0")
        if self.request.object_id is None and not self._question_mentions_object(store_id, object_id):
            raise ToolExecutionError(
                "This question does not identify a specific object. Omit object_id for store-wide analytics or resolve the object name from the question first."
            )
        return object_id

    def _resolve_time_window(self, args: dict[str, Any]) -> tuple[datetime, datetime]:
        start_raw = args.get("start_time")
        end_raw = args.get("end_time")
        start_time = self._parse_datetime(start_raw) if start_raw else self.request.start_time
        end_time = self._parse_datetime(end_raw) if end_raw else self.request.end_time
        if start_time is None or end_time is None:
            raise ToolExecutionError(
                "start_time and end_time are required unless the request already provides a default interval"
            )
        if end_time <= start_time:
            raise ToolExecutionError("end_time must be greater than start_time")
        return start_time, end_time

    def _resolve_date_range(
        self, args: dict[str, Any], tz: ZoneInfo
    ) -> tuple[date, date]:
        start_raw = args.get("start_date")
        end_raw = args.get("end_date")
        if start_raw:
            start_date = self._parse_date(str(start_raw))
        elif self.request.start_time is not None:
            start_date = self.request.start_time.astimezone(tz).date()
        else:
            raise ToolExecutionError(
                "start_date is required unless the request already provides a default interval"
            )

        if end_raw:
            end_date = self._parse_date(str(end_raw))
        elif self.request.end_time is not None:
            end_date = (self.request.end_time - timedelta(microseconds=1)).astimezone(tz).date()
        else:
            raise ToolExecutionError(
                "end_date is required unless the request already provides a default interval"
            )

        if end_date < start_date:
            raise ToolExecutionError("end_date must not be earlier than start_date")
        return start_date, end_date

    def _get_objects(self, store_id: int) -> list[TrackerObject]:
        if store_id not in self._objects_cache:
            self._objects_cache[store_id] = self.tracker_client.list_objects(store_id)
        self.store_object_count = len(self._objects_cache[store_id])
        return self._objects_cache[store_id]

    def _get_store_counts(
        self, store_id: int, start_time: datetime, end_time: datetime
    ) -> list[TrackerCounts]:
        cache_key = (store_id, start_time.isoformat(), end_time.isoformat())
        if cache_key not in self._store_counts_cache:
            self._store_counts_cache[cache_key] = self.tracker_client.get_store_counts(
                store_id, start_time, end_time
            )
        return self._store_counts_cache[cache_key]

    def _resolve_object_name(
        self,
        store_id: int,
        object_id: int,
        current_counts: TrackerCounts | None,
    ) -> str:
        if current_counts is not None:
            return current_counts.name
        objects = self._get_objects(store_id)
        selected = next((item for item in objects if item.id == object_id), None)
        if selected is None:
            raise ToolExecutionError(
                f"Object {object_id} was not found in store {store_id}"
            )
        return selected.name

    def _question_mentions_object(self, store_id: int, object_id: int) -> bool:
        object_name = self._resolve_object_name(store_id, object_id, current_counts=None)
        question = self.request.question.casefold()
        if object_name.casefold() in question:
            return True

        tokens = [
            token
            for token in re.split(r"[^0-9a-zA-Zа-яА-ЯёЁ]+", object_name.casefold())
            if token
        ]
        for token in tokens:
            if len(token) >= 4 and token in question:
                return True
            if len(token) >= 4 and token[:4] in question:
                return True
        return False

    @staticmethod
    def _retry_hint_for_message(message: str) -> str | None:
        lowered = message.casefold()
        if "does not identify a specific object" in lowered:
            return "retry_without_object_id"
        if "start_time and end_time are required" in lowered:
            return "provide_or_infer_time_window"
        if "start_date is required" in lowered or "end_date is required" in lowered:
            return "provide_or_infer_date_range"
        if "question is about entry/exit traffic" in lowered:
            return "use_entry_traffic_tools"
        if "question is about demographics" in lowered:
            return "use_demographics_tools"
        return None

    @staticmethod
    def _parse_arguments(arguments: str | dict[str, Any] | None) -> dict[str, Any]:
        if arguments is None:
            return {}
        if isinstance(arguments, dict):
            return arguments
        try:
            parsed = json.loads(arguments)
        except json.JSONDecodeError as exc:
            raise ToolExecutionError("Tool arguments must be valid JSON") from exc
        if not isinstance(parsed, dict):
            raise ToolExecutionError("Tool arguments must decode to a JSON object")
        return parsed

    def _parse_datetime(self, value: str) -> datetime:
        normalized = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ToolExecutionError(f"Invalid datetime: {value}") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=ZoneInfo(self.request.timezone))
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _parse_date(value: str) -> date:
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ToolExecutionError(f"Invalid date: {value}") from exc

    @staticmethod
    def _sorted_counts(
        counts: list[TrackerCounts], key_name: str, tie_breaker: str
    ) -> list[TrackerCounts]:
        return sorted(
            counts,
            key=lambda item: (
                -getattr(item, key_name),
                -getattr(item, tie_breaker),
                item.name.lower(),
            ),
        )

    @staticmethod
    def _rank_of_object(counts: list[TrackerCounts], object_id: int) -> int | None:
        for index, item in enumerate(counts, start=1):
            if item.id == object_id:
                return index
        return None

    @staticmethod
    def _serialize_count(item: TrackerCounts) -> dict[str, Any]:
        payload = item.model_dump(mode="json")
        payload["points_combined"] = item.points_inside + item.points_around
        return payload

    @staticmethod
    def _store_totals(counts: list[TrackerCounts]) -> dict[str, int]:
        points_inside = sum(item.points_inside for item in counts)
        points_around = sum(item.points_around for item in counts)
        return {
            "points_inside": points_inside,
            "points_around": points_around,
            "points_combined": points_inside + points_around,
        }

    @staticmethod
    def _time_window_payload(start_time: datetime, end_time: datetime) -> dict[str, str]:
        return {
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
        }

    @staticmethod
    def _best_days(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any] | None]:
        if not rows:
            return {"by_inside": None, "by_around": None, "by_combined": None}

        def picker(metric: str) -> dict[str, Any]:
            return max(
                rows,
                key=lambda item: (
                    int(item.get(metric, 0)),
                    int(item.get("points_combined", 0)),
                    item.get("date", ""),
                ),
            )

        return {
            "by_inside": picker("points_inside"),
            "by_around": picker("points_around"),
            "by_combined": picker("points_combined"),
        }
