from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, Field, field_validator, model_validator


class TrackerObject(BaseModel):
    id: int
    store_id: int
    name: str
    polygon_points: list[tuple[float, float]]
    buffer_polygon_points: list[tuple[float, float]] | None = None
    buffer_radius: float | int | None = None


class TrackerCounts(BaseModel):
    id: int
    name: str
    store_id: int
    points_inside: int
    points_around: int


class ObjectChatRequest(BaseModel):
    store_id: int = Field(..., ge=1)
    object_id: int | None = Field(default=None, ge=1)
    question: str = Field(..., min_length=1, max_length=4000)
    start_time: datetime | None = None
    end_time: datetime | None = None
    timezone: str = "UTC"
    model: str | None = None
    previous_response_id: str | None = None

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"Unknown timezone: {value}") from exc
        return value

    @model_validator(mode="after")
    def validate_time_window(self) -> "ObjectChatRequest":
        if (self.start_time is None) != (self.end_time is None):
            raise ValueError("start_time and end_time must be provided together")
        if self.start_time and self.end_time and self.end_time <= self.start_time:
            raise ValueError("end_time must be greater than start_time")
        return self


class ObjectChatContext(BaseModel):
    store_id: int
    object_id: int | None = None
    object_name: str | None = None
    store_object_count: int | None = None
    timezone: str = "UTC"
    start_time: datetime | None = None
    end_time: datetime | None = None
    points_inside: int | None = None
    points_around: int | None = None
    rank_by_inside: int | None = None
    rank_by_around: int | None = None
    tools_used: list[str] = Field(default_factory=list)


class ObjectChatResponse(BaseModel):
    answer: str
    model: str
    response_id: str | None = None
    context: ObjectChatContext


class HealthResponse(BaseModel):
    status: str
    openai_api_key_configured: bool
    default_model: str
    tracker_api_base_url: str


class DependencyHealthResponse(HealthResponse):
    tracker_reachable: bool
    tracker_store_id_checked: int
    tracker_objects_found: int | None = None
    tracker_error: str | None = None
