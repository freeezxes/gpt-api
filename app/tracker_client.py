from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx

from app.schemas import TrackerCounts, TrackerObject


class TrackerClientError(RuntimeError):
    """Raised when the tracker API cannot be reached or returns an invalid response."""


def _format_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


class TrackerClient:
    def __init__(self, base_url: str, timeout_seconds: float) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def list_objects(self, store_id: int) -> list[TrackerObject]:
        payload = self._get("/objects", params={"store_id": store_id})
        return [TrackerObject.model_validate(item) for item in payload]

    def get_store_counts(
        self, store_id: int, start_time: datetime, end_time: datetime
    ) -> list[TrackerCounts]:
        payload = self._get(
            "/objects/counts",
            params={
                "store_id": store_id,
                "start_time": _format_datetime(start_time),
                "end_time": _format_datetime(end_time),
            },
        )
        return [TrackerCounts.model_validate(item) for item in payload]

    def get_object_counts(
        self, object_id: int, start_time: datetime, end_time: datetime
    ) -> TrackerCounts:
        payload = self._get(
            f"/objects/{object_id}/counts",
            params={
                "start_time": _format_datetime(start_time),
                "end_time": _format_datetime(end_time),
            },
        )
        return TrackerCounts.model_validate(payload)

    def _get(self, path: str, params: dict[str, Any]) -> Any:
        url = f"{self.base_url}{path}"
        try:
            response = httpx.get(url, params=params, timeout=self.timeout_seconds)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise TrackerClientError(
                f"Tracker API returned {exc.response.status_code} for {path}"
            ) from exc
        except httpx.RequestError as exc:
            raise TrackerClientError(f"Tracker API request failed: {exc}") from exc

        try:
            return response.json()
        except ValueError as exc:
            raise TrackerClientError("Tracker API returned invalid JSON") from exc
