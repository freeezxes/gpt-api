from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row


class StoreAnalyticsClientError(RuntimeError):
    """Raised when store-level analytics cannot be loaded from Postgres."""


class StoreAnalyticsClient:
    def __init__(self, database_url: str | None, timeout_seconds: float) -> None:
        self.database_url = database_url
        self.timeout_seconds = timeout_seconds

    @property
    def configured(self) -> bool:
        return bool(self.database_url)

    def get_entry_traffic_interval(
        self, store_id: int, start_time: datetime, end_time: datetime
    ) -> dict[str, Any]:
        self._ensure_configured()
        rows = self._fetchall(
            """
            select
                e.door_counter_id,
                coalesce(dc.name, 'Door ' || e.door_counter_id::text) as door_counter_name,
                count(*) filter (where e.direction = 'IN')::bigint as entries_in,
                count(*) filter (where e.direction = 'OUT')::bigint as exits_out
            from event_entry_exit e
            left join door_counter dc on dc.id = e.door_counter_id
            where e.store_id = %(store_id)s
              and e.timestamp >= %(start_time)s
              and e.timestamp < %(end_time)s
            group by e.door_counter_id, dc.name
            order by entries_in desc, exits_out desc, e.door_counter_id asc
            """,
            {
                "store_id": store_id,
                "start_time": start_time,
                "end_time": end_time,
            },
        )
        by_door_counter = [self._normalize_counter_row(row) for row in rows]
        entries_in = sum(item["entries_in"] for item in by_door_counter)
        exits_out = sum(item["exits_out"] for item in by_door_counter)
        return {
            "scope": "store_entry_traffic",
            "store_id": store_id,
            "time_window": {
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
            },
            "metric_note": (
                "Counts come from entry/exit door-counter events, not object interaction points."
            ),
            "traffic": {
                "entries_in": entries_in,
                "exits_out": exits_out,
                "net_flow": entries_in - exits_out,
            },
            "by_door_counter": by_door_counter,
        }

    def get_daily_entry_traffic(
        self,
        store_id: int,
        start_date: date,
        end_date: date,
        timezone_name: str,
    ) -> dict[str, Any]:
        self._ensure_configured()
        tz = ZoneInfo(timezone_name)
        local_start = datetime.combine(start_date, time.min, tzinfo=tz)
        local_end = datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=tz)
        start_utc = local_start.astimezone(timezone.utc)
        end_utc = local_end.astimezone(timezone.utc)
        rows = self._fetchall(
            """
            select
                (e.timestamp at time zone %(timezone)s)::date as local_date,
                e.door_counter_id,
                coalesce(dc.name, 'Door ' || e.door_counter_id::text) as door_counter_name,
                count(*) filter (where e.direction = 'IN')::bigint as entries_in,
                count(*) filter (where e.direction = 'OUT')::bigint as exits_out
            from event_entry_exit e
            left join door_counter dc on dc.id = e.door_counter_id
            where e.store_id = %(store_id)s
              and e.timestamp >= %(start_time)s
              and e.timestamp < %(end_time)s
            group by local_date, e.door_counter_id, dc.name
            order by local_date asc, e.door_counter_id asc
            """,
            {
                "store_id": store_id,
                "timezone": timezone_name,
                "start_time": start_utc,
                "end_time": end_utc,
            },
        )

        daily_rows: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            local_date = row["local_date"].isoformat()
            daily_rows.setdefault(local_date, []).append(self._normalize_counter_row(row))

        days: list[dict[str, Any]] = []
        current_date = start_date
        while current_date <= end_date:
            day_key = current_date.isoformat()
            counters = daily_rows.get(day_key, [])
            entries_in = sum(item["entries_in"] for item in counters)
            exits_out = sum(item["exits_out"] for item in counters)
            day_local_start = datetime.combine(current_date, time.min, tzinfo=tz)
            day_local_end = day_local_start + timedelta(days=1)
            days.append(
                {
                    "date": day_key,
                    "start_time": day_local_start.astimezone(timezone.utc).isoformat(),
                    "end_time": day_local_end.astimezone(timezone.utc).isoformat(),
                    "entries_in": entries_in,
                    "exits_out": exits_out,
                    "net_flow": entries_in - exits_out,
                    "by_door_counter": counters,
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
            "metric_note": (
                "Counts come from entry/exit door-counter events, not object interaction points."
            ),
            "best_days": self._best_days(days),
            "days": days,
        }

    def get_demographics_interval(
        self, store_id: int, start_time: datetime, end_time: datetime
    ) -> dict[str, Any]:
        self._ensure_configured()
        row = self._fetchone(
            """
            select
                coalesce(sum(total_detections), 0)::bigint as total_detections,
                coalesce(sum(unique_sessions), 0)::bigint as unique_sessions,
                coalesce(sum(male_count), 0)::bigint as male_count,
                coalesce(sum(female_count), 0)::bigint as female_count,
                coalesce(sum(unknown_gender_count), 0)::bigint as unknown_gender_count,
                coalesce(sum(age0_17), 0)::bigint as age0_17,
                coalesce(sum(age18_24), 0)::bigint as age18_24,
                coalesce(sum(age25_34), 0)::bigint as age25_34,
                coalesce(sum(age35_44), 0)::bigint as age35_44,
                coalesce(sum(age45_54), 0)::bigint as age45_54,
                coalesce(sum(age55_plus), 0)::bigint as age55_plus
            from person_traffic_aggregate
            where store_id = %(store_id)s
              and zone_id is null
              and time_bucket >= %(start_time)s
              and time_bucket < %(end_time)s
            """,
            {
                "store_id": store_id,
                "start_time": start_time,
                "end_time": end_time,
            },
        )
        gender = self._normalize_gender_breakdown(row)
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
                "total_detections": int(row["total_detections"]),
                "unique_sessions": int(row["unique_sessions"]),
                "gender": gender,
                "top_gender": self._top_gender(gender),
                "age_breakdown": self._normalize_age_breakdown(row),
            },
        }

    def get_daily_demographics(
        self,
        store_id: int,
        start_date: date,
        end_date: date,
        timezone_name: str,
    ) -> dict[str, Any]:
        self._ensure_configured()
        tz = ZoneInfo(timezone_name)
        local_start = datetime.combine(start_date, time.min, tzinfo=tz)
        local_end = datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=tz)
        start_utc = local_start.astimezone(timezone.utc)
        end_utc = local_end.astimezone(timezone.utc)
        rows = self._fetchall(
            """
            select
                (time_bucket at time zone %(timezone)s)::date as local_date,
                coalesce(sum(total_detections), 0)::bigint as total_detections,
                coalesce(sum(unique_sessions), 0)::bigint as unique_sessions,
                coalesce(sum(male_count), 0)::bigint as male_count,
                coalesce(sum(female_count), 0)::bigint as female_count,
                coalesce(sum(unknown_gender_count), 0)::bigint as unknown_gender_count,
                coalesce(sum(age0_17), 0)::bigint as age0_17,
                coalesce(sum(age18_24), 0)::bigint as age18_24,
                coalesce(sum(age25_34), 0)::bigint as age25_34,
                coalesce(sum(age35_44), 0)::bigint as age35_44,
                coalesce(sum(age45_54), 0)::bigint as age45_54,
                coalesce(sum(age55_plus), 0)::bigint as age55_plus
            from person_traffic_aggregate
            where store_id = %(store_id)s
              and zone_id is null
              and time_bucket >= %(start_time)s
              and time_bucket < %(end_time)s
            group by local_date
            order by local_date asc
            """,
            {
                "store_id": store_id,
                "timezone": timezone_name,
                "start_time": start_utc,
                "end_time": end_utc,
            },
        )
        rows_by_date = {
            row["local_date"].isoformat(): row
            for row in rows
        }
        days: list[dict[str, Any]] = []
        current_date = start_date
        while current_date <= end_date:
            day_key = current_date.isoformat()
            row = rows_by_date.get(day_key)
            if row is None:
                row = self._empty_demographics_row()
            gender = self._normalize_gender_breakdown(row)
            age_breakdown = self._normalize_age_breakdown(row)
            day_local_start = datetime.combine(current_date, time.min, tzinfo=tz)
            day_local_end = day_local_start + timedelta(days=1)
            days.append(
                {
                    "date": day_key,
                    "start_time": day_local_start.astimezone(timezone.utc).isoformat(),
                    "end_time": day_local_end.astimezone(timezone.utc).isoformat(),
                    "total_detections": int(row["total_detections"]),
                    "unique_sessions": int(row["unique_sessions"]),
                    "gender": gender,
                    "top_gender": self._top_gender(gender),
                    "age_breakdown": age_breakdown,
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
            "best_days": self._best_gender_days(days),
            "days": days,
        }

    def _fetchall(self, query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        if not self.database_url:
            raise StoreAnalyticsClientError("Store analytics database is not configured")
        connect_timeout = max(1, int(self.timeout_seconds))
        try:
            with psycopg.connect(
                self.database_url,
                connect_timeout=connect_timeout,
                row_factory=dict_row,
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(query, params)
                    return list(cursor.fetchall())
        except psycopg.Error as exc:
            raise StoreAnalyticsClientError(f"Store analytics query failed: {exc}") from exc

    def _fetchone(self, query: str, params: dict[str, Any]) -> dict[str, Any]:
        rows = self._fetchall(query, params)
        if not rows:
            return self._empty_demographics_row()
        return rows[0]

    def _ensure_configured(self) -> None:
        if not self.database_url:
            raise StoreAnalyticsClientError("Store analytics database is not configured")

    @staticmethod
    def _normalize_counter_row(row: dict[str, Any]) -> dict[str, Any]:
        entries_in = int(row.get("entries_in") or 0)
        exits_out = int(row.get("exits_out") or 0)
        return {
            "door_counter_id": int(row["door_counter_id"]),
            "door_counter_name": str(row["door_counter_name"]),
            "entries_in": entries_in,
            "exits_out": exits_out,
            "net_flow": entries_in - exits_out,
        }

    @staticmethod
    def _best_days(days: list[dict[str, Any]]) -> dict[str, dict[str, Any] | None]:
        if not days:
            return {"by_entries_in": None, "by_exits_out": None}
        by_entries_in = max(days, key=lambda item: (item["entries_in"], item["exits_out"], item["date"]))
        by_exits_out = max(days, key=lambda item: (item["exits_out"], item["entries_in"], item["date"]))
        return {
            "by_entries_in": by_entries_in,
            "by_exits_out": by_exits_out,
        }

    @staticmethod
    def _normalize_gender_breakdown(row: dict[str, Any]) -> dict[str, Any]:
        male_count = int(row.get("male_count") or 0)
        female_count = int(row.get("female_count") or 0)
        unknown_gender_count = int(row.get("unknown_gender_count") or 0)
        known_total = male_count + female_count
        return {
            "male_count": male_count,
            "female_count": female_count,
            "unknown_gender_count": unknown_gender_count,
            "known_total": known_total,
            "male_share": round((male_count / known_total) * 100, 1) if known_total else 0.0,
            "female_share": round((female_count / known_total) * 100, 1)
            if known_total
            else 0.0,
        }

    @staticmethod
    def _normalize_age_breakdown(row: dict[str, Any]) -> dict[str, int]:
        return {
            "age0_17": int(row.get("age0_17") or 0),
            "age18_24": int(row.get("age18_24") or 0),
            "age25_34": int(row.get("age25_34") or 0),
            "age35_44": int(row.get("age35_44") or 0),
            "age45_54": int(row.get("age45_54") or 0),
            "age55_plus": int(row.get("age55_plus") or 0),
        }

    @staticmethod
    def _top_gender(gender: dict[str, Any]) -> dict[str, Any] | None:
        male_count = int(gender.get("male_count") or 0)
        female_count = int(gender.get("female_count") or 0)
        if male_count == 0 and female_count == 0:
            return None
        if male_count >= female_count:
            return {"label": "male", "count": male_count, "margin": male_count - female_count}
        return {"label": "female", "count": female_count, "margin": female_count - male_count}

    @staticmethod
    def _best_gender_days(days: list[dict[str, Any]]) -> dict[str, dict[str, Any] | None]:
        if not days:
            return {"by_male_count": None, "by_female_count": None}
        by_male_count = max(
            days,
            key=lambda item: (
                int(item["gender"]["male_count"]),
                int(item["gender"]["female_count"]),
                item["date"],
            ),
        )
        by_female_count = max(
            days,
            key=lambda item: (
                int(item["gender"]["female_count"]),
                int(item["gender"]["male_count"]),
                item["date"],
            ),
        )
        return {
            "by_male_count": by_male_count,
            "by_female_count": by_female_count,
        }

    @staticmethod
    def _empty_demographics_row() -> dict[str, int]:
        return {
            "total_detections": 0,
            "unique_sessions": 0,
            "male_count": 0,
            "female_count": 0,
            "unknown_gender_count": 0,
            "age0_17": 0,
            "age18_24": 0,
            "age25_34": 0,
            "age35_44": 0,
            "age45_54": 0,
            "age55_plus": 0,
        }
