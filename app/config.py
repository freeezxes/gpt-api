from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[1]

for env_name in (".env", ".env.local"):
    env_path = BASE_DIR / env_name
    if env_path.exists():
        load_dotenv(env_path, override=False)


def _parse_cors_allowed_origins(raw_value: str | None) -> list[str]:
    if not raw_value:
        return ["*"]

    origins = [item.strip() for item in raw_value.split(",") if item.strip()]
    return origins or ["*"]


@dataclass(frozen=True)
class Settings:
    app_name: str = "Myrza Object Chat API"
    tracker_api_base_url: str = "http://127.0.0.1:8010/tracker-api/api"
    openai_api_key: str | None = None
    openai_model: str = "gpt-5-mini"
    openai_max_output_tokens: int = 500
    openai_reasoning_effort: str = "low"
    openai_max_tool_rounds: int = 6
    request_timeout_seconds: float = 30.0
    cors_allowed_origins: list[str] | None = None


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings(
        tracker_api_base_url=os.getenv(
            "TRACKER_API_BASE_URL", "http://127.0.0.1:8010/tracker-api/api"
        ).rstrip("/"),
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        openai_model=os.getenv("OPENAI_MODEL", "gpt-5-mini"),
        openai_max_output_tokens=int(os.getenv("OPENAI_MAX_OUTPUT_TOKENS", "500")),
        openai_reasoning_effort=os.getenv("OPENAI_REASONING_EFFORT", "low"),
        openai_max_tool_rounds=int(os.getenv("OPENAI_MAX_TOOL_ROUNDS", "6")),
        request_timeout_seconds=float(os.getenv("REQUEST_TIMEOUT_SECONDS", "30")),
        cors_allowed_origins=_parse_cors_allowed_origins(
            os.getenv("CORS_ALLOWED_ORIGINS")
        ),
    )
