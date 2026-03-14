from __future__ import annotations

from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from app.config import Settings, get_settings
from app.schemas import (
    DependencyHealthResponse,
    HealthResponse,
    ObjectChatRequest,
    ObjectChatResponse,
    TrackerObject,
)
from app.service import ConfigurationError, ObjectChatService, ObjectNotFoundError
from app.tracker_client import TrackerClientError

STATIC_DIR = Path(__file__).resolve().parent / "static"

app = FastAPI(title="Myrza Object Chat API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def get_chat_service(settings: Settings = Depends(get_settings)) -> ObjectChatService:
    return ObjectChatService(settings=settings)


@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health", response_model=HealthResponse)
def health(settings: Settings = Depends(get_settings)) -> HealthResponse:
    return HealthResponse(
        status="ok",
        openai_api_key_configured=bool(settings.openai_api_key),
        default_model=settings.openai_model,
        tracker_api_base_url=settings.tracker_api_base_url,
    )


@app.get("/health/dependencies", response_model=DependencyHealthResponse)
def health_dependencies(
    store_id: int = Query(default=5, ge=1),
    settings: Settings = Depends(get_settings),
    service: ObjectChatService = Depends(get_chat_service),
) -> DependencyHealthResponse:
    tracker_reachable = False
    tracker_objects_found = None
    tracker_error = None

    try:
        tracker_objects_found = len(service.tracker_client.list_objects(store_id))
        tracker_reachable = True
    except TrackerClientError as exc:
        tracker_error = str(exc)

    return DependencyHealthResponse(
        status="ok" if tracker_reachable else "degraded",
        openai_api_key_configured=bool(settings.openai_api_key),
        default_model=settings.openai_model,
        tracker_api_base_url=settings.tracker_api_base_url,
        tracker_reachable=tracker_reachable,
        tracker_store_id_checked=store_id,
        tracker_objects_found=tracker_objects_found,
        tracker_error=tracker_error,
    )


@app.get("/api/store-objects", response_model=list[TrackerObject])
def store_objects(
    store_id: int = Query(..., ge=1),
    service: ObjectChatService = Depends(get_chat_service),
) -> list[TrackerObject]:
    try:
        return service.tracker_client.list_objects(store_id)
    except TrackerClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.post("/api/object-chat", response_model=ObjectChatResponse)
def object_chat(
    payload: ObjectChatRequest,
    service: ObjectChatService = Depends(get_chat_service),
) -> ObjectChatResponse:
    try:
        return service.answer_question(payload)
    except ConfigurationError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except ObjectNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except TrackerClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
