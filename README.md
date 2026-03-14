# Myrza Object Chat API

Минимальный сервис на FastAPI, который:

- сам решает, какие backend-данные нужны под вопрос, и добирает их через tool calling;
- ходит в `myrza_tracker` по HTTP за объектами, сводкой за интервал и дневной динамикой;
- задает вопрос модели OpenAI через Responses API с backend-tools;
- отдает ответ фронту одним REST-эндпоинтом.

## Что уже есть

- `GET /` отдает тестовый веб-чат для ручной проверки сценариев;
- `POST /api/object-chat` для вопроса по магазину или конкретному объекту;
- `GET /api/store-objects` для загрузки списка зон магазина в UI;
- `GET /health` для базовой диагностики;
- `GET /health/dependencies` для проверки `OPENAI_API_KEY` и доступности `myrza_tracker`.

## Требования

- Python 3.11+
- запущенный `myrza_tracker`
- `OPENAI_API_KEY`

## Быстрый старт

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env.local
uvicorn app.main:app --reload --port 8020
```

После запуска открой `http://127.0.0.1:8020/` и тестируй чат из браузера.

Отдельно поднимите `myrza_tracker`, например:

```bash
cd "/Users/carelogs/Own projects/myrza_tracker"
"/Users/carelogs/Own projects/gpt-api/.venv311/bin/python" -m uvicorn backend.myrza_tracker.main:app --port 8010
```

## Примеры запросов

```bash
curl -X POST "http://127.0.0.1:8020/api/object-chat" \
  -H "Content-Type: application/json" \
  -d '{
    "store_id": 5,
    "object_id": 8,
    "question": "Что можно сказать про этот объект за интервал?",
    "start_time": "2026-03-02T06:00:00Z",
    "end_time": "2026-03-02T06:30:00Z"
  }'
```

```bash
curl -X POST "http://127.0.0.1:8020/api/object-chat" \
  -H "Content-Type: application/json" \
  -d '{
    "store_id": 5,
    "question": "В какой день за последний месяц было больше всего клиентов?",
    "timezone": "UTC"
  }'
```

Во втором примере сервис не получает готовые counts заранее. Модель сама вызывает backend-tools и, если нужно, строит дневную выборку за период.

## Что нужно для нормального production-сервиса

- вынести реальный секрет в безопасное хранилище, а не держать в локальном env;
- добавить auth между фронтом и этим API;
- договориться с фронтом о контракте: всегда ли он знает `store_id`, когда передает `object_id`, и в каком `timezone` считать относительные периоды;
- решить стратегию истории диалога: stateless, Redis, Postgres или OpenAI `previous_response_id`;
- добавить rate limiting, structured logs и tracing;
- покрыть интеграционными тестами happy path, tool-loop ошибки и недоступность `myrza_tracker`.
