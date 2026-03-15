from __future__ import annotations

import re

ENTRY_TRAFFIC_KEYWORDS = (
    "вход",
    "входы",
    "входов",
    "входам",
    "входящий",
    "вошло",
    "вошли",
    "зашло",
    "зашли",
    "выход",
    "выходы",
    "выходов",
    "выходам",
    "вышло",
    "вышли",
    "entry",
    "entries",
    "exit",
    "exits",
    "door",
    "doors",
    "footfall",
)


def normalize_question(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower().replace("ё", "е")).strip()


def question_mentions_entry_traffic(question: str) -> bool:
    normalized = normalize_question(question)
    return any(keyword in normalized for keyword in ENTRY_TRAFFIC_KEYWORDS)
