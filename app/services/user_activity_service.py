import json
from typing import Any, Dict, Optional
from fastapi import Request

# DB connection (root-level db folder)
from db.connection import get_db


def _safe_json(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except Exception:
        return str(value)


async def log_event(
    request: Request,
    event_type: str,
    metadata: Optional[Dict[str, Any]] = None,
    provider_id: Optional[int] = None,
    specialty_id: Optional[int] = None,
    query_text: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
    radius_miles: Optional[int] = None,
    source: str = "search",  # ✅ CHANGED from "api" to "search"
    intent_score: Optional[float] = None,
) -> None:
    """
    Async-safe analytics logger.
    Matches main.py exactly.
    Writes to analytics_events_v2.
    """

    payload = metadata or {}
    payload = {k: _safe_json(v) for k, v in payload.items()}

    if intent_score is not None:
        payload["intent_score"] = intent_score

    payload["path"] = request.url.path
    payload["method"] = request.method

    # ✅ CORRECT: use get_db() as a context manager
    with get_db() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO analytics_events_v2
                    (
                        event_name,
                        provider_id,
                        specialty_id,
                        query_text,
                        city,
                        state,
                        radius_miles,
                        source,
                        metadata
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                    """,
                    (
                        event_type,
                        provider_id,
                        specialty_id,
                        query_text,
                        city,
                        state,
                        radius_miles,
                        source,
                        json.dumps(payload),
                    ),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
