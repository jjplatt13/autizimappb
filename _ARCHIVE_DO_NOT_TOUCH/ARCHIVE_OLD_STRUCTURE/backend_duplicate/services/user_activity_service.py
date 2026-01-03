# ============================================================
#   Phase 2C — User Activity Logger (Postgres Event System)
#   Psycopg2-Compatible Version (Corrected)
# ============================================================

import json

from datetime import datetime
import uuid
from typing import Optional, Dict, Any

from fastapi import Request
from psycopg2.extras import RealDictCursor
from db import get_db


# ------------------------------------------------------------
#   Session ID Helper
# ------------------------------------------------------------
def get_or_create_session_id(request: Request) -> str:
    session_id = request.cookies.get("session_id")
    if not session_id:
        session_id = str(uuid.uuid4())
    return session_id


# ------------------------------------------------------------
#   Device Fingerprint
# ------------------------------------------------------------
def get_device_id(request: Request) -> str:
    ua = request.headers.get("user-agent", "")
    accept = request.headers.get("accept", "")
    ip = request.client.host if request.client else "0.0.0.0"
    raw = f"{ua}|{accept}|{ip}"
    return uuid.uuid5(uuid.NAMESPACE_DNS, raw).hex


# ------------------------------------------------------------
#   LOG EVENT → Postgres analytics_events table
#   (Correct psycopg2 version)
# ------------------------------------------------------------
async def log_event(
    request: Request,
    event_type: str,
    metadata: Optional[Dict[str, Any]] = None,
    user_id: Optional[int] = None,
    intent_score: Optional[float] = None,
    child_profile: Optional[Dict[str, Any]] = None
):
    """
    Writes analytics event into Postgres using psycopg2.
    (Your DB returns a raw psycopg2 connection, not SQLAlchemy Session.)
    """

    conn = get_db()  # raw psycopg2 connection
    cur = conn.cursor()

    session_id = get_or_create_session_id(request)
    device_id = get_device_id(request)

    metadata = metadata or {}
    child_profile = child_profile or {}

    query = """
        INSERT INTO analytics_events (
            event_type,
            session_id,
            device_id,
            user_id,
            metadata,
            child_age,
            child_needs,
            diagnosis,
            preferred_services,
            intent_score,
            created_at
        )
        VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s
        );
    """

    params = (
        event_type,
        session_id,
        device_id,
        user_id,
        json.dumps(metadata),                         # metadata::jsonb
        child_profile.get("child_age"),
        child_profile.get("child_needs"),
        child_profile.get("diagnosis"),
        child_profile.get("preferred_services"),
        intent_score,
        datetime.utcnow(),
    )

    try:
        cur.execute(query, params)
        conn.commit()
    except Exception as e:
        print("❌ Analytics logging failed:", e)
    finally:
        cur.close()
        conn.close()

    return {"status": "ok", "session_id": session_id}
