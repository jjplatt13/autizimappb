"""
User Activity Service
Logs all user events to Postgres for analytics
"""
import json
from datetime import datetime
from fastapi import Request
from typing import Optional, Dict, Any
from app.core.database import get_db
from app.analytics.session import get_device_id, hash_ip
import os

ANALYTICS_SALT = os.getenv("ANALYTICS_SALT", "CHANGE_ME")

async def log_event(
    request: Request,
    event_type: str,
    metadata: Dict[str, Any],
    intent_score: float,
    user_id: Optional[int] = None,
    session_id: Optional[str] = None,
    provider_id: Optional[int] = None
):
    """
    Log user activity event to Postgres
    """
    try:
        device_id = get_device_id(request)
        ip = request.client.host if request.client else None
        ip_hashed = hash_ip(ip, ANALYTICS_SALT) if ip else None
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO user_activity (
                event_type,
                user_id,
                session_id,
                device_id,
                ip_hash,
                provider_id,
                metadata,
                intent_score,
                created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            event_type,
            user_id,
            session_id,
            device_id,
            ip_hashed,
            provider_id,
            json.dumps(metadata),
            intent_score,
            datetime.utcnow()
        ))
        
        conn.commit()
        cur.close()
        conn.close()
        
    except Exception as e:
        # Silent fail - don't break app if analytics fails
        import sentry_sdk
        sentry_sdk.capture_exception(e)
