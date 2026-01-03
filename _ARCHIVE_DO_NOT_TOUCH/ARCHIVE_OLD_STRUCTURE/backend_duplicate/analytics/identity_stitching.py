# ============================================================
#   Phase 2C — Identity Stitching
#   When user signs up → merge anonymous history
# ============================================================

from sqlalchemy import text
from db import get_db


def merge_anonymous_history_into_user(user_id: int, session_id: str, device_id: str):
    """
    After sign-up:
    All previous events belonging to same session/device
    now belong to the new authenticated user.
    """

    db = get_db()

    query = text("""
        UPDATE analytics_events
        SET user_id = :user_id
        WHERE session_id = :session_id
           OR device_id = :device_id
    """)

    db.execute(query, {
        "user_id": user_id,
        "session_id": session_id,
        "device_id": device_id
    })
    db.commit()

    return {"merged": True}
