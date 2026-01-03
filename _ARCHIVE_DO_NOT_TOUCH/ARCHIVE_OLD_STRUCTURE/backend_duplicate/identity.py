"""
Identity Stitching Engine
Merges anonymous sessions when users log in
"""
from typing import Optional

async def merge_anonymous_history_into_user(
    anonymous_session_id: str,
    user_id: int,
    conn
) -> int:
    """
    Merge all anonymous session activity into authenticated user profile
    Returns: number of events merged
    """
    try:
        cur = conn.cursor()
        
        # Update all events from anonymous session to user
        cur.execute("""
            UPDATE user_activity
            SET user_id = %s, merged_at = NOW()
            WHERE session_id = %s AND user_id IS NULL
        """, (user_id, anonymous_session_id))
        
        merged_count = cur.rowcount
        conn.commit()
        cur.close()
        
        return merged_count
    except Exception as e:
        conn.rollback()
        return 0


def get_unified_user_id(session_id: str, user_id: Optional[int], device_id: str) -> str:
    """
    Generate unified identifier for cross-device tracking
    Priority: user_id > session_id > device_id
    """
    if user_id:
        return f"user_{user_id}"
    elif session_id:
        return f"session_{session_id}"
    else:
        return f"device_{device_id}"
