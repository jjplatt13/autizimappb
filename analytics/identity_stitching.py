"""
Identity Stitching - Merges anonymous histories when users log in
Location: analytics/identity_stitching.py
"""

from typing import Optional
from psycopg2.extras import RealDictCursor
from db.connection import get_db


async def merge_anonymous_history_into_user(
    anonymous_session_id: str,
    user_id: int,
    conn=None
) -> int:
    """
    Merge anonymous user_activity events into the authenticated user.

    Purpose:
        When a user logs in, their past anonymous session activity
        (searches, clicks, views) should be connected to their
        authenticated profile, giving you complete analytics.

    Args:
        anonymous_session_id: The session_id before login
        user_id: The authenticated user's ID
        conn: Optional database connection. If not provided, one is opened.

    Returns:
        Number of events that were merged.
    """

    # Allow external injections OR fallback to get_db()
    if conn is None:
        conn = get_db()

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Secure update – no SQL injection possible using parameters
        cur.execute(
            """
            UPDATE user_activity
            SET user_id = %s
            WHERE session_id = %s
              AND user_id IS NULL
            """,
            (user_id, anonymous_session_id)
        )

        merged_count = cur.rowcount

        conn.commit()
        cur.close()

        return merged_count

    except Exception as e:
        # Do NOT crash the app — log safely
        print(f"[Identity Stitching] Error merging history: {e}")
        return 0
