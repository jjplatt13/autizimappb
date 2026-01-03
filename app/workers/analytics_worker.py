"""
Analytics Worker
- Consumes Redis analytics_stream
- Persists events to Postgres (user_activity)
- Updates provider_stats for dashboards + monetization
- Preserves full event metadata for attribution & geo demand
"""

import json
import time
import os
from typing import Dict, Any

import redis
from psycopg2.extras import Json

from db.connection import get_db

# ============================================================
# REDIS CONFIG
# ============================================================

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

STREAM = "analytics_stream"
GROUP = "analytics_group"
CONSUMER = "analytics_worker_1"

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True
)

# ============================================================
# REDIS GROUP INIT
# ============================================================

def ensure_consumer_group():
    try:
        redis_client.xgroup_create(
            STREAM,
            GROUP,
            id="0",
            mkstream=True
        )
    except redis.exceptions.ResponseError:
        pass  # group already exists


# ============================================================
# DB: USER ACTIVITY (FULL EVENT STORAGE)
# ============================================================

def persist_user_activity(event: Dict[str, Any]) -> None:
    """
    Stores the FULL event payload in metadata.
    This enables:
    - search → click attribution
    - city / state demand analysis
    - keyword monetization
    - future trend queries (7d / 30d)
    """

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO user_activity (
            event_type,
            provider_id,
            session_id,
            device_id,
            ip_hash,
            source,
            metadata,
            timestamp
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
    """, (
        event.get("event") or event.get("event_type"),
        event.get("provider_id"),
        event.get("session_id"),
        event.get("device_id"),
        event.get("ip_hash"),
        event.get("source"),
        Json(event)  # 🔑 FULL EVENT STORED (search_query, city, state, etc.)
    ))

    conn.commit()
    conn.close()


# ============================================================
# DB: PROVIDER STATS (REVENUE SIGNALS)
# ============================================================

def update_provider_stats(event: Dict[str, Any]) -> None:
    event_type = event.get("event") or event.get("event_type") or ""
    provider_id_raw = event.get("provider_id")

    if provider_id_raw in (None, "", "null"):
        return

    try:
        provider_id = int(provider_id_raw)
    except Exception:
        return

    # Core revenue + engagement signals
    views_inc = 1 if event_type == "provider_view" else 0
    phone_inc = 1 if event_type in ("provider_phone_click", "phone_click") else 0
    website_inc = 1 if event_type in ("provider_website_click", "website_click") else 0

    if views_inc == 0 and phone_inc == 0 and website_inc == 0:
        return

    conversions_inc = phone_inc + website_inc

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO provider_stats (
            provider_id,
            views,
            searches,
            conversions,
            last_event_at
        )
        VALUES (%s, %s, 0, %s, NOW())
        ON CONFLICT (provider_id) DO UPDATE SET
            views = provider_stats.views + EXCLUDED.views,
            conversions = provider_stats.conversions + EXCLUDED.conversions,
            last_event_at = NOW()
    """, (
        provider_id,
        views_inc,
        conversions_inc
    ))

    conn.commit()
    conn.close()


# ============================================================
# MAIN WORKER LOOP
# ============================================================

def run():
    ensure_consumer_group()
    print("📊 Analytics worker running...")

    while True:
        messages = redis_client.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={STREAM: ">"},
            count=10,
            block=5000
        )

        if not messages:
            continue

        for _, entries in messages:
            for message_id, event in entries:
                try:
                    persist_user_activity(event)
                    update_provider_stats(event)
                    redis_client.xack(STREAM, GROUP, message_id)
                except Exception as e:
                    print(f"✗ Analytics worker error: {e}")


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    run()
