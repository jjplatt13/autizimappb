"""
Analytics API Endpoints for Provider Dashboard
Revenue tracking + provider intelligence + unmet demand detection
Location: app/api/v1/analytics.py
"""

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from typing import Dict, Optional, List, Any
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor
import time
import json

from db.connection import get_db
from app.services.user_activity_service import log_event
from app.utils.redis_client import redis_client  # ✅ centralized

router = APIRouter(prefix="/analytics", tags=["analytics"])

# ============================================================
# CACHE HELPERS
# ============================================================

CACHE_TTL = 30  # seconds

def cache_get(key: str):
    try:
        val = redis_client.get(key)
        return json.loads(val) if val else None
    except Exception:
        return None

def cache_set(key: str, payload):
    try:
        redis_client.setex(key, CACHE_TTL, json.dumps(payload, default=str))
    except Exception:
        pass

# ============================================================
# MODELS
# ============================================================

class ClickEvent(BaseModel):
    provider_id: int
    click_type: str  # phone | website | email


class ConversionEvent(BaseModel):
    provider_id: int
    event_type: str  # provider_phone_click | provider_website_click | provider_email_click
    metadata: Optional[Dict] = {}


class SearchResultEvent(BaseModel):
    query: Optional[str]
    city: Optional[str]
    state: Optional[str]
    radius_miles: Optional[int]
    results_count: int

# ============================================================
# CONSTANTS
# ============================================================

ALLOWED_CLICK_TYPES = {"phone", "website", "email"}

ALLOWED_CONVERSION_EVENTS = {
    "provider_phone_click",
    "provider_website_click",
    "provider_email_click",
}

LOW_RESULT_THRESHOLD = 2  # <= this means underserved

# ============================================================
# INTERNAL: COLUMN DETECTION (provider_stats + user_activity)
# ============================================================

_PROVIDER_STATS_COLS: Optional[set] = None
_USER_ACTIVITY_COLS: Optional[set] = None

def _load_table_cols(table_name: str) -> set:
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
    """, (table_name,))
    cols = {r["column_name"] for r in cur.fetchall()}
    conn.close()
    return cols

def provider_stats_cols() -> set:
    global _PROVIDER_STATS_COLS
    if _PROVIDER_STATS_COLS is None:
        _PROVIDER_STATS_COLS = _load_table_cols("provider_stats")
    return _PROVIDER_STATS_COLS

def user_activity_cols() -> set:
    global _USER_ACTIVITY_COLS
    if _USER_ACTIVITY_COLS is None:
        _USER_ACTIVITY_COLS = _load_table_cols("user_activity")
    return _USER_ACTIVITY_COLS

def _activity_time_col() -> str:
    cols = user_activity_cols()
    if "timestamp" in cols:
        return "timestamp"
    if "created_at" in cols:
        return "created_at"
    return "timestamp"

# ============================================================
# CLICK TRACKING (LEGACY + COMPATIBLE)
# ============================================================

@router.post("/track/click")
async def track_click(request: Request, event: ClickEvent):
    if event.click_type not in ALLOWED_CLICK_TYPES:
        raise HTTPException(400, "Invalid click_type")

    session_id = request.cookies.get("session_id", "unknown")
    event_type = f"provider_{event.click_type}_click"

    redis_client.xadd("analytics_stream", {
        "event": event_type,
        "provider_id": event.provider_id,
        "session_id": session_id,
        "high_value": 1,
        "ts": int(time.time()),
        "search_query": request.headers.get("X-Search-Query", ""),
        "city": request.headers.get("X-City", ""),
        "state": request.headers.get("X-State", ""),
        "source": "click",
    })

    await log_event(
        request=request,
        event_type=event_type,
        metadata={"provider_id": event.provider_id},
        intent_score=1.0
    )

    return {"status": "tracked", "event": event_type}

# ============================================================
# EXPLICIT CONVERSION EVENTS (PREFERRED)
# ============================================================

@router.post("/track/conversion")
async def track_conversion(request: Request, payload: ConversionEvent):
    if payload.event_type not in ALLOWED_CONVERSION_EVENTS:
        raise HTTPException(400, "Invalid conversion event")

    session_id = request.cookies.get("session_id", "unknown")

    redis_client.xadd("analytics_stream", {
        "event": payload.event_type,
        "provider_id": payload.provider_id,
        "session_id": session_id,
        "high_value": 1,
        "ts": int(time.time()),
        "source": "conversion",
        **(payload.metadata or {}),
    })

    await log_event(
        request=request,
        event_type=payload.event_type,
        metadata={
            "provider_id": payload.provider_id,
            **(payload.metadata or {})
        },
        intent_score=1.0
    )

    return {"status": "ok", "event": payload.event_type}

# ============================================================
# SEARCH RESULT TRACKING (UNMET DEMAND)
# ============================================================

@router.post("/track/search_result")
async def track_search_result(request: Request, payload: SearchResultEvent):
    session_id = request.cookies.get("session_id", "unknown")

    unmet = payload.results_count == 0
    low_supply = payload.results_count <= LOW_RESULT_THRESHOLD

    event_type = "search_unmet" if unmet else "search_low_supply" if low_supply else "search_satisfied"

    await log_event(
        request=request,
        event_type=event_type,
        metadata={
            "query": payload.query,
            "city": payload.city,
            "state": payload.state,
            "radius_miles": payload.radius_miles,
            "results_count": payload.results_count
        },
        intent_score=1.5 if unmet else 1.2 if low_supply else 0.5
    )

    try:
        redis_client.xadd("analytics_stream", {
            "event": event_type,
            "session_id": session_id,
            "query": payload.query or "",
            "city": payload.city or "",
            "state": payload.state or "",
            "radius_miles": payload.radius_miles or 0,
            "results_count": payload.results_count,
            "ts": int(time.time()),
            "source": "search_result",
        })
    except Exception:
        pass

    return {
        "status": "tracked",
        "event": event_type,
        "results_count": payload.results_count
    }

# ============================================================
# READ: PROVIDER STATS (REVENUE READY)
# ============================================================

@router.get("/provider/{provider_id}/stats")
async def get_provider_stats(provider_id: int, days: int = 30):
    cache_key = f"analytics:provider:{provider_id}:stats:{days}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    pcols = provider_stats_cols()
    tcol = _activity_time_col()
    cutoff = datetime.now() - timedelta(days=days)

    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT name FROM providers WHERE id = %s", (provider_id,))
    provider = cur.fetchone()
    if not provider:
        conn.close()
        raise HTTPException(404, "Provider not found")

    if {"total_views", "total_phone_clicks", "total_website_clicks"}.issubset(pcols):
        cur.execute(f"""
            SELECT
                total_views AS views,
                total_phone_clicks AS phone_clicks,
                total_website_clicks AS website_clicks,
                ROUND(
                    (total_phone_clicks + total_website_clicks)::numeric
                    / NULLIF(total_views, 0) * 100, 2
                ) AS conversion_rate,
                {("last_updated" if "last_updated" in pcols else "NULL")} AS last_updated
            FROM provider_stats
            WHERE provider_id = %s
        """, (provider_id,))
        stats_row = cur.fetchone() or {
            "views": 0,
            "phone_clicks": 0,
            "website_clicks": 0,
            "conversion_rate": 0.0,
            "last_updated": None,
        }
    else:
        cur.execute("""
            SELECT views, searches, conversions, last_event_at
            FROM provider_stats
            WHERE provider_id = %s
        """, (provider_id,))
        row = cur.fetchone() or {}
        views = int(row.get("views") or 0)
        conversions = int(row.get("conversions") or 0)
        stats_row = {
            "views": views,
            "phone_clicks": None,
            "website_clicks": None,
            "conversion_rate": round((conversions / views * 100), 2) if views else 0.0,
            "last_updated": row.get("last_event_at"),
            "searches": int(row.get("searches") or 0),
            "conversions": conversions,
        }

    cur.execute(f"""
        SELECT event_type, COUNT(*) AS count
        FROM user_activity
        WHERE provider_id = %s
          AND {tcol} >= %s
        GROUP BY event_type
        ORDER BY count DESC
    """, (provider_id, cutoff))
    breakdown = cur.fetchall()

    conn.close()

    payload = {
        "provider_id": provider_id,
        "provider_name": provider["name"],
        "period_days": days,
        "stats": stats_row,
        "breakdown": breakdown,
    }

    cache_set(cache_key, payload)
    return payload

# ============================================================
# READ: TOP PROVIDERS
# ============================================================

@router.get("/providers/top")
async def providers_top(limit: int = 25):
    if limit < 1 or limit > 200:
        raise HTTPException(400, "limit must be between 1 and 200")

    cache_key = f"analytics:providers:top:{limit}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    pcols = provider_stats_cols()
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    if {"total_views", "total_phone_clicks", "total_website_clicks"}.issubset(pcols):
        cur.execute("""
            SELECT
                p.id,
                p.name,
                p.city,
                p.state,
                p.services,
                ps.total_views AS views,
                ps.total_phone_clicks AS phone_clicks,
                ps.total_website_clicks AS website_clicks,
                ROUND(
                    (ps.total_phone_clicks + ps.total_website_clicks)::numeric
                    / NULLIF(ps.total_views, 0) * 100, 2
                ) AS conversion_rate,
                ps.last_updated
            FROM provider_stats ps
            JOIN providers p ON p.id = ps.provider_id
            ORDER BY ps.total_views DESC, (ps.total_phone_clicks + ps.total_website_clicks) DESC
            LIMIT %s
        """, (limit,))
    else:
        cur.execute("""
            SELECT
                p.id,
                p.name,
                p.city,
                p.state,
                p.services,
                ps.views,
                ps.conversions,
                ROUND(ps.conversions::numeric / NULLIF(ps.views, 0) * 100, 2) AS conversion_rate,
                ps.last_event_at
            FROM provider_stats ps
            JOIN providers p ON p.id = ps.provider_id
            ORDER BY ps.views DESC, ps.conversions DESC
            LIMIT %s
        """, (limit,))

    rows = cur.fetchall()
    conn.close()

    payload = {"limit": limit, "items": rows}
    cache_set(cache_key, payload)
    return payload

# ============================================================
# UNMET DEMAND REPORT
# ============================================================

@router.get("/unmet-demand")
async def get_unmet_demand(days: int = 30):
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cutoff = datetime.now() - timedelta(days=days)
    tcol = _activity_time_col()

    cur.execute(f"""
        SELECT
            metadata->>'query' AS query,
            metadata->>'city' AS city,
            metadata->>'state' AS state,
            COUNT(*) AS searches
        FROM user_activity
        WHERE event_type IN ('search_unmet', 'search_low_supply')
          AND {tcol} >= %s
        GROUP BY query, city, state
        ORDER BY searches DESC
        LIMIT 50
    """, (cutoff,))

    results = cur.fetchall()
    conn.close()

    return {"period_days": days, "hot_unmet_searches": results}

# ============================================================
# SYSTEM OVERVIEW (EXISTING)
# ============================================================

@router.get("/overview")
async def get_overview(days: int = 7):
    cache_key = f"analytics:overview:{days}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cutoff = datetime.now() - timedelta(days=days)
    tcol = _activity_time_col()

    cur.execute(f"""
        SELECT event_type, COUNT(*) AS count
        FROM user_activity
        WHERE {tcol} >= %s
        GROUP BY event_type
    """, (cutoff,))
    events = {r["event_type"]: r["count"] for r in cur.fetchall()}

    cur.execute(f"""
        SELECT COUNT(DISTINCT session_id) AS sessions
        FROM user_activity
        WHERE {tcol} >= %s
    """, (cutoff,))
    sessions = cur.fetchone()["sessions"]

    conn.close()

    payload = {"period_days": days, "sessions": sessions, "events": events}
    cache_set(cache_key, payload)
    return payload

# ============================================================
# 🆕 ADDITIVE: TIME-WINDOWED OVERVIEW (NO BREAKING CHANGES)
# ============================================================

def _window_start(window: str) -> datetime:
    now = datetime.utcnow()
    return {
        "hour": now - timedelta(hours=1),
        "day": now - timedelta(days=1),
        "week": now - timedelta(days=7),
        "month": now - timedelta(days=30),
        "year": now - timedelta(days=365),
    }.get(window)

@router.get("/overview/window/{window}")
async def get_overview_window(window: str):
    if window not in {"hour", "day", "week", "month", "year"}:
        raise HTTPException(400, "Invalid window")

    start = _window_start(window)
    tcol = _activity_time_col()
    cache_key = f"analytics:overview:window:{window}"

    cached = cache_get(cache_key)
    if cached:
        return cached

    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute(f"""
        SELECT event_type, COUNT(*) AS count
        FROM user_activity
        WHERE {tcol} >= %s
        GROUP BY event_type
    """, (start,))
    events = {r["event_type"]: r["count"] for r in cur.fetchall()}

    cur.execute(f"""
        SELECT COUNT(DISTINCT session_id) AS sessions
        FROM user_activity
        WHERE {tcol} >= %s
    """, (start,))
    sessions = cur.fetchone()["sessions"]

    conn.close()

    payload = {
        "window": window,
        "since": start.isoformat(),
        "sessions": sessions,
        "events": events,
    }

    cache_set(cache_key, payload)
    return payload
