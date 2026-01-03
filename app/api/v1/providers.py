from fastapi import APIRouter, HTTPException, Request, Response
from typing import List
import json
import time
from contextlib import closing

from psycopg2.extras import RealDictCursor

from db.connection import get_db
from schemas.provider import Provider
from app.utils.redis_client import redis_client

from app.services.user_activity_service import log_event
from analytics.intent_model import score_intent

router = APIRouter(prefix="/providers", tags=["providers"])


def get_or_create_session(request: Request, response: Response) -> str:
    session_id = request.cookies.get("session_id")
    if not session_id:
        import uuid
        session_id = str(uuid.uuid4())
        response.set_cookie("session_id", session_id, max_age=60 * 60 * 24 * 365)
    return session_id


def get_device_id(request: Request) -> str:
    import uuid
    ua = request.headers.get("user-agent", "")
    accept = request.headers.get("accept", "")
    ip = request.client.host if request.client else ""
    raw = f"{ua}|{accept}|{ip}"
    return uuid.uuid5(uuid.NAMESPACE_DNS, raw).hex


# -------------------------------------------------------------------
# ALL PROVIDERS
# -------------------------------------------------------------------
@router.get("/all", response_model=List[Provider])
async def get_all_providers(request: Request, response: Response):
    session = get_or_create_session(request, response)
    device = get_device_id(request)

    with get_db() as conn:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM providers ORDER BY id;")
        rows = cur.fetchall()

    metadata = {"result_count": len(rows)}
    intent = score_intent("provider_list", metadata)

    await log_event(
        request=request,
        event_type="provider_list",
        metadata=metadata,
        intent_score=intent,
        source="list",
    )

    redis_client.xadd(
        "analytics_stream",
        {
            "event": "provider_list",
            "result_count": len(rows),
            "session_id": session,
            "device_id": device,
            "ts": int(time.time()),
        },
    )

    return rows


# -------------------------------------------------------------------
# SEARCH PROVIDERS
# -------------------------------------------------------------------
@router.get("/search", response_model=List[Provider])
async def search_providers(
    request: Request,
    response: Response,
    query: str,
    limit: int = 50,
):
    session = get_or_create_session(request, response)
    device = get_device_id(request)
    start = time.time()

    term = f"%{query}%"

    with closing(next(get_db())) as conn:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT * FROM providers
            WHERE name ILIKE %s OR city ILIKE %s OR services ILIKE %s
            LIMIT %s;
            """,
            (term, term, term, limit),
        )
        rows = cur.fetchall()

    ms = int((time.time() - start) * 1000)

    metadata = {
        "query": query,
        "limit": limit,
        "result_count": len(rows),
        "response_ms": ms,
    }
    intent = score_intent("search", metadata)

    await log_event(request, "search", metadata, intent, source="search")

    redis_client.xadd(
        "analytics_stream",
        {
            "event": "search",
            "query": query,
            "result_count": len(rows),
            "response_ms": ms,
            "session_id": session,
            "device_id": device,
            "ts": int(time.time()),
        },
    )

    return rows


# -------------------------------------------------------------------
# NEARBY PROVIDERS (FIXED ROOT CAUSE)
# -------------------------------------------------------------------
@router.get("/nearby", response_model=List[Provider])
async def nearby_providers(
    request: Request,
    response: Response,
    lat: float,
    lon: float,
    radius: int = 25,
):
    session = get_or_create_session(request, response)
    device = get_device_id(request)
    start = time.time()

    cache_key = f"nearby:{lat}:{lon}:{radius}"
    cached = redis_client.get(cache_key)

    if cached:
        return json.loads(cached)

    radius_meters = radius * 1609.34

    with get_db() as conn:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT id, name, phone, email, website, street, city, state, zip,
                   full_address, latitude, longitude, services,
                   ROUND(
                       CAST(
                           ST_Distance(
                               location,
                               ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                           ) / 1609.34 AS numeric
                       ), 2
                   ) AS distance_miles
            FROM providers
            WHERE location IS NOT NULL
            AND ST_DWithin(
                location,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                %s
            )
            ORDER BY distance_miles ASC;
            """,
            (lon, lat, lon, lat, radius_meters),
        )

        rows = cur.fetchall()

    redis_client.setex(cache_key, 3600, json.dumps(rows, default=str))

    metadata = {
        "lat": lat,
        "lon": lon,
        "radius": radius,
        "result_count": len(rows),
        "response_ms": int((time.time() - start) * 1000),
    }
    intent = score_intent("nearby_search", metadata)

    await log_event(request, "nearby_search", metadata, intent, source="map")

    return rows


# -------------------------------------------------------------------
# PROVIDER BY ID
# -------------------------------------------------------------------
@router.get("/{provider_id}", response_model=Provider)
async def get_provider_by_id(
    request: Request,
    response: Response,
    provider_id: int,
):
    session = get_or_create_session(request, response)
    device = get_device_id(request)

    with closing(next(get_db())) as conn:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM providers WHERE id = %s;", (provider_id,))
        row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Provider not found")

    metadata = {"provider_id": provider_id}
    intent = score_intent("provider_view", metadata)

    await log_event(
        request=request,
        event_type="provider_view",
        metadata=metadata,
        intent_score=intent,
        source="direct",
    )

    redis_client.xadd(
        "analytics_stream",
        {
            "event": "provider_view",
            "provider_id": provider_id,
            "session_id": session,
            "device_id": device,
            "ts": int(time.time()),
        },
    )

    return row
