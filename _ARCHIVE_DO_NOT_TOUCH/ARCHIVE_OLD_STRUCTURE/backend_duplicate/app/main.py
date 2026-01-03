# ============================================================
#   AUTIZIM Provider API — Phase 2C (Correct Async Version)
#   FULL PRODUCTION MAIN.PY — Dual Logging (Postgres + Redis)
# ============================================================

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import json
import time
import uuid
import hashlib
import os

from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
import redis
import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

# DB + Analytics
from db import get_db, engine
from services.user_activity_service import log_event
from analytics.intent_model import score_intent
from analytics.identity_stitching import merge_anonymous_history_into_user
from analytics.personalization_engine import calculate_personalization_score

# Analytics Endpoints (NEW)
from analytics_endpoints import router as analytics_router

# ============================================================
# ENV & SECURITY
# ============================================================
load_dotenv()
SECRET_ANALYTICS_SALT = os.getenv("ANALYTICS_SALT", "CHANGE_ME_SALT")

# ============================================================
# SENTRY INIT
# ============================================================
sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"),
    integrations=[FastApiIntegration(), SqlalchemyIntegration()],
    traces_sample_rate=1.0,
    profiles_sample_rate=1.0,
    send_default_pii=True,
)

# ============================================================
# FASTAPI APP
# ============================================================
app = FastAPI(title="AUTIZIM Backend — Phase 2C")

# ============================================================
# CORS
# ============================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register Analytics Router (NEW)
app.include_router(analytics_router)

# ============================================================
# REDIS
# ============================================================
redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True
)

# ============================================================
# SESSION + DEVICE HELPERS
# ============================================================
def get_or_create_session(request: Request, response: Response) -> str:
    session_id = request.cookies.get("session_id")
    if not session_id:
        session_id = str(uuid.uuid4())
        response.set_cookie("session_id", session_id, max_age=60*60*24*365)
    return session_id

def get_device_id(request: Request) -> str:
    ua = request.headers.get("user-agent", "")
    accept = request.headers.get("accept", "")
    ip = request.client.host if request.client else ""
    raw = f"{ua}|{accept}|{ip}"
    return uuid.uuid5(uuid.NAMESPACE_DNS, raw).hex

def hash_ip(ip: str) -> Optional[str]:
    if not ip:
        return None
    return hashlib.sha256((ip + SECRET_ANALYTICS_SALT).encode()).hexdigest()

# ============================================================
# PROVIDER MODEL
# ============================================================
class Provider(BaseModel):
    id: int
    name: str
    phone: Optional[str]
    email: Optional[str]
    website: Optional[str]
    street: Optional[str]
    city: Optional[str]
    state: Optional[str]
    zip: Optional[str]
    full_address: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    services: Optional[str]
    distance_miles: Optional[float] = None

# ============================================================
# ROOT ROUTE
# ============================================================
@app.get("/")
async def root():
    return {
        "message": "AUTIZIM Backend (Phase 2C — Async Analytics)",
        "redis": "connected" if redis_client.ping() else "down",
        "status": "OK"
    }

# ============================================================
# GET ALL PROVIDERS
# ============================================================
@app.get("/providers/all", response_model=List[Provider])
async def get_all_providers(request: Request, response: Response):

    session = get_or_create_session(request, response)
    device = get_device_id(request)

    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM providers ORDER BY id;")
    rows = cur.fetchall()
    conn.close()

    metadata = {"result_count": len(rows)}
    intent = score_intent("provider_list", metadata)

    # Postgres analytics
    await log_event(
        request=request,
        event_type="provider_list",
        metadata=metadata,
        intent_score=intent
    )

    # Redis analytics
    redis_client.xadd("analytics_stream", {
        "event": "provider_list",
        "result_count": len(rows),
        "session_id": session,
        "device_id": device,
        "ts": int(time.time())
    })

    return rows

# ============================================================
# BASIC SEARCH
# ============================================================
@app.get("/providers/search", response_model=List[Provider])
async def search(request: Request, response: Response, query: str, limit: int = 50):

    session = get_or_create_session(request, response)
    device = get_device_id(request)

    start = time.time()

    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    term = f"%{query}%"
    cur.execute("""
        SELECT * FROM providers
        WHERE name ILIKE %s OR city ILIKE %s OR services ILIKE %s
        LIMIT %s;
    """, (term, term, term, limit))

    rows = cur.fetchall()
    conn.close()

    ms = int((time.time() - start) * 1000)

    metadata = {
        "query": query,
        "limit": limit,
        "result_count": len(rows),
        "response_ms": ms
    }
    intent = score_intent("search", metadata)

    await log_event(
        request=request,
        event_type="search",
        metadata=metadata,
        intent_score=intent,
    )

    redis_client.xadd("analytics_stream", {
        "event": "search",
        "query": query,
        "result_count": len(rows),
        "response_ms": ms,
        "session_id": session,
        "device_id": device,
        "ts": int(time.time())
    })

    return rows

# ============================================================
# FUZZY SEARCH
# ============================================================
@app.get("/providers/search_fuzzy", response_model=List[Provider])
async def search_fuzzy(request: Request, response: Response, q: str, limit: int = 50):

    session = get_or_create_session(request, response)
    device = get_device_id(request)
    start = time.time()

    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        SELECT *,
        greatest(similarity(name, %s),
                 similarity(city, %s),
                 similarity(services, %s)) AS score
        FROM providers
        WHERE name % %s OR city % %s OR services % %s
        ORDER BY score DESC
        LIMIT %s;
    """, (q, q, q, q, q, q, limit))

    rows = cur.fetchall()
    conn.close()

    ms = int((time.time() - start) * 1000)

    metadata = {
        "query": q,
        "limit": limit,
        "result_count": len(rows),
        "response_ms": ms
    }
    intent = score_intent("fuzzy_search", metadata)

    await log_event(
        request=request,
        event_type="fuzzy_search",
        metadata=metadata,
        intent_score=intent,
    )

    redis_client.xadd("analytics_stream", {
        "event": "fuzzy_search",
        "query": q,
        "result_count": len(rows),
        "response_ms": ms,
        "session_id": session,
        "device_id": device,
        "ts": int(time.time())
    })

    return rows

# ============================================================
# NEARBY PROVIDERS
# ============================================================
@app.get("/providers/nearby", response_model=List[Provider])
async def nearby(request: Request, response: Response, lat: float, lon: float, radius: int = 25):

    session = get_or_create_session(request, response)
    device = get_device_id(request)
    start = time.time()

    cache_key = f"nearby:{lat}:{lon}:{radius}"
    cached = redis_client.get(cache_key)

    # -------- Cached Response --------
    if cached:
        rows = json.loads(cached)

        metadata = {
            "lat": lat,
            "lon": lon,
            "radius": radius,
            "cached": True,
            "result_count": len(rows)
        }
        intent = score_intent("nearby_search", metadata)

        await log_event(
            request=request,
            event_type="nearby_search",
            metadata=metadata,
            intent_score=intent
        )

        redis_client.xadd("analytics_stream", {
            "event": "nearby_search",
            "cached": True,
            "lat": lat,
            "lon": lon,
            "radius": radius,
            "result_count": len(rows),
            "session_id": session,
            "device_id": device,
            "ts": int(time.time())
        })

        return rows

    # -------- Database Query --------
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    radius_meters = radius * 1609.34

    cur.execute("""
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
    """, (lon, lat, lon, lat, radius_meters))

    rows = cur.fetchall()
    conn.close()

    redis_client.setex(cache_key, 3600, json.dumps(rows))

    ms = int((time.time() - start) * 1000)

    metadata = {
        "lat": lat,
        "lon": lon,
        "radius": radius,
        "cached": False,
        "result_count": len(rows),
        "response_ms": ms
    }
    intent = score_intent("nearby_search", metadata)

    await log_event(
        request=request,
        event_type="nearby_search",
        metadata=metadata,
        intent_score=intent
    )

    redis_client.xadd("analytics_stream", {
        "event": "nearby_search",
        "cached": False,
        "lat": lat,
        "lon": lon,
        "radius": radius,
        "result_count": len(rows),
        "response_ms": ms,
        "session_id": session,
        "device_id": device,
        "ts": int(time.time())
    })

    return rows

# ============================================================
# PROVIDER BY ID
# ============================================================
@app.get("/providers/{provider_id}", response_model=Provider)
async def get_provider(request: Request, response: Response, provider_id: int):

    session = get_or_create_session(request, response)
    device = get_device_id(request)

    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM providers WHERE id = %s;", (provider_id,))
    row = cur.fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Provider not found")

    metadata = {"provider_id": provider_id}
    intent = score_intent("provider_view", metadata)

    await log_event(
        request=request,
        event_type="provider_view",
        metadata=metadata,
        intent_score=intent
    )

    redis_client.xadd("analytics_stream", {
        "event": "provider_view",
        "provider_id": provider_id,
        "session_id": session,
        "device_id": device,
        "ts": int(time.time())
    })

    return row

# ============================================================
# HEALTH CHECK
# ============================================================
@app.get("/health")
async def health():
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT COUNT(*) FROM providers;")
        count = cur.fetchone()["count"]
        conn.close()

        return {"status": "healthy", "providers": count}
    except Exception as e:
        sentry_sdk.capture_exception(e)
        raise HTTPException(503, f"Health check failed: {e}")

# ============================================================
# SENTRY DEBUG
# ============================================================
@app.get("/sentry-debug")
async def trigger_sentry():
    return 1 / 0

# ============================================================
# UVICORN ENTRYPOINT
# ============================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
