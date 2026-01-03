# ============================================================
#   AUTIZIM Provider API – Phase 2C (Production + Security)
#   FULL PRODUCTION MAIN.PY – Dual Logging + Rate Limiting
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
import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

# ✅ SECURITY - Rate limiting
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

# ============================================================
# IMPORTS – NORMALIZED & FIXED
# ============================================================

# DB
from db.connection import get_db, engine

# Redis
from app.utils.redis_client import redis_client

# Analytics / Services
from app.services.user_activity_service import log_event
from analytics.intent_model import score_intent
from analytics.identity_stitching import merge_anonymous_history_into_user
from analytics.personalization_engine import calculate_personalization_score

from app.api.v1.analytics import router as analytics_router
from app.api.v1.router import api_router

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
# FASTAPI APP + RATE LIMITING
# ============================================================
app = FastAPI(title="AUTIZIM Backend – Phase 2C (Protected)")

# ✅ SECURITY - Redis-backed rate limiter (persists across restarts)
limiter = Limiter(
    key_func=get_remote_address,
    storage_uri=os.getenv("REDIS_URL", "redis://localhost:6379")
)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

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

# ============================================================
# REGISTER ROUTERS
# ============================================================
app.include_router(analytics_router)
app.include_router(api_router)

# ============================================================
# SESSION + DEVICE HELPERS
# ============================================================
def get_or_create_session(request: Request, response: Response) -> str:
    session_id = request.cookies.get("session_id")
    if not session_id:
        session_id = str(uuid.uuid4())
        response.set_cookie("session_id", session_id, max_age=60 * 60 * 24 * 365)
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
    try:
        redis_status = "connected" if redis_client.ping() else "down"
    except Exception:
        redis_status = "down"
    
    return {
        "message": "AUTIZIM Backend (Phase 2C – Protected)",
        "redis": redis_status,
        "status": "OK"
    }

# ============================================================
# HONEYPOT ENDPOINTS (Scraper detection)
# ============================================================
@app.get("/admin")
@app.get("/api/admin")
@app.get("/wp-admin")
async def honeypot(request: Request):
    ip = request.client.host if request.client else "unknown"
    print(f"⚠️ SCRAPER DETECTED: {ip} - accessing honeypot endpoint")
    
    try:
        redis_client.xadd("security_stream", {
            "event": "honeypot_hit",
            "ip": ip,
            "endpoint": str(request.url),
            "ts": int(time.time())
        })
    except:
        pass
    
    raise HTTPException(status_code=404, detail="Not found")

# ============================================================
# BASIC SEARCH (MVP-SCOPED FREE TEXT SEARCH)
# ============================================================
@app.get("/providers/search", response_model=List[Provider])
@limiter.limit("30/minute")
async def search(request: Request, response: Response, query: str, limit: int = 50):
    
    session = get_or_create_session(request, response)
    device = get_device_id(request)
    
    ua = request.headers.get("user-agent", "").lower()
    if any(bot in ua for bot in ["python-requests", "curl", "scrapy", "bot", "crawler"]):
        ip = request.client.host if request.client else "unknown"
        print(f"⚠️ BOT BLOCKED: {ip} - User-Agent: {ua}")
        raise HTTPException(status_code=403, detail="Forbidden")
    
    if len(query.strip()) < 2:
        return []
    
    limit = min(limit, 50)
    start = time.time()

    with get_db() as conn:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        term = f"%{query}%"
        
        # ✅ MVP-SCOPED SEARCH (INTENT-FOCUSED)
        cur.execute("""
            SELECT
                id,
                name,
                services,
                street,
                city,
                state,
                zip,
                phone,
                website,
                latitude,
                longitude
            FROM providers
            WHERE
                services ILIKE %s
             OR name ILIKE %s
             OR city ILIKE %s
             OR state ILIKE %s
             OR zip ILIKE %s
            LIMIT %s;
        """, (term, term, term, term, term, limit))

        rows = cur.fetchall()
        cur.close()

    ms = int((time.time() - start) * 1000)

    metadata = {
        "query": query,
        "limit": limit,
        "result_count": len(rows),
        "response_ms": ms
    }
    intent = score_intent("search", metadata)

    await log_event(request, "search", metadata, intent, source="search")

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
# FUZZY SEARCH (Rate limited)
# ============================================================
@app.get("/providers/search_fuzzy", response_model=List[Provider])
@limiter.limit("30/minute")
async def search_fuzzy(request: Request, response: Response, q: str, limit: int = 50):

    session = get_or_create_session(request, response)
    device = get_device_id(request)
    start = time.time()

    with get_db() as conn:
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
        cur.close()

    ms = int((time.time() - start) * 1000)

    metadata = {
        "query": q,
        "limit": limit,
        "result_count": len(rows),
        "response_ms": ms
    }
    intent = score_intent("fuzzy_search", metadata)

    await log_event(request, "fuzzy_search", metadata, intent, source="search")

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
# NEARBY PROVIDERS (Rate limited)
# ============================================================
@app.get("/providers/nearby", response_model=List[Provider])
@limiter.limit("60/minute")
async def nearby(request: Request, response: Response, lat: float, lon: float, radius: int = 25):

    session = get_or_create_session(request, response)
    device = get_device_id(request)
    start = time.time()

    cache_key = f"nearby:{lat}:{lon}:{radius}"
    cached = redis_client.get(cache_key)

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

        await log_event(request, "nearby_search", metadata, intent, source="map")

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
        
    with get_db() as conn:
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
        cur.close()

    redis_client.setex(cache_key, 3600, json.dumps(rows, default=str))

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

    await log_event(request, "nearby_search", metadata, intent, source="map")

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
# PROVIDER BY ID (Rate limited)
# ============================================================
@app.get("/providers/{provider_id}", response_model=Provider)
@limiter.limit("120/minute")
async def get_provider(request: Request, response: Response, provider_id: int):

    session = get_or_create_session(request, response)
    device = get_device_id(request)

    with get_db() as conn:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM providers WHERE id = %s;", (provider_id,))
        row = cur.fetchone()
        cur.close()

    if not row:
        raise HTTPException(status_code=404, detail="Provider not found")

    metadata = {"provider_id": provider_id}
    intent = score_intent("provider_view", metadata)

    await log_event(
        request=request,
        event_type="provider_view",
        metadata=metadata,
        intent_score=intent,
        source="direct"
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
        with get_db() as conn:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("SELECT COUNT(*) FROM providers;")
            count = cur.fetchone()["count"]
            cur.close()

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
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
