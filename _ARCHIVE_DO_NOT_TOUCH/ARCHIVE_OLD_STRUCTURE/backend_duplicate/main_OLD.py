# ============================================================
#   AUTIZIM Provider API — FULL MERGED VERSION (PART 1 OF 2)
# ============================================================

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
import math
from dotenv import load_dotenv
import os
import redis
import json
import time
import hashlib

# -------- RATE LIMITING ----------
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

# -------- SENTRY ----------
import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration


# ============================================================
# Load environment variables
# ============================================================
load_dotenv()

SECRET_ANALYTICS_SALT = os.getenv("ANALYTICS_SALT", "CHANGE_ME_SALT")


# ============================================================
#   SENTRY
# ============================================================
sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"),
    integrations=[FastApiIntegration(), SqlalchemyIntegration()],
    traces_sample_rate=1.0,
    profiles_sample_rate=1.0,
    send_default_pii=True,
    environment="production"
)

app = FastAPI(title="AUTIZIM Provider API")


# ============================================================
#   RATE LIMITER
# ============================================================
limiter = Limiter(key_func=get_remote_address, default_limits=["200/minute"])
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)


# ============================================================
#   DATABASE URL WITH PORT
# ============================================================
DATABASE_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=20,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=3600,
    pool_pre_ping=True
)


# ============================================================
#   REDIS
# ============================================================
redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True
)


# ============================================================
#   CORS (for React Native / Expo)
# ============================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
#   DB CONNECTION
# ============================================================
def get_db():
    return engine.raw_connection()


# ============================================================
#   ANALYTICS HELPERS
# ============================================================

def hash_ip(ip: str) -> str:
    if not ip:
        return None
    raw = ip + SECRET_ANALYTICS_SALT
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def log_search_event(query, result_count, lat, lon, radius, ip_hash, response_ms, device, source):
    """
    Push analytics event into Redis Stream (non-blocking).
    """
    try:
        event = {
            "event": "search",
            "query": query,
            "result_count": result_count,
            "lat": lat,
            "lon": lon,
            "radius": radius,
            "ip_hash": ip_hash,
            "response_time_ms": response_ms,
            "device": device,
            "source": source,
            "ts": int(time.time())
        }
        redis_client.xadd("analytics_stream", event)
    except Exception as e:
        sentry_sdk.capture_exception(e)


# ============================================================
#   MODELS
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
#   ROOT
# ============================================================
@app.get("/")
def root(request: Request):
    return {
        "message": "AUTIZIM Provider API",
        "cache": "Redis",
        "db_pool": "SQLAlchemy",
        "rate_limit": "200/min",
        "monitoring": "Sentry"
    }


# ============================================================
#   PROVIDER FILTERING (unchanged)
# ============================================================
@app.get("/providers/all", response_model=List[Provider])
def get_all_providers(request: Request):
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM providers ORDER BY id;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


@app.get("/providers/aba", response_model=List[Provider])
def get_aba(request: Request):
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM providers WHERE services ILIKE '%ABA%';")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


@app.get("/providers/speech", response_model=List[Provider])
def get_speech(request: Request):
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM providers WHERE services ILIKE '%Speech%';")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


@app.get("/providers/ot", response_model=List[Provider])
def get_ot(request: Request):
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT * FROM providers
        WHERE services ILIKE '%OT%' OR services ILIKE '%Occupational%';
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# ============================================================
#   BASIC SEARCH (analytics added)
# ============================================================
@app.get("/providers/search", response_model=List[Provider])
def basic_search(request: Request, query: str, limit: int = 50):

    start = time.time()
    term = f"%{query}%"

    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    sql = """
        SELECT *
        FROM providers
        WHERE name ILIKE %s
           OR city ILIKE %s
           OR services ILIKE %s
        LIMIT %s;
    """

    cur.execute(sql, (term, term, term, limit))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    response_ms = int((time.time() - start) * 1000)
    ip_hash = hash_ip(request.client.host)

    device = request.headers.get("User-Agent", "unknown")

    log_search_event(
        query=query,
        result_count=len(rows),
        lat=None,
        lon=None,
        radius=None,
        ip_hash=ip_hash,
        response_ms=response_ms,
        device=device,
        source="basic"
    )

    return rows


# ============================================================
#   FUZZY SEARCH (analytics added)
# ============================================================
@app.get("/providers/search_fuzzy", response_model=List[Provider])
def fuzzy_search(request: Request, q: str, limit: int = 50):

    start = time.time()

    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    sql = """
        SELECT *,
            greatest(
                similarity(name, %s),
                similarity(city, %s),
                similarity(services, %s)
            ) AS score
        FROM providers
        WHERE name % %s
           OR city % %s
           OR services % %s
        ORDER BY score DESC
        LIMIT %s;
    """

    cur.execute(sql, (q, q, q, q, q, q, limit))
    rows = cur.fetchall()

    cur.close()
    conn.close()

    response_ms = int((time.time() - start) * 1000)
    ip_hash = hash_ip(request.client.host)
    device = request.headers.get("User-Agent", "unknown")

    log_search_event(
        query=q,
        result_count=len(rows),
        lat=None,
        lon=None,
        radius=None,
        ip_hash=ip_hash,
        response_ms=response_ms,
        device=device,
        source="fuzzy"
    )

    return rows

# ============================================================
#   >>> END OF PART 1 — CONTINUE TO PART 2 <<<
# ============================================================
# ============================================================
#   NEARBY PROVIDERS (analytics added)
# ============================================================
@app.get("/providers/nearby", response_model=List[Provider])
def nearby(request: Request, lat: float, lon: float, radius: int = 25):

    start = time.time()
    cache_key = f"nearby:{lat}:{lon}:{radius}"

    # Try Redis cache
    try:
        cached = redis_client.get(cache_key)
        if cached:
            rows = json.loads(cached)

            # Analytics for cached response
            response_ms = int((time.time() - start) * 1000)
            ip_hash = hash_ip(request.client.host)
            device = request.headers.get("User-Agent", "unknown")

            log_search_event(
                query="__nearby__",
                result_count=len(rows),
                lat=lat,
                lon=lon,
                radius=radius,
                ip_hash=ip_hash,
                response_ms=response_ms,
                device=device,
                source="nearby"
            )

            return rows

    except Exception as e:
        sentry_sdk.capture_exception(e)

    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    radius_meters = radius * 1609.34

    sql = """
        SELECT id, name, phone, email, website, street, city, state, zip,
               full_address, latitude, longitude, services,
               ROUND(
                   CAST(
                       ST_Distance(
                           location,
                           ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                       ) / 1609.34
                   AS numeric), 2
               ) AS distance_miles
        FROM providers
        WHERE location IS NOT NULL
          AND ST_DWithin(
                location,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                %s
          )
        ORDER BY distance_miles ASC;
    """

    cur.execute(sql, (lon, lat, lon, lat, radius_meters))
    rows = cur.fetchall()

    cur.close()
    conn.close()

    # Cache new results
    try:
        redis_client.setex(cache_key, 3600, json.dumps(rows))
    except Exception as e:
        sentry_sdk.capture_exception(e)

    # Analytics logging
    response_ms = int((time.time() - start) * 1000)
    ip_hash = hash_ip(request.client.host)
    device = request.headers.get("User-Agent", "unknown")

    log_search_event(
        query="__nearby__",
        result_count=len(rows),
        lat=lat,
        lon=lon,
        radius=radius,
        ip_hash=ip_hash,
        response_ms=response_ms,
        device=device,
        source="nearby"
    )

    return rows


# ============================================================
#   GET PROVIDER BY ID (unchanged)
# ============================================================
@app.get("/providers/{provider_id}", response_model=Provider)
def get_provider(request: Request, provider_id: int):

    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT * FROM providers WHERE id = %s;", (provider_id,))
    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Provider not found")

    return row


# ============================================================
#   HEALTH CHECK (unchanged except for IP hash if used later)
# ============================================================
@app.get("/health")
def health(request: Request):

    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT COUNT(*) FROM providers;")
        provider_count = cur.fetchone()["count"]
        cur.close()
        conn.close()

        pool = engine.pool

        return {
            "status": "healthy",
            "providers": provider_count,
            "redis": "connected" if redis_client.ping() else "down",
            "pool": {
                "size": pool.size(),
                "checked_in": pool.checkedin(),
                "checked_out": pool.checkedout(),
                "overflow": pool.overflow(),
            },
            "sentry": "active"
        }

    except Exception as e:
        sentry_sdk.capture_exception(e)
        raise HTTPException(503, f"Health check failed: {e}")


# ============================================================
#   SENTRY TEST ROUTE
# ============================================================
@app.get("/sentry-debug")
def trigger_sentry():
    return 1 / 0


# ============================================================
#   RUN SERVER (unchanged)
# ============================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
