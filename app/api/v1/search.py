from psycopg2.extras import RealDictCursor
from db.connection import get_db
from fastapi import APIRouter, Request, Depends, Query
from typing import List
import json
import hashlib

from slowapi import Limiter
from slowapi.util import get_remote_address

from schemas.provider import Provider
from app.services.search import SearchService
from app.core.dependencies import get_search_service
from app.utils.redis_client import redis_client

# 🔹 Analytics (UNMET DEMAND + SEARCH INTELLIGENCE)
from app.api.v1.analytics import track_search_result, SearchResultEvent


router = APIRouter(prefix="/search", tags=["search"])
limiter = Limiter(key_func=get_remote_address)

# ============================================================
# REDIS SEARCH CACHE HELPERS (READ-THROUGH, FAIL-SAFE)
# ============================================================

SEARCH_CACHE_TTL = 60  # seconds

def _cache_key(prefix: str, payload: dict) -> str:
    raw = json.dumps(payload, sort_keys=True)
    digest = hashlib.sha256(raw.encode()).hexdigest()
    return f"{prefix}:{digest}"

def cache_get(prefix: str, payload: dict):
    try:
        key = _cache_key(prefix, payload)
        val = redis_client.get(key)
        if not val:
            return None
        return json.loads(val)
    except Exception:
        return None

def cache_set(prefix: str, payload: dict, results):
    try:
        key = _cache_key(prefix, payload)
        redis_client.setex(
            key,
            SEARCH_CACHE_TTL,
            json.dumps(results, default=str),
        )
    except Exception:
        pass


# ============================================================
# BASIC SEARCH
# ============================================================

@router.get("/basic", response_model=List[Provider])
@limiter.limit("100/minute")
async def basic(
    request: Request,
    q: str = Query(..., min_length=2, max_length=100),
    limit: int = Query(50, ge=1, le=100),
    service: SearchService = Depends(get_search_service),
):
    """
    Basic keyword search.
    Intended for fast, exact-ish matching.
    """
    cache_payload = {
        "q": q,
        "limit": limit,
    }

    cached = cache_get("search:basic", cache_payload)

    if cached is not None:
        results = cached
    else:
        results = service.basic_search(q, limit)
        cache_set("search:basic", cache_payload, results)

    # 🔹 ALWAYS track analytics (cached or not)
    await track_search_result(
        request,
        SearchResultEvent(
            query=q,
            city=None,
            state=None,
            radius_miles=None,
            results_count=len(results),
        )
    )

    return results


# ============================================================
# FUZZY SEARCH
# ============================================================

@router.get("/fuzzy", response_model=List[Provider])
@limiter.limit("100/minute")
async def fuzzy(
    request: Request,
    q: str = Query(..., min_length=2, max_length=100),
    limit: int = Query(50, ge=1, le=100),
    service: SearchService = Depends(get_search_service),
):
    """
    Fuzzy keyword search.
    Intended for misspellings and partial matches.
    """
    cache_payload = {
        "q": q,
        "limit": limit,
    }

    cached = cache_get("search:fuzzy", cache_payload)

    if cached is not None:
        results = cached
    else:
        results = service.fuzzy_search(q, limit)
        cache_set("search:fuzzy", cache_payload, results)

    # 🔹 ALWAYS track analytics
    await track_search_result(
        request,
        SearchResultEvent(
            query=q,
            city=None,
            state=None,
            radius_miles=None,
            results_count=len(results),
        )
    )

    return results


# ============================================================
# NEARBY (GEO) SEARCH
# ============================================================

@router.get("/nearby", response_model=List[Provider])
@limiter.limit("100/minute")
async def nearby(
    request: Request,
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    radius_miles: int = Query(25, ge=1, le=100),
    limit: int = Query(50, ge=1, le=100),
    service: SearchService = Depends(get_search_service),
):
    """
    Geo-based nearby search.
    """
    cache_payload = {
        "lat": lat,
        "lon": lon,
        "radius_miles": radius_miles,
        "limit": limit,
    }

    cached = cache_get("search:nearby", cache_payload)

    if cached is not None:
        results = cached
    else:
        results = service.nearby_search(
            lat=lat,
            lon=lon,
            radius_miles=radius_miles,
            limit=limit,
        )
        cache_set("search:nearby", cache_payload, results)

    # 🔹 ALWAYS track analytics (geo demand intelligence)
    await track_search_result(
        request,
        SearchResultEvent(
            query=None,
            city=None,
            state=None,
            radius_miles=radius_miles,
            results_count=len(results),
        )
    )

    return results
