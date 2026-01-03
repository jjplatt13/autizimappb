from fastapi import APIRouter, HTTPException, Request
import sentry_sdk
from db.connection import get_db
from app.utils.redis_client import redis_client
from slowapi import Limiter
from slowapi.util import get_remote_address
from psycopg2.extras import RealDictCursor

router = APIRouter(tags=["health"])
limiter = Limiter(key_func=get_remote_address)

@router.get("/health")
@limiter.limit("500/minute")
async def health(request: Request):
    try:
        with get_db() as conn:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("SELECT COUNT(*) FROM providers")
            count = cur.fetchone()["count"]
            cur.close()
        
        # Test Redis connection
        redis_ping = redis_client.ping()
        
        return {
            "status": "healthy",
            "providers_count": count,
            "cache": "connected" if redis_ping else "disconnected",
            "monitoring": "Sentry active"
        }
    except Exception as e:
        sentry_sdk.capture_exception(e)
        raise HTTPException(503, f"Health check failed: {str(e)}")