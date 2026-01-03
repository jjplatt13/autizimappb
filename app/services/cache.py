# app/services/cache.py

from typing import Optional
import json
from app.utils.redis_client import redis_client


class CacheService:
    @staticmethod
    def get(key: str) -> Optional[dict]:
        try:
            value = redis_client.get(key)
            return json.loads(value) if value else None
        except Exception:
            return None

    @staticmethod
    def set(key: str, value, ttl: int = 60):
        try:
            redis_client.setex(key, ttl, json.dumps(value, default=str))
        except Exception:
            pass
