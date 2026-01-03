import json
import hashlib
from app.utils.redis_client import redis_client

# ============================================================
# SEARCH RESULT CACHE (READ-THROUGH)
# ============================================================

DEFAULT_TTL = 60  # seconds

def _make_cache_key(prefix: str, payload: dict) -> str:
    raw = json.dumps(payload, sort_keys=True)
    digest = hashlib.sha256(raw.encode()).hexdigest()
    return f"{prefix}:{digest}"

def get_cached_search(payload: dict):
    key = _make_cache_key("search", payload)
    data = redis_client.get(key)
    if not data:
        return None
    return json.loads(data)

def set_cached_search(payload: dict, results, ttl: int = DEFAULT_TTL):
    key = _make_cache_key("search", payload)
    redis_client.setex(key, ttl, json.dumps(results, default=str))
