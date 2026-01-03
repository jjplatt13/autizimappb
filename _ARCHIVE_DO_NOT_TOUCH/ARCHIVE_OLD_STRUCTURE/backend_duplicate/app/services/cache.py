import redis
import json
from app.core.config import get_settings

settings = get_settings()

class CacheService:
    def __init__(self):
        self.client = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            decode_responses=True
        )
    
    def get(self, key: str):
        try:
            cached = self.client.get(key)
            return json.loads(cached) if cached else None
        except:
            return None
    
    def set(self, key: str, value, ttl: int = 3600):
        try:
            self.client.setex(key, ttl, json.dumps(value))
        except:
            pass
    
    def ping(self):
        try:
            return self.client.ping()
        except:
            return False
