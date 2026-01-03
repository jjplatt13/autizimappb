import time
import hashlib
import sentry_sdk
from app.services.cache import CacheService
from app.core.config import get_settings

settings = get_settings()

class AnalyticsService:
    def __init__(self):
        self.cache = CacheService()
    
    def hash_ip(self, ip: str) -> str:
        if not ip:
            return None
        return hashlib.sha256((ip + settings.ANALYTICS_SALT).encode()).hexdigest()
    
    def log_search(self, query, result_count, lat, lon, radius, ip, device, source):
        try:
            event = {
                "event": "search",
                "query": query,
                "result_count": result_count,
                "lat": lat or "",
                "lon": lon or "",
                "radius": radius or "",
                "ip_hash": self.hash_ip(ip),
                "device": device,
                "source": source,
                "ts": int(time.time())
            }
            self.cache.client.xadd("analytics_stream", event)
        except Exception as e:
            sentry_sdk.capture_exception(e)
