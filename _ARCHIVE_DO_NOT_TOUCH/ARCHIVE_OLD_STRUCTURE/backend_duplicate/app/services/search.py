import time
from app.repositories.provider import ProviderRepository
from app.services.cache import CacheService
from app.services.analytics import AnalyticsService

class SearchService:
    def __init__(self, repo: ProviderRepository, cache: CacheService, analytics: AnalyticsService):
        self.repo = repo
        self.cache = cache
        self.analytics = analytics
    
    def basic_search(self, query: str, limit: int, request):
        start = time.time()
        results = self.repo.search_basic(query, limit)
        response_ms = int((time.time() - start) * 1000)
        
        self.analytics.log_search(
            query, len(results), None, None, None,
            request.client.host, request.headers.get("User-Agent", "unknown"), "basic"
        )
        return results
    
    def fuzzy_search(self, query: str, limit: int, request):
        start = time.time()
        results = self.repo.search_fuzzy(query, limit)
        response_ms = int((time.time() - start) * 1000)
        
        self.analytics.log_search(
            query, len(results), None, None, None,
            request.client.host, request.headers.get("User-Agent", "unknown"), "fuzzy"
        )
        return results
    
    def nearby_search(self, lat: float, lon: float, radius: int, request):
        start = time.time()
        cache_key = f"nearby:{lat}:{lon}:{radius}"
        
        cached = self.cache.get(cache_key)
        if cached:
            self.analytics.log_search(
                "__nearby__", len(cached), lat, lon, radius,
                request.client.host, request.headers.get("User-Agent", "unknown"), "nearby"
            )
            return cached
        
        radius_meters = radius * 1609.34
        results = self.repo.get_nearby(lon, lat, radius_meters)
        self.cache.set(cache_key, results)
        
        response_ms = int((time.time() - start) * 1000)
        self.analytics.log_search(
            "__nearby__", len(results), lat, lon, radius,
            request.client.host, request.headers.get("User-Agent", "unknown"), "nearby"
        )
        return results
