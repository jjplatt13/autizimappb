from typing import List

from app.repositories.provider import ProviderRepository
from schemas.provider import Provider
from app.services.cache import CacheService


class SearchService:
    def __init__(
        self,
        repo: ProviderRepository,
        cache: CacheService,
    ):
        self.repo = repo
        self.cache = cache

    def basic_search(self, query: str, limit: int = 50) -> List[Provider]:
        results = self.repo.search_basic(query=query, limit=limit)
        return results[:limit]

    def fuzzy_search(self, query: str, limit: int = 50) -> List[Provider]:
        results = self.repo.search_fuzzy(query=query, limit=limit)
        return results[:limit]

    def nearby_search(
        self,
        lat: float,
        lon: float,
        radius_miles: int,
        limit: int = 50,
    ) -> List[Provider]:
        radius_meters = radius_miles * 1609.34
        results = self.repo.search_nearby(
            lat=lat,
            lon=lon,
            radius_meters=radius_meters,
            limit=limit,
        )
        return results[:limit]
