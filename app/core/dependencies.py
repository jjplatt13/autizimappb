from functools import lru_cache

from app.repositories.provider import ProviderRepository
from app.services.search import SearchService
from app.services.cache import CacheService


# ============================================================
# REPOSITORIES
# ============================================================

@lru_cache()
def get_provider_repo() -> ProviderRepository:
    return ProviderRepository()


# ============================================================
# SERVICES
# ============================================================

@lru_cache()
def get_cache_service() -> CacheService:
    return CacheService()


@lru_cache()
def get_search_service() -> SearchService:
    return SearchService(
        repo=get_provider_repo(),
        cache=get_cache_service(),
    )
