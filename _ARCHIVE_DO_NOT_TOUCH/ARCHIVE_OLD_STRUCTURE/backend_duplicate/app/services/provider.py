from app.repositories.provider import ProviderRepository
from app.services.cache import CacheService

class ProviderService:
    def __init__(self, repo: ProviderRepository, cache: CacheService):
        self.repo = repo
        self.cache = cache
    
    def get_all(self):
        return self.repo.get_all()
    
    def get_by_id(self, provider_id: int):
        return self.repo.get_by_id(provider_id)
    
    def get_by_service_type(self, service_type: str):
        return self.repo.get_by_service(service_type)
