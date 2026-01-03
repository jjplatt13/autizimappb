from fastapi import APIRouter
from app.api.v1 import providers, search, health, analytics

api_router = APIRouter()
api_router.include_router(providers.router)
api_router.include_router(search.router)
api_router.include_router(health.router)
api_router.include_router(analytics.router)
