from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from app.core.config import get_settings


settings = get_settings()

engine = create_engine(
    settings.DATABASE_URL,
    poolclass=QueuePool,
    pool_size=20,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=3600,
    pool_pre_ping=True
)

def get_db():
    return engine.raw_connection()
