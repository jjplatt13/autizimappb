from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager
from config.settings import DATABASE_URL

# ============================================================
# DATABASE ENGINE WITH CONNECTION POOLING
# ============================================================
# Production-safe configuration for ~10k active users.
# Includes defensive protection against connection leaks.
# ============================================================

engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=20,          # steady-state connections
    max_overflow=10,       # burst capacity
    pool_timeout=30,       # seconds to wait for a connection
    pool_recycle=1800,     # recycle connections every 30 min
    pool_pre_ping=True     # auto-heal stale connections
)

@contextmanager
def get_db():
    """
    Context-managed database connection.

    Guarantees:
    - Connection is ALWAYS returned to the pool
    - Prevents connection leaks
    - Safe for high-concurrency FastAPI usage
    """
    conn = engine.raw_connection()
    try:
        yield conn
    finally:
        conn.close()
