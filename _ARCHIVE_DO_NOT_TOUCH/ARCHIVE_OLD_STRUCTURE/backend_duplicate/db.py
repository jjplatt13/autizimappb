# ============================================================
#   AUTIZIM BACKEND — Database Module (db.py)
#   Handles SQLAlchemy engine + connection pooling
# ============================================================

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

# Load environment variables
load_dotenv()

# ============================================================
#   DATABASE URL FROM .env
# ============================================================
DATABASE_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

# ============================================================
#   CREATE ENGINE WITH CONNECTION POOL
# ============================================================
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=20,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=3600,
    pool_pre_ping=True
)

# ============================================================
#   GET DB CONNECTION
# ============================================================
def get_db():
    """
    Returns a raw DB connection compatible with psycopg2.
    Matches your existing SQL execution style.
    """
    return engine.raw_connection()
