import os
from dotenv import load_dotenv

load_dotenv()

# Database
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "autizim_app")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Redis
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# Sentry
SENTRY_DSN = os.getenv("SENTRY_DSN")

# Analytics
ANALYTICS_SALT = os.getenv("ANALYTICS_SALT", "CHANGE_ME_SALT")

# API Settings
API_TITLE = "AUTIZIM Provider API"
API_HOST = "0.0.0.0"
API_PORT = 8000
