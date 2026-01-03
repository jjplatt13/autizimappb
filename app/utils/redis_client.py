import redis
import os

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

redis_client = redis.StrictRedis.from_url(REDIS_URL, decode_responses=True)
