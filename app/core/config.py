from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache

class Settings(BaseSettings):
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = "autizim_app"
    DB_USER: str = "postgres"
    DB_PASSWORD: str
    
    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    SENTRY_DSN: str | None = None
    ANALYTICS_SALT: str = "CHANGE_ME"
    API_V1_PREFIX: str = "/api/v1"
    PROJECT_NAME: str = "AUTIZIM Provider API"
    
    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=True,
        extra="ignore"
    )

@lru_cache()
def get_settings() -> Settings:
    return Settings()
