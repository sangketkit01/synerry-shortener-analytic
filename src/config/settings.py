from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PORT: int = 8000
    ENVIRONMENT: str = "development"
    DATABASE_URL: str = "postgresql://postgres:postgres@localhost:5432/shorturl_analytics_db"
    BACKEND_INTERNAL_URL: str = "http://localhost:5000"
    INTERNAL_PIPELINE_KEY: str = "internal_pipeline_secret_key_32chars"
    CRON_HOUR: int = 22
    CRON_MINUTE: int = 0

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()
