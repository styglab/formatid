from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache
from pathlib import Path
from pydantic import SecretStr, field_validator
import os

BASE_DIR = Path(__file__).resolve().parents[2]  # pipeline/
APP_ENV = os.getenv("APP_ENV", "dev")
ENV_FILE_MAP = {
    "dev": ".env.dev",
    "prod": ".env.prod",
}

class Settings(BaseSettings):
    # app_settings
    APP_ENV: str = APP_ENV
    APP_NAME: str = "formatid-pipeline"
    # db_settings
    DB_HOST: str
    DB_PORT: int = 5432
    DB_USER: str
    DB_PASSWORD: SecretStr
    DB_NAME: str
    # api_settings
    PUBLIC_API_KEY: str
    
    @property
    def DATABASE_URL(self) -> str:
        return (
            f"postgresql+asyncpg://{self.DB_USER}:"
            f"{self.DB_PASSWORD.get_secret_value()}@"
            f"{self.DB_HOST}:"
            f"{self.DB_PORT}/"
            f"{self.DB_NAME}"
        )
    
    @field_validator("PUBLIC_API_KEY")
    @classmethod
    def check_key(cls, v):
        if not v or v == "CHANGE_ME":
            raise ValueError("PUBLIC_API_KEY is not set or invalid")
        return v
    
    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ENV_FILE_MAP.get(APP_ENV, ".env.dev")), 
        env_file_encoding='utf-8', 
        case_sensitive=True
    )

@lru_cache
def get_settings():
    return Settings()