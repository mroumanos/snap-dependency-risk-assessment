"""Database settings used by the Postgres service.

This module provides a Pydantic `DatabaseSettings` helper that reads
database connection values from environment variables and exposes a
SQLAlchemy-compatible URI.

Usage example:

    from app.settings.db import postgres_settings
    engine = create_engine(postgres_settings.sqlalchemy_url)
"""
from pydantic_settings import BaseSettings
from pydantic import Field, SecretStr


class PostgresSettings(BaseSettings):
    """Pydantic settings for a Postgres DB connection."""

    host: str = Field("db", description="Database host (default: db for docker-compose)")
    port: int = Field(5432, description="Database port")
    db: str = Field(..., description="Database name")
    user: SecretStr = Field(..., description="Database user")
    password: SecretStr = Field(..., description="Database password")

    @property
    def sqlalchemy_url(self) -> str:
        """Return a SQLAlchemy-compatible database URL.

        Example: postgresql+psycopg2://user:pass@host:port/dbname
        """
        user = self.user.get_secret_value()
        password = self.password.get_secret_value()
        return f"postgresql+psycopg2://{user}:{password}@{self.host}:{self.port}/{self.db}"

    class Config:
        env_prefix = "postgres_"


postgres_settings = PostgresSettings()