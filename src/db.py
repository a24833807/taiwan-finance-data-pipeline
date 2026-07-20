"""Database engine factory."""

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from config import get_database_config


def get_engine() -> Engine:
    """Create a SQLAlchemy engine for PostgreSQL."""
    config = get_database_config()
    connection_url = (
        "postgresql+psycopg2://"
        f"{config.user}:{config.password}"
        f"@{config.host}:{config.port}/{config.database}"
    )
    return create_engine(connection_url, pool_pre_ping=True)
