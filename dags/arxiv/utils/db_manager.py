"""Shared database manager for arXiv paper processing."""

from utility_hub import DatabaseConnection, PandasDataOperations
from .config import DatabaseConfig


# Singleton database connections
DB_CONNECTION = DatabaseConnection(DatabaseConfig.get_connection_id())
DB_MANAGER = PandasDataOperations(DB_CONNECTION)


def get_db_connection():
    """Get the shared database connection."""
    return DB_CONNECTION


def get_db_manager():
    """Get the shared database manager."""
    return DB_MANAGER