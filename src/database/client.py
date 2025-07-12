"""Main TimescaleDB client coordinator."""
import logging
from typing import Dict

import psycopg2.pool

from config_loader import DatabaseConfig
from .market_data import MarketDataClient
from .failed_download import FailedDownloadsClient
from .data_quality import DataQualityClient


class TimescaleDBClient:
    """
    Main database client that coordinates all table-specific clients.
    
    This is the primary interface for database operations.
    """
    
    def __init__(self, db_config: DatabaseConfig):
        """Initialize database client with configuration."""
        pass
    
    def _create_connection_pool(self) -> psycopg2.pool.ThreadedConnectionPool:
        """Create shared connection pool for all clients."""
        pass
    
    def create_schema_and_tables(self) -> None:
        """Create database schema and tables if they don't exist."""
        pass
    
    def verify_schema(self) -> Dict[str, bool]:
        """Verify all required tables and extensions exist."""
        pass
    
    def test_connection(self) -> bool:
        """Test database connectivity."""
        pass
    
    def get_pool_stats(self) -> Dict[str, int]:
        """Get connection pool statistics."""
        pass
    
    def close(self) -> None:
        """Close all connections and cleanup resources."""
        pass
    
    @property
    def market_data(self) -> MarketDataClient:
        """Access market data operations."""
        pass
    
    @property
    def failed_downloads(self) -> FailedDownloadsClient:
        """Access failed downloads tracking."""
        pass
    
    @property
    def data_quality(self) -> DataQualityClient:
        """Access data quality metrics."""
        pass