"""
Database package for TBP Data Pipeline.

Provides modular clients for TimescaleDB operations.
"""
from .client import TimescaleDBClient
from .market_data import MarketDataClient
from .failed_download import FailedDownloadsClient
from .data_quality import DataQualityClient
from .utils import verify_timescaledb_extension, create_database_url

__all__ = [
    'TimescaleDBClient',
    'MarketDataClient', 
    'FailedDownloadsClient',
    'DataQualityClient',
    'verify_timescaledb_extension',
    'create_database_url'
]