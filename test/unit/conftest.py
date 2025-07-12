#!/usr/bin/env python3
"""
Pytest configuration for unit tests.

Unit tests use mocks and don't require external dependencies.
"""

import pytest
import sys
from pathlib import Path
from datetime import datetime, date
from unittest.mock import Mock
import pandas as pd

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))


@pytest.fixture
def mock_db_config():
    """Create mock database configuration."""
    config = Mock()
    config.connection.username = "test_user"
    config.connection.password = "test_pass"
    config.connection.host = "localhost"
    config.connection.port = 5432
    config.connection.database = "test_db"
    return config


@pytest.fixture
def mock_pool():
    """Create mock connection pool."""
    return Mock()


@pytest.fixture
def sample_dataframe():
    """Create a sample market data DataFrame for testing."""
    data = {
        'ticker': ['AAPL', 'AAPL', 'GOOGL', 'GOOGL', 'MSFT'],
        'timestamp': pd.to_datetime([
            '2024-01-02', '2024-01-03', '2024-01-02', 
            '2024-01-03', '2024-01-02'
        ]),
        'open': [180.0, 181.5, 140.0, 142.0, 370.0],
        'high': [182.0, 183.0, 142.5, 144.0, 375.0],
        'low': [179.5, 180.0, 139.5, 141.0, 369.0],
        'close': [181.5, 182.0, 141.5, 143.5, 373.0],
        'volume': [75000000, 80000000, 25000000, 28000000, 30000000],
        'transactions': [500000, 520000, 300000, 310000, 350000]
    }
    return pd.DataFrame(data)


@pytest.fixture
def empty_dataframe():
    """Create an empty DataFrame for testing edge cases."""
    return pd.DataFrame()