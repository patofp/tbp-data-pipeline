#!/usr/bin/env python3
"""
Global pytest configuration and shared fixtures.

This file contains fixtures that are shared between unit and integration tests.
Specific fixtures for unit tests should go in test/unit/conftest.py
Specific fixtures for integration tests should go in test/integration/conftest.py
"""

import pytest
import sys
from pathlib import Path
from datetime import date

# Configure pytest marks
def pytest_configure(config):
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests (fast, no external dependencies)"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests (slower, requires Docker)"
    )
    config.addinivalue_line(
        "markers", "database: marks tests that require database connection"
    )
    config.addinivalue_line(
        "markers", "slow: marks tests that take more than a few seconds"
    )

# Add src to path for all tests
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))


# Global test data fixtures that can be used by both unit and integration tests
@pytest.fixture(scope="session")
def test_data_dates():
    """Common test dates using January 2024."""
    return {
        'ticker': 'AAPL',
        'trading_date': date(2024, 1, 2),   # Tuesday - should exist
        'weekend_date': date(2024, 1, 6),   # Saturday - should not exist
        'holiday_date': date(2024, 1, 1),   # New Year's Day - market closed
        'start_date': date(2024, 1, 2),     # Start of fixtures
        'end_date': date(2024, 1, 5),       # End of week
        'month_start': date(2024, 1, 1),
        'month_end': date(2024, 1, 31),
    }


@pytest.fixture(scope="session")
def test_tickers():
    """Common test ticker symbols."""
    return {
        'valid': ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'META'],
        'invalid': ['INVALID', 'NOTREAL', '123456'],
        'etf': ['SPY', 'QQQ', 'IWM'],
        'crypto': ['BTC-USD', 'ETH-USD'],
    }


@pytest.fixture(scope="session")
def fixtures_path():
    """Path to shared test fixtures directory."""
    return Path(__file__).parent / 'fixtures'


@pytest.fixture(scope="session")
def raw_data_fixtures_path(fixtures_path):
    """Path to raw data fixtures (CSV files)."""
    return fixtures_path / 'raw'


# Helper fixtures for test organization
@pytest.fixture
def test_output_dir(tmp_path):
    """Create a temporary directory for test outputs."""
    output_dir = tmp_path / "test_output"
    output_dir.mkdir(exist_ok=True)
    return output_dir