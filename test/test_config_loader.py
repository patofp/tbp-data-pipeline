#!/usr/bin/env python3
"""Test script for ConfigLoader"""

import os
import sys
from pathlib import Path
import pytest

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from config_loader import ConfigLoader

@pytest.fixture(autouse=True)
def set_env_vars(monkeypatch):
    test_env_vars = {
        'POLYGON_S3_ACCESS_KEY': 'test_s3_access_key',
        'POLYGON_S3_SECRET_KEY': 'test_s3_secret_key',
        'POLYGON_API_KEY': 'test_api_key',
        'DB_HOST': '192.168.1.11',
        'DB_PORT': '5432',
        'DB_NAME': 'trading_tbp',
        'DB_USER': 'postgres',
        'DB_PASSWORD': 'test_password',
        'LOG_LEVEL': 'DEBUG'
    }
    for key, value in test_env_vars.items():
        monkeypatch.setenv(key, value)
    yield
    # Cleanup is automatic with monkeypatch

def test_environment_validation():
    config = ConfigLoader()
    assert config.validate_environment() is True
    print("✅ test_environment_validation passed")

def test_loading_tickers():
    config = ConfigLoader()
    tickers = config.get_all_tickers()
    assert isinstance(tickers, list)
    assert len(tickers) == 12
    assert tickers[0].symbol == "AAPL"
    print("✅ test_loading_tickers passed")

def test_ticker_groups():
    config = ConfigLoader()
    groups = config.get_ticker_groups()
    assert "high_priority" in groups
    assert len(groups["high_priority"]) == 4
    print("✅ test_ticker_groups passed")

def test_s3_config():
    config = ConfigLoader()
    s3_config = config.get_s3_config()
    assert s3_config.endpoint == "https://files.polygon.io"
    assert s3_config.bucket_name == "flatfiles"
    assert s3_config.credentials.access_key == "test_s3_access_key"
    assert s3_config.file_format == "csv"
    print("✅ test_s3_config passed")

def test_database_config():
    config = ConfigLoader()
    db_config = config.get_database_config()
    assert db_config.connection.host == "192.168.1.11"
    assert db_config.connection.port == "5432"
    assert db_config.connection.database == "trading_tbp"
    assert db_config.schema == "trading"
    assert db_config.tables.market_data_raw == "market_data_raw"
    print("✅ test_database_config passed")

def test_missing_env_var(monkeypatch):
    monkeypatch.delenv("DB_PASSWORD", raising=False)
    config = ConfigLoader()
    assert config.validate_environment() is False
    print("✅ test_missing_env_var passed")
