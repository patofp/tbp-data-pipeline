#!/usr/bin/env python3
"""Test script for ConfigLoader"""

import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from config_loader import ConfigLoader

def test_config_loader():
    """Test the ConfigLoader with sample environment variables"""
    
    # Set some test environment variables
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
    
    # Set environment variables
    for key, value in test_env_vars.items():
        os.environ[key] = value
    
    # Create config loader
    print("Loading configuration...")
    config = ConfigLoader()
    
    # Test environment validation
    print("\n1. Testing environment validation:")
    is_valid = config.validate_environment()
    print(f"   Environment valid: {is_valid}")
    
    # Test loading tickers
    print("\n2. Testing ticker loading:")
    tickers = config.get_all_tickers()
    print(f"   Loaded {len(tickers)} tickers")
    if tickers:
        print(f"   First ticker: {tickers[0]}")
    
    # Test ticker groups
    print("\n3. Testing ticker groups:")
    groups = config.get_ticker_groups()
    for group_name, symbols in groups.items():
        print(f"   {group_name}: {len(symbols)} symbols")
    
    # Test S3 config
    print("\n4. Testing S3 configuration:")
    s3_config = config.get_s3_config()
    print(f"   Endpoint: {s3_config.endpoint}")
    print(f"   Bucket: {s3_config.bucket_name}")
    print(f"   Access Key: {s3_config.credentials.access_key}")
    print(f"   File Format: {s3_config.file_format}")
    
    # Test database config
    print("\n5. Testing database configuration:")
    db_config = config.get_database_config()
    print(f"   Host: {db_config.connection.host}")
    print(f"   Port: {db_config.connection.port}")
    print(f"   Database: {db_config.connection.database}")
    print(f"   Schema: {db_config.schema}")
    print(f"   Main table: {db_config.tables.market_data_raw}")
    
    # Test with missing environment variable
    print("\n6. Testing with missing environment variable:")
    del os.environ['DB_PASSWORD']
    config2 = ConfigLoader()
    is_valid2 = config2.validate_environment()
    print(f"   Environment valid (missing DB_PASSWORD): {is_valid2}")
    
    # Restore for cleanup
    os.environ['DB_PASSWORD'] = test_env_vars['DB_PASSWORD']
    
    print("\nAll tests completed!")

if __name__ == "__main__":
    test_config_loader()
