#!/usr/bin/env python3
"""
Pytest configuration for integration tests.

Integration tests use real services (Docker containers) and don't mock external dependencies.
"""

import pytest
import subprocess
import time
import os
import sys
from pathlib import Path
from datetime import datetime, date, timedelta
import pandas as pd
import psycopg2
import boto3

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

from src.database.market_data import MarketDataClient
from src.config_loader import ConfigLoader
from src.s3_client import PolygonS3Client, DataType


# S3/LocalStack fixtures
@pytest.fixture(scope="session")
def localstack_container():
    """Start LocalStack container and wait for it to be ready."""
    
    print("\n🐳 Starting LocalStack container...")
    
    # Start LocalStack
    result = subprocess.run(
        ["docker", "compose", "-f", "test/integration/docker-compose.test.yml", "up", "-d", "localstack"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent.parent
    )
    
    if result.returncode != 0:
        pytest.fail(f"Failed to start LocalStack: {result.stderr}")
    
    # Wait for LocalStack to be ready
    max_attempts = 30
    for attempt in range(max_attempts):
        try:
            # Test if LocalStack S3 is responding
            response = subprocess.run(
                ["curl", "-s", "http://localhost:4566/health"],
                capture_output=True,
                timeout=5
            )
            if response.returncode == 0:
                print("✅ LocalStack is ready")
                break
        except Exception:
            pass
            
        if attempt < max_attempts - 1:
            print(f"⏳ Waiting for LocalStack... (attempt {attempt + 1}/{max_attempts})")
            time.sleep(2)
    else:
        pytest.fail("LocalStack failed to start within expected time")
    
    yield
    
    # Cleanup: Stop LocalStack
    print("\n🧹 Stopping LocalStack container...")
    subprocess.run(
        ["docker", "compose", "-f", "test/integration/docker-compose.test.yml", "down", "localstack"],
        capture_output=True,
        cwd=Path(__file__).parent.parent.parent
    )


@pytest.fixture(scope="session")
def localstack_s3(localstack_container, raw_data_fixtures_path):
    """Setup LocalStack S3 with test data."""
    
    print("\n📦 Setting up LocalStack S3 with test fixtures...")
    
    # Create S3 client for LocalStack
    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )
    
    # Create bucket
    bucket_name = 'flatfiles'
    try:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"✅ Created bucket: {bucket_name}")
    except Exception as e:
        print(f"⚠️  Bucket creation failed (may already exist): {e}")
    
    # Upload test fixtures
    uploaded_count = 0
    
    for fixture_file in raw_data_fixtures_path.glob('*.csv.gz'):
        # Generate S3 key matching Polygon.io structure
        # Remove .csv from stem since files are named like "2024-01-02.csv.gz"
        date_str = fixture_file.stem.replace('.csv', '')  # e.g., "2024-01-02"
        year, month, day = date_str.split('-')
        s3_key = f"us_stocks_sip/day_aggs_v1/{year}/{month}/{date_str}.csv.gz"
        
        try:
            # Upload file
            with open(fixture_file, 'rb') as f:
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=s3_key,
                    Body=f.read()
                )
            uploaded_count += 1
            print(f"✅ Uploaded: {s3_key}")
            
        except Exception as e:
            print(f"❌ Failed to upload {fixture_file.name}: {e}")
    
    print(f"📊 Uploaded {uploaded_count} fixtures to LocalStack S3")
    
    # Verify uploads
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix='us_stocks_sip/day_aggs_v1/2024/01/'
        )
        objects = response.get('Contents', [])
        print(f"🔍 Verified {len(objects)} objects in S3")
        
    except Exception as e:
        print(f"⚠️  Failed to verify uploads: {e}")
    
    yield s3_client


@pytest.fixture(scope="session")
def test_s3_config(localstack_s3):
    """Load test configuration with LocalStack endpoint."""
    
    # Load .env.test if exists
    env_file = Path(__file__).parent.parent / '.env.test'
    if env_file.exists():
        from dotenv import load_dotenv
        load_dotenv(env_file)
    
    # Override S3 endpoint for LocalStack
    os.environ['S3_ENDPOINT_URL'] = 'http://localhost:4566'
    
    # Change to test directory to load config files
    original_cwd = os.getcwd()
    try:
        os.chdir(Path(__file__).parent.parent.parent)  # Go to project root
        config_loader = ConfigLoader()
        
        # Validate environment
        if not config_loader.validate_environment():
            pytest.skip("Test environment validation failed")
        
        return config_loader
    finally:
        os.chdir(original_cwd)


@pytest.fixture(scope="session")
def test_s3_client(test_s3_config):
    """Create S3 client configured for LocalStack."""
    
    s3_config = test_s3_config.get_s3_config()
    
    # Override endpoint for LocalStack
    s3_config.endpoint = 'http://localhost:4566'
    
    return PolygonS3Client(s3_config)


# Database fixtures
@pytest.fixture(scope="session")
def docker_compose_file():
    """Path to docker-compose file for integration tests."""
    return Path(__file__).parent / "docker-compose.test.yml"


@pytest.fixture(scope="session")
def postgres_container(docker_compose_file):
    """Start PostgreSQL container and wait for it to be ready."""
    
    print("\n🐘 Starting PostgreSQL container for integration tests...")
    
    # Start PostgreSQL
    result = subprocess.run(
        ["docker", "compose", "-f", str(docker_compose_file), "up", "-d", "postgres-test"],
        capture_output=True,
        text=True,
        cwd=docker_compose_file.parent.parent.parent
    )
    
    if result.returncode != 0:
        pytest.fail(f"Failed to start PostgreSQL: {result.stderr}")
    
    # Wait for PostgreSQL to be ready
    max_attempts = 30
    for attempt in range(max_attempts):
        try:
            # Test if PostgreSQL is accepting connections
            test_conn = psycopg2.connect(
                host="localhost",
                port=5433,
                database="test_db",
                user="test_user",
                password="test_password"
            )
            test_conn.close()
            print("✅ PostgreSQL is ready")
            break
        except Exception:
            pass
            
        if attempt < max_attempts - 1:
            print(f"⏳ Waiting for PostgreSQL... (attempt {attempt + 1}/{max_attempts})")
            time.sleep(2)
    else:
        pytest.fail("PostgreSQL failed to start within expected time")
    
    yield
    
    # Cleanup: Stop PostgreSQL
    print("\n🧹 Stopping PostgreSQL container...")
    subprocess.run(
        ["docker", "compose", "-f", str(docker_compose_file), "down"],
        capture_output=True,
        cwd=docker_compose_file.parent.parent.parent
    )


@pytest.fixture(scope="session")
def test_db_config():
    """Test database configuration."""
    from src.config_loader import (
        DatabaseConfig, DatabaseConnection, DatabasePool, DatabaseTables
    )
    
    connection = DatabaseConnection(
        host="localhost",
        port=5433,
        database="test_db",
        username="test_user",
        password="test_password"
    )
    
    pool = DatabasePool(
        min_connection=2,
        max_connection=5,
        connection_timeout_seconds=10,
        idle_timeout_seconds=60
    )
    
    tables = DatabaseTables(
        market_data_raw="market_data_raw",
        ingestion_log="ingestion_log",
        data_quality="data_quality_metrics"
    )
    
    return DatabaseConfig(
        connection=connection,
        pool=pool,
        schema="trading",
        tables=tables,
        batch_insert_size=1000,
        upsert_on_conflict=True,
        column_mapping={}
    )


@pytest.fixture(scope="session")
def test_db_connection(postgres_container, test_db_config):
    """Create test database connection."""
    
    print("\n🔌 Creating test database connection...")
    
    # Create connection
    connection = psycopg2.connect(
        host=test_db_config.connection.host,
        port=test_db_config.connection.port,
        database=test_db_config.connection.database,
        user=test_db_config.connection.username,
        password=test_db_config.connection.password
    )
    
    # Enable autocommit for tests
    connection.autocommit = True
    
    yield connection
    
    # Cleanup: Close connection
    connection.close()
    print("🔌 Test database connection closed")


@pytest.fixture(scope="function")
def setup_test_schema(test_db_connection):
    """Create test schema and tables if they don't exist."""
    cursor = test_db_connection.cursor()
    
    try:
        # Create schema
        cursor.execute("CREATE SCHEMA IF NOT EXISTS trading")
        
        # Create market_data_raw table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trading.market_data_raw (
                ticker VARCHAR(20) NOT NULL,
                timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                timeframe VARCHAR(10) NOT NULL,
                data_source VARCHAR(50) NOT NULL,
                open NUMERIC(20, 6),
                high NUMERIC(20, 6),
                low NUMERIC(20, 6),
                close NUMERIC(20, 6),
                volume BIGINT,
                transactions INTEGER,
                ingested_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT market_data_raw_pkey PRIMARY KEY (ticker, timestamp, timeframe, data_source)
            )
        """)
        
        # Create indexes
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_market_data_raw_ticker_timestamp 
            ON trading.market_data_raw(ticker, timestamp)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_market_data_raw_timestamp 
            ON trading.market_data_raw(timestamp)
        """)
        
        test_db_connection.commit()
        
    except Exception as e:
        test_db_connection.rollback()
        raise e
    finally:
        cursor.close()
    
    yield


@pytest.fixture(autouse=True)
def clean_db_per_test(test_db_connection, setup_test_schema):
    """Auto-clean database before each test to ensure isolation."""
    yield  # Run the test
    
    # Clean after test
    cursor = test_db_connection.cursor()
    try:
        cursor.execute("TRUNCATE TABLE trading.market_data_raw RESTART IDENTITY CASCADE")
        test_db_connection.commit()
    except Exception:
        test_db_connection.rollback()
    finally:
        cursor.close()


@pytest.fixture(scope="function") 
def test_market_data_client(postgres_container):
    """Create MarketDataClient with real connection pool."""
    from src.database.connection import ConnectionManager
    from src.config_loader import (
        DatabaseConfig, DatabaseConnection, DatabasePool, DatabaseTables
    )
    
    # Create real database configuration
    connection = DatabaseConnection(
        host="localhost",
        port=5433,
        database="test_db",
        username="test_user",
        password="test_password"
    )
    
    pool = DatabasePool(
        min_connection=2,
        max_connection=5,
        connection_timeout_seconds=10,
        idle_timeout_seconds=60
    )
    
    tables = DatabaseTables(
        market_data_raw="market_data_raw",
        ingestion_log="ingestion_log",
        data_quality="data_quality_metrics"
    )
    
    db_config = DatabaseConfig(
        connection=connection,
        pool=pool,
        schema="trading",
        tables=tables,
        batch_insert_size=1000,
        upsert_on_conflict=True,
        column_mapping={}
    )
    
    # Create real connection manager
    conn_manager = ConnectionManager(db_config)
    conn_manager.initialize()
    
    # Create MarketDataClient with the internal psycopg2 pool
    # The MarketDataClient expects a psycopg2 pool, not our wrapper
    client = MarketDataClient(db_config, conn_manager.pool._pool)
    
    yield client
    
    # Cleanup
    conn_manager.close()


# Test data fixtures
@pytest.fixture
def sample_market_data():
    """Generate sample market data for multiple tickers and dates."""
    data = []
    tickers = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'META']
    base_date = date(2024, 1, 2)  # Tuesday
    
    for ticker in tickers:
        # Generate 5 days of data (skip weekends)
        for i in range(5):
            current_date = base_date + timedelta(days=i)
            
            # Skip weekends
            if current_date.weekday() >= 5:  # Saturday=5, Sunday=6
                continue
            
            # Generate realistic OHLCV data
            base_price = {
                'AAPL': 180.0,
                'GOOGL': 140.0,
                'MSFT': 370.0,
                'AMZN': 150.0,
                'META': 350.0
            }[ticker]
            
            # Add some randomness
            open_price = base_price + (i * 0.5)
            high_price = open_price + 2.0
            low_price = open_price - 1.5
            close_price = open_price + 0.5
            volume = 50000000 + (i * 1000000)
            transactions = 400000 + (i * 10000)
            
            data.append({
                'ticker': ticker,
                'timestamp': datetime.combine(current_date, datetime.min.time()),
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': volume,
                'transactions': transactions
            })
    
    return pd.DataFrame(data)


@pytest.fixture
def sample_data_with_gaps():
    """Generate sample data with intentional gaps for gap testing."""
    data = []
    ticker = 'AAPL'
    
    # Dates with gaps: Jan 2, 3, 5, 8, 9 (missing 4, 6-7 are weekend)
    dates = [
        date(2024, 1, 2),   # Tuesday
        date(2024, 1, 3),   # Wednesday
        # Missing Jan 4 (Thursday)
        date(2024, 1, 5),   # Friday
        # Weekend Jan 6-7
        date(2024, 1, 8),   # Monday
        date(2024, 1, 9),   # Tuesday
    ]
    
    for i, current_date in enumerate(dates):
        data.append({
            'ticker': ticker,
            'timestamp': datetime.combine(current_date, datetime.min.time()),
            'open': 180.0 + i,
            'high': 182.0 + i,
            'low': 179.0 + i,
            'close': 181.0 + i,
            'volume': 75000000,
            'transactions': 500000
        })
    
    return pd.DataFrame(data)


@pytest.fixture
def sample_duplicate_data():
    """Generate sample data with duplicates for conflict testing."""
    # Create base data
    base_data = {
        'ticker': ['AAPL', 'AAPL', 'GOOGL'],
        'timestamp': pd.to_datetime(['2024-01-02', '2024-01-02', '2024-01-02']),
        'open': [180.0, 180.5, 140.0],  # Different values for same ticker/date
        'high': [182.0, 182.5, 142.0],
        'low': [179.5, 179.0, 139.5],
        'close': [181.5, 181.0, 141.5],
        'volume': [75000000, 76000000, 25000000],
        'transactions': [500000, 510000, 300000]
    }
    
    return pd.DataFrame(base_data)