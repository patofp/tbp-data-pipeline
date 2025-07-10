#!/usr/bin/env python3
"""
Pytest configuration and fixtures for S3Client testing with LocalStack
"""

import pytest
import boto3
import subprocess
import time
import os
import sys
from pathlib import Path
from datetime import date

# Configure pytest marks
def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from config_loader import ConfigLoader
from s3_client import PolygonS3Client, DataType


@pytest.fixture(scope="session")
def localstack_container():
    """Start LocalStack container and wait for it to be ready"""
    
    print("\n🐳 Starting LocalStack container...")
    
    # Start LocalStack
    result = subprocess.run(
        ["docker", "compose", "-f", "test/docker-compose.test.yml", "up", "-d"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent
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
        ["docker", "compose", "-f", "test/docker-compose.test.yml", "down"],
        capture_output=True,
        cwd=Path(__file__).parent.parent
    )


@pytest.fixture(scope="session")
def localstack_s3(localstack_container):
    """Setup LocalStack S3 with test data"""
    
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
    fixtures_dir = Path(__file__).parent / 'fixtures' / 'raw'
    uploaded_count = 0
    
    for fixture_file in fixtures_dir.glob('*.csv.gz'):
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
def test_config(localstack_s3):
    """Load test configuration with LocalStack endpoint"""
    
    # Load .env.test
    env_file = Path(__file__).parent / '.env.test'
    if env_file.exists():
        from dotenv import load_dotenv
        load_dotenv(env_file)
    
    # Override S3 endpoint for LocalStack
    os.environ['S3_ENDPOINT_URL'] = 'http://localhost:4566'
    
    # Change to test directory to load config files
    original_cwd = os.getcwd()
    try:
        os.chdir(Path(__file__).parent.parent)  # Go to project root
        config_loader = ConfigLoader()
        
        # Validate environment
        if not config_loader.validate_environment():
            pytest.skip("Test environment validation failed")
        
        return config_loader
    finally:
        os.chdir(original_cwd)


@pytest.fixture(scope="session")
def test_s3_client(test_config):
    """Create S3 client configured for LocalStack"""
    
    s3_config = test_config.get_s3_config()
    
    # Override endpoint for LocalStack
    s3_config.endpoint = 'http://localhost:4566'
    
    return PolygonS3Client(s3_config)


@pytest.fixture(scope="session")
def test_data():
    """Test data parameters using January 2024 fixtures"""
    return {
        'ticker': 'AAPL',
        'trading_date': date(2024, 1, 2),   # Tuesday - should exist
        'weekend_date': date(2024, 1, 6),   # Saturday - should not exist
        'start_date': date(2024, 1, 2),     # Start of fixtures
        'end_date': date(2024, 1, 5),       # End of week
        'data_type': DataType.DAY_AGGS
    }
