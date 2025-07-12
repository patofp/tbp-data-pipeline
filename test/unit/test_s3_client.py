#!/usr/bin/env python3
"""
Unit tests for S3Client.

These tests use mocks and don't require LocalStack or external dependencies.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import date, datetime
import pandas as pd
from io import BytesIO
import gzip

from src.s3_client import PolygonS3Client, DataType, FailedDownload


def create_mock_s3_config():
    """Helper function to create a properly configured mock S3 config."""
    mock_config = Mock()
    mock_config.endpoint = "https://test.endpoint.com"
    mock_config.bucket_name = "test-bucket"
    mock_config.region = "us-east-1"
    mock_config.credentials.access_key = "test_key"
    mock_config.credentials.secret_key = "test_secret"
    mock_config.file_format = "csv"
    
    # Create path_structure as an object with attributes
    mock_path_structure = Mock()
    mock_path_structure.day_aggs = "us_stocks_sip/day_aggs_v1/{year}/{month:02d}/{year}-{month:02d}-{day:02d}.csv.gz"
    mock_path_structure.minute_aggs = "us_stocks_sip/minute_aggs_v1/{year}/{month:02d}/{year}-{month:02d}-{day:02d}.csv.gz"
    mock_path_structure.trades = "us_stocks_sip/trades_v1/{year}/{month:02d}/{year}-{month:02d}-{day:02d}.csv.gz"
    mock_path_structure.quotes = "us_stocks_sip/quotes_v1/{year}/{month:02d}/{year}-{month:02d}-{day:02d}.csv.gz"
    mock_config.path_structure = mock_path_structure
    
    mock_config.connect_timeout_seconds = 30
    mock_config.read_timeout_seconds = 300
    mock_config.max_retries = 3
    return mock_config


@pytest.mark.unit
class TestS3ClientInitialization:
    """Test S3 client initialization with mocks."""
    
    @patch('boto3.client')
    def test_s3_client_initialization(self, mock_boto3_client):
        """Test that S3 client initializes with provided config."""
        # Mock the S3 client methods
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {'Contents': []}
        
        mock_config = create_mock_s3_config()
        
        client = PolygonS3Client(mock_config)
        
        assert client.bucket_name == "test-bucket"
        assert client.path_structure.day_aggs == "us_stocks_sip/day_aggs_v1/{year}/{month:02d}/{year}-{month:02d}-{day:02d}.csv.gz"
    
    @patch('boto3.client')
    def test_s3_client_boto3_initialization(self, mock_boto3_client):
        """Test that boto3 client is initialized correctly."""
        # Mock the S3 client methods
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {'Contents': []}
        
        mock_config = create_mock_s3_config()
        
        client = PolygonS3Client(mock_config)
        
        # Verify boto3 client was created with correct parameters
        assert mock_boto3_client.called
        call_args = mock_boto3_client.call_args
        assert call_args[0][0] == 's3'
        assert call_args[1]['aws_access_key_id'] == "test_key"
        assert call_args[1]['aws_secret_access_key'] == "test_secret"
        assert call_args[1]['region_name'] == "us-east-1"
        assert call_args[1]['endpoint_url'] == "https://test.endpoint.com"
        # We can't easily check the Config object, just verify it exists
        assert 'config' in call_args[1]


@pytest.mark.unit
class TestS3ClientPathGeneration:
    """Test S3 path generation logic."""
    
    @patch('boto3.client')
    def test_generate_s3_path_day_aggs(self, mock_boto3_client):
        """Test S3 path generation for day aggregates."""
        # Mock the S3 client
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {'Contents': []}
        
        mock_config = create_mock_s3_config()
        client = PolygonS3Client(mock_config)
        
        test_date = date(2024, 1, 15)
        path = client._generate_s3_path(test_date, DataType.DAY_AGGS)
        
        expected = "us_stocks_sip/day_aggs_v1/2024/01/2024-01-15.csv.gz"
        assert path == expected
    
    @patch('boto3.client')
    def test_generate_s3_path_different_format(self, mock_boto3_client):
        """Test S3 path generation with different path structure."""
        # Mock the S3 client
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {'Contents': []}
        
        mock_config = create_mock_s3_config()
        # Override path structure for day_aggs
        mock_config.path_structure.day_aggs = "data/{year}/{month:02d}/{day:02d}/file.csv"
        
        client = PolygonS3Client(mock_config)
        
        test_date = date(2024, 12, 31)
        path = client._generate_s3_path(test_date, DataType.DAY_AGGS)
        
        expected = "data/2024/12/31/file.csv"
        assert path == expected


@pytest.mark.unit
class TestS3ClientFileOperations:
    """Test S3 file operations with mocks."""
    
    @patch('boto3.client')
    def test_check_file_exists_true(self, mock_boto3_client):
        """Test file existence check when file exists."""
        # Setup mock S3 client
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {'Contents': []}
        mock_s3.head_object.return_value = {'ContentLength': 1024}
        
        mock_config = create_mock_s3_config()
        client = PolygonS3Client(mock_config)
        
        exists = client.check_file_exists('AAPL', date(2024, 1, 2), DataType.DAY_AGGS)
        
        assert exists is True
        mock_s3.head_object.assert_called_once()
    
    @patch('boto3.client')
    def test_check_file_exists_false(self, mock_boto3_client):
        """Test file existence check when file doesn't exist."""
        # Setup mock S3 client
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {'Contents': []}
        
        # Simulate file not found
        from botocore.exceptions import ClientError
        error_response = {'Error': {'Code': '404'}}
        mock_s3.head_object.side_effect = ClientError(error_response, 'HeadObject')
        
        mock_config = create_mock_s3_config()
        client = PolygonS3Client(mock_config)
        
        exists = client.check_file_exists('AAPL', date(2024, 1, 2), DataType.DAY_AGGS)
        
        assert exists is False


@pytest.mark.unit
class TestS3ClientDownloads:
    """Test download operations with mocks."""
    
    @patch('boto3.client')
    def test_download_daily_data_success(self, mock_boto3_client):
        """Test successful download of daily data."""
        # Create mock CSV data
        csv_data = """ticker,timestamp,open,high,low,close,volume,vwap,transactions
AAPL,2024-01-02 00:00:00,180.0,182.0,179.5,181.5,75000000,181.0,500000
MSFT,2024-01-02 00:00:00,370.0,375.0,369.0,373.0,30000000,372.0,350000"""
        
        # Compress the data
        compressed_data = BytesIO()
        with gzip.GzipFile(fileobj=compressed_data, mode='wb') as gz:
            gz.write(csv_data.encode('utf-8'))
        compressed_data.seek(0)
        
        # Setup mock S3 client
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {'Contents': []}
        mock_s3.get_object.return_value = {
            'Body': compressed_data,
            'ContentLength': len(compressed_data.getvalue())
        }
        
        mock_config = create_mock_s3_config()
        client = PolygonS3Client(mock_config)
        
        df = client.download_daily_data('AAPL', date(2024, 1, 2), DataType.DAY_AGGS)
        
        assert df is not None
        assert not df.empty
        assert len(df) == 1  # Filtered to AAPL only
        assert df.iloc[0]['ticker'] == 'AAPL'
        assert df.iloc[0]['open'] == 180.0
        assert 'ingestion_date' in df.columns
        assert 'data_source' in df.columns
    
    @patch('src.s3_client.PolygonS3Client.check_file_exists')
    @patch('boto3.client')
    def test_download_daily_data_file_not_found(self, mock_boto3_client, mock_check_exists):
        """Test download when file doesn't exist."""
        # Setup mock S3 client
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {'Contents': []}
        
        # Mock check_file_exists to return False
        mock_check_exists.return_value = False
        
        mock_config = create_mock_s3_config()
        client = PolygonS3Client(mock_config)
        
        df = client.download_daily_data('AAPL', date(2024, 1, 2), DataType.DAY_AGGS)
        
        assert df is None
        mock_check_exists.assert_called_once_with('AAPL', date(2024, 1, 2), DataType.DAY_AGGS)
    
    @patch('boto3.client')
    def test_download_date_range(self, mock_boto3_client):
        """Test downloading data for a date range."""
        # Setup mock S3 client
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {'Contents': []}
        
        # Mock head_object to return True for some dates
        def head_object_side_effect(Bucket, Key):
            if '2024-01-02' in Key or '2024-01-03' in Key:
                return {'ContentLength': 1024}
            else:
                from botocore.exceptions import ClientError
                error_response = {'Error': {'Code': '404'}}
                raise ClientError(error_response, 'HeadObject')
        
        mock_s3.head_object.side_effect = head_object_side_effect
        
        # Mock successful downloads
        csv_data = """ticker,timestamp,open,high,low,close,volume,vwap,transactions
AAPL,2024-01-02 00:00:00,180.0,182.0,179.5,181.5,75000000,181.0,500000"""
        
        compressed_data = BytesIO()
        with gzip.GzipFile(fileobj=compressed_data, mode='wb') as gz:
            gz.write(csv_data.encode('utf-8'))
        compressed_data.seek(0)
        
        mock_s3.get_object.return_value = {
            'Body': compressed_data,
            'ContentLength': len(compressed_data.getvalue())
        }
        
        mock_config = create_mock_s3_config()
        client = PolygonS3Client(mock_config)
        
        df, failures = client.download_date_range(
            'AAPL',
            date(2024, 1, 1),
            date(2024, 1, 5),
            DataType.DAY_AGGS
        )
        
        # Should have some successes and some failures
        assert df is not None
        assert isinstance(failures, list)
        
        # Check failures are FailedDownload objects
        for failure in failures:
            assert isinstance(failure, FailedDownload)
            assert hasattr(failure, 'ticker')
            assert hasattr(failure, 'date')
            assert hasattr(failure, 'error_message')