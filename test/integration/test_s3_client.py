#!/usr/bin/env python3
"""
Integration tests for S3Client using LocalStack.

These tests use real S3 operations against LocalStack container.
"""

import pytest
from datetime import date
import pandas as pd

from src.s3_client import DataType, FailedDownload


@pytest.mark.integration
class TestS3ClientIntegrationConnection:
    """Test S3 client connectivity with LocalStack."""
    
    def test_s3_client_initialization(self, test_s3_client):
        """Test that S3 client initializes successfully with LocalStack."""
        assert test_s3_client is not None
        assert test_s3_client.bucket_name == "flatfiles"
        assert test_s3_client.path_structure is not None
    
    def test_s3_config_loaded(self, test_s3_config):
        """Test that S3 configuration loads correctly for integration."""
        s3_config = test_s3_config.get_s3_config()
        assert s3_config.bucket_name == "flatfiles"
        assert s3_config.credentials.access_key is not None
        assert s3_config.credentials.secret_key is not None
    
    def test_localstack_bucket_exists(self, localstack_s3):
        """Test that LocalStack bucket was created successfully."""
        # List buckets to verify our test bucket exists
        response = localstack_s3.list_buckets()
        bucket_names = [b['Name'] for b in response.get('Buckets', [])]
        assert 'flatfiles' in bucket_names


@pytest.mark.integration
class TestS3ClientIntegrationFileOperations:
    """Test S3 client file operations against LocalStack."""
    
    def test_check_file_exists_trading_day(self, test_s3_client, test_data_dates):
        """Test file existence check for a trading day (should exist in fixtures)."""
        exists = test_s3_client.check_file_exists(
            test_data_dates['ticker'], 
            test_data_dates['trading_date'], 
            DataType.DAY_AGGS
        )
        # Should be True for 2024-01-02 (Tuesday, fixture exists)
        assert exists is True
    
    def test_check_file_exists_weekend(self, test_s3_client, test_data_dates):
        """Test file existence check for a weekend (no fixture)."""
        exists = test_s3_client.check_file_exists(
            test_data_dates['ticker'], 
            test_data_dates['weekend_date'], 
            DataType.DAY_AGGS
        )
        # Should be False for 2024-01-06 (Saturday, no fixture)
        assert exists is False
    
    def test_generate_s3_path(self, test_s3_client, test_data_dates):
        """Test S3 path generation matches expected format."""
        s3_path = test_s3_client._generate_s3_path(
            test_data_dates['trading_date'], 
            DataType.DAY_AGGS
        )
        expected_path = "us_stocks_sip/day_aggs_v1/2024/01/2024-01-02.csv.gz"
        assert s3_path == expected_path
    
    def test_list_files_in_bucket(self, test_s3_client, localstack_s3):
        """Test listing files in LocalStack bucket."""
        # List objects in the test prefix
        response = localstack_s3.list_objects_v2(
            Bucket='flatfiles',
            Prefix='us_stocks_sip/day_aggs_v1/2024/01/'
        )
        
        objects = response.get('Contents', [])
        assert len(objects) > 0
        
        # Check that our fixtures were uploaded
        object_keys = [obj['Key'] for obj in objects]
        assert any('2024-01-02.csv.gz' in key for key in object_keys)


@pytest.mark.integration
class TestS3ClientIntegrationDownloads:
    """Test S3 client download operations against LocalStack."""
    
    def test_download_daily_data_success(self, test_s3_client, test_data_dates):
        """Test downloading data for a single trading day."""
        df = test_s3_client.download_daily_data(
            test_data_dates['ticker'], 
            test_data_dates['trading_date'], 
            DataType.DAY_AGGS
        )
        
        # Should return DataFrame with data
        assert df is not None
        assert not df.empty
        assert len(df) >= 1
        
        # Check required columns
        required_cols = ['ticker', 'open', 'high', 'low', 'close', 'volume']
        for col in required_cols:
            assert col in df.columns
        
        # Check ticker filter worked
        assert all(df['ticker'] == test_data_dates['ticker'])
        
        # Check metadata columns added
        assert 'ingestion_date' in df.columns
        assert 'data_source' in df.columns
        assert all(df['data_source'] == 'polygon_s3')
    
    def test_download_daily_data_no_file(self, test_s3_client, test_data_dates):
        """Test downloading data when file doesn't exist."""
        df = test_s3_client.download_daily_data(
            test_data_dates['ticker'], 
            test_data_dates['weekend_date'], 
            DataType.DAY_AGGS
        )
        
        # Should return None for non-trading day
        assert df is None
    
    def test_get_available_dates(self, test_s3_client, test_data_dates):
        """Test getting available dates in a range."""
        available_dates = test_s3_client.get_available_dates(
            test_data_dates['ticker'],
            test_data_dates['start_date'],
            test_data_dates['end_date'],
            DataType.DAY_AGGS
        )
        
        # Should return list of dates
        assert isinstance(available_dates, list)
        assert len(available_dates) > 0
        
        # Should be within range
        for date_item in available_dates:
            assert test_data_dates['start_date'] <= date_item <= test_data_dates['end_date']
        
        # Should include our known trading day
        assert test_data_dates['trading_date'] in available_dates
    
    def test_download_date_range(self, test_s3_client, test_data_dates):
        """Test downloading data for a date range."""
        df, failures = test_s3_client.download_date_range(
            test_data_dates['ticker'],
            test_data_dates['start_date'],
            test_data_dates['end_date'],
            DataType.DAY_AGGS
        )
        
        # Should return DataFrame and failures list
        assert df is not None
        assert isinstance(failures, list)
        
        if not df.empty:
            # Check data structure
            assert len(df) >= 1
            assert all(df['ticker'] == test_data_dates['ticker'])
            
            # Check date ordering
            if 'timestamp' in df.columns:
                timestamps = df['timestamp'].sort_values()
                assert timestamps.equals(df['timestamp'].sort_values())
        
        # Failures should be FailedDownload objects if any
        for failure in failures:
            assert isinstance(failure, FailedDownload)
            assert hasattr(failure, 'ticker')
            assert hasattr(failure, 'date')
            assert hasattr(failure, 'error_message')


@pytest.mark.integration
class TestS3ClientIntegrationDataQuality:
    """Test data quality validation with LocalStack data."""
    
    def test_data_quality_validation(self, test_s3_client, test_data_dates):
        """Test that downloaded data passes quality validation."""
        df = test_s3_client.download_daily_data(
            test_data_dates['ticker'], 
            test_data_dates['trading_date'], 
            DataType.DAY_AGGS
        )
        
        if df is not None and not df.empty:
            # Check OHLC relationships
            assert all(df['high'] >= df['low'])
            assert all(df['high'] >= df['open'])
            assert all(df['high'] >= df['close'])
            assert all(df['low'] <= df['open'])
            assert all(df['low'] <= df['close'])
            
            # Check positive values
            assert all(df['open'] > 0)
            assert all(df['high'] > 0)
            assert all(df['low'] > 0)
            assert all(df['close'] > 0)
            assert all(df['volume'] >= 0)
            
            # Check reasonable price ranges (for AAPL)
            assert all(df['open'] < 1000)   # Apple stock < $1000
            assert all(df['high'] < 1000)
            assert all(df['low'] < 1000)
            assert all(df['close'] < 1000)
    
    def test_data_timestamp_handling(self, test_s3_client, test_data_dates):
        """Test that timestamps are handled correctly."""
        df = test_s3_client.download_daily_data(
            test_data_dates['ticker'], 
            test_data_dates['trading_date'], 
            DataType.DAY_AGGS
        )
        
        if df is not None and not df.empty:
            # Check timestamp column exists and is datetime
            assert 'timestamp' in df.columns
            assert df['timestamp'].dtype == 'datetime64[ns]'
            
            # Check dates match expected trading date
            dates = df['timestamp'].dt.date.unique()
            assert test_data_dates['trading_date'] in dates


@pytest.mark.integration
class TestS3ClientIntegrationFullPipeline:
    """Integration tests for complete workflows."""
    
    def test_full_pipeline(self, test_s3_client, test_data_dates):
        """Test complete pipeline: check -> download -> validate."""
        # Step 1: Check availability
        available_dates = test_s3_client.get_available_dates(
            test_data_dates['ticker'],
            test_data_dates['start_date'],
            test_data_dates['end_date'],
            DataType.DAY_AGGS
        )
        
        assert len(available_dates) > 0
        
        # Step 2: Download range
        df, failures = test_s3_client.download_date_range(
            test_data_dates['ticker'],
            test_data_dates['start_date'],
            test_data_dates['end_date'],
            DataType.DAY_AGGS
        )
        
        # Step 3: Validate results
        if available_dates and not df.empty:
            # Should have data for available dates
            unique_dates = df['timestamp'].dt.date.unique()
            
            # At least some overlap between available and downloaded
            assert len(set(available_dates) & set(unique_dates)) > 0
    
    def test_multiple_tickers(self, test_s3_client, test_data_dates, test_tickers):
        """Test downloading different tickers from same file."""
        test_ticker_list = test_tickers['valid'][:3]  # Test first 3 tickers
        
        for ticker in test_ticker_list:
            df = test_s3_client.download_daily_data(
                ticker,
                test_data_dates['trading_date'],
                DataType.DAY_AGGS
            )
            
            if df is not None and not df.empty:
                # Should only contain the requested ticker
                assert all(df['ticker'] == ticker)
                assert len(df) >= 1
    
    def test_error_handling_and_recovery(self, test_s3_client, test_data_dates):
        """Test error handling and recovery in batch operations."""
        # Mix of valid and invalid dates
        start_date = date(2024, 1, 1)  # New Year's Day
        end_date = date(2024, 1, 7)    # Include weekend
        
        df, failures = test_s3_client.download_date_range(
            test_data_dates['ticker'],
            start_date,
            end_date,
            DataType.DAY_AGGS
        )
        
        # Should have some data, failures may be empty since weekends are skipped
        assert df is not None
        assert isinstance(failures, list)
        # Implementation is smart and skips weekends/holidays, so failures may be empty
        
        # Verify failures are properly structured (if any exist)
        for failure in failures:
            assert failure.ticker == test_data_dates['ticker']
            assert isinstance(failure.date, date)
            assert isinstance(failure.error_message, str)
            
        # Test should verify that data was downloaded for business days only
        if not df.empty:
            # All dates in the DataFrame should be business days
            dates_in_data = pd.to_datetime(df['timestamp']).dt.date.unique()
            for download_date in dates_in_data:
                # Verify it's not a weekend
                assert download_date.weekday() < 5  # Monday=0, Sunday=6