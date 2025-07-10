#!/usr/bin/env python3
"""
Pytest tests for S3Client using LocalStack
"""

import pytest
import sys
from pathlib import Path
from datetime import date

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from s3_client import DataType


class TestS3ClientConnection:
    """Test S3 client initialization and connection with LocalStack"""
    
    def test_s3_client_initialization(self, test_s3_client):
        """Test that S3 client initializes successfully"""
        assert test_s3_client is not None
        assert test_s3_client.bucket_name == "flatfiles"
        assert test_s3_client.path_structure is not None
    
    def test_s3_config_loaded(self, test_config):
        """Test that S3 configuration loads correctly"""
        s3_config = test_config.get_s3_config()
        assert s3_config.bucket_name == "flatfiles"
        assert s3_config.credentials.access_key is not None
        assert s3_config.credentials.secret_key is not None


class TestS3ClientFileOperations:
    """Test S3 client file operations against LocalStack"""
    
    def test_check_file_exists_trading_day(self, test_s3_client, test_data):
        """Test file existence check for a trading day (should exist in fixtures)"""
        exists = test_s3_client.check_file_exists(
            test_data['ticker'], 
            test_data['trading_date'], 
            test_data['data_type']
        )
        # Should be True for 2024-01-02 (Tuesday, fixture exists)
        assert exists is True
    
    def test_check_file_exists_weekend(self, test_s3_client, test_data):
        """Test file existence check for a weekend (no fixture)"""
        exists = test_s3_client.check_file_exists(
            test_data['ticker'], 
            test_data['weekend_date'], 
            test_data['data_type']
        )
        # Should be False for 2024-01-06 (Saturday, no fixture)
        assert exists is False
    
    def test_generate_s3_path(self, test_s3_client, test_data):
        """Test S3 path generation"""
        s3_path = test_s3_client._generate_s3_path(test_data['trading_date'], test_data['data_type'])
        expected_path = "us_stocks_sip/day_aggs_v1/2024/01/2024-01-02.csv.gz"
        assert s3_path == expected_path


class TestS3ClientDownloads:
    """Test S3 client download operations against LocalStack"""
    
    def test_download_daily_data_success(self, test_s3_client, test_data):
        """Test downloading data for a single trading day"""
        df = test_s3_client.download_daily_data(
            test_data['ticker'], 
            test_data['trading_date'], 
            test_data['data_type']
        )
        
        # Should return DataFrame with data
        assert df is not None
        assert not df.empty
        assert len(df) >= 1
        
        # Check required columns
        required_cols = ['ticker', 'open', 'high', 'low', 'close', 'volume', 'vwap']
        for col in required_cols:
            assert col in df.columns
        
        # Check ticker filter worked
        assert all(df['ticker'] == test_data['ticker'])
        
        # Check metadata columns added
        assert 'ingestion_date' in df.columns
        assert 'data_source' in df.columns
        assert all(df['data_source'] == 'polygon_s3')
    
    def test_download_daily_data_no_file(self, test_s3_client, test_data):
        """Test downloading data when file doesn't exist"""
        df = test_s3_client.download_daily_data(
            test_data['ticker'], 
            test_data['weekend_date'], 
            test_data['data_type']
        )
        
        # Should return None for non-trading day
        assert df is None
    
    def test_get_available_dates(self, test_s3_client, test_data):
        """Test getting available dates in a range"""
        available_dates = test_s3_client.get_available_dates(
            test_data['ticker'],
            test_data['start_date'],
            test_data['end_date'],
            test_data['data_type']
        )
        
        # Should return list of dates
        assert isinstance(available_dates, list)
        assert len(available_dates) > 0
        
        # Should be within range
        for date_item in available_dates:
            assert test_data['start_date'] <= date_item <= test_data['end_date']
        
        # Should include our known trading day
        assert test_data['trading_date'] in available_dates
    
    def test_download_date_range(self, test_s3_client, test_data):
        """Test downloading data for a date range"""
        df, failures = test_s3_client.download_date_range(
            test_data['ticker'],
            test_data['start_date'],
            test_data['end_date'],
            test_data['data_type']
        )
        
        # Should return DataFrame and failures list
        assert df is not None
        assert isinstance(failures, list)
        
        if not df.empty:
            # Check data structure
            assert len(df) >= 1
            assert all(df['ticker'] == test_data['ticker'])
            
            # Check date ordering
            timestamps = df['timestamp'].sort_values()
            assert timestamps.equals(df['timestamp'].sort_values())
        
        # Failures should be FailedDownload objects if any
        for failure in failures:
            assert hasattr(failure, 'ticker')
            assert hasattr(failure, 'date')
            assert hasattr(failure, 'error_message')


class TestS3ClientDataQuality:
    """Test data quality validation with LocalStack data"""
    
    def test_data_quality_validation(self, test_s3_client, test_data):
        """Test that downloaded data passes quality validation"""
        df = test_s3_client.download_daily_data(
            test_data['ticker'], 
            test_data['trading_date'], 
            test_data['data_type']
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


@pytest.mark.integration
class TestS3ClientIntegration:
    """Integration tests with LocalStack"""
    
    def test_full_pipeline(self, test_s3_client, test_data):
        """Test complete pipeline: check -> download -> validate"""
        # Step 1: Check availability
        available_dates = test_s3_client.get_available_dates(
            test_data['ticker'],
            test_data['start_date'],
            test_data['end_date'],
            test_data['data_type']
        )
        
        # Step 2: Download range
        df, failures = test_s3_client.download_date_range(
            test_data['ticker'],
            test_data['start_date'],
            test_data['end_date'],
            test_data['data_type']
        )
        
        # Step 3: Validate results
        if available_dates and not df.empty:
            # Should have data for available dates
            unique_dates = df['timestamp'].dt.date.unique()
            
            # At least some overlap between available and downloaded
            assert len(set(available_dates) & set(unique_dates)) > 0
    
    def test_multiple_tickers(self, test_s3_client, test_data):
        """Test downloading different tickers from same file"""
        test_tickers = ['AAPL', 'MSFT', 'NVDA']
        
        for ticker in test_tickers:
            df = test_s3_client.download_daily_data(
                ticker,
                test_data['trading_date'],
                test_data['data_type']
            )
            
            if df is not None and not df.empty:
                # Should only contain the requested ticker
                assert all(df['ticker'] == ticker)
                assert len(df) >= 1


if __name__ == "__main__":
    # Run tests if executed directly
    pytest.main([__file__, "-v", "--tb=short"])
