"""
Integration tests for MarketDataClient class.

These tests use real database connections and don't mock external dependencies.
They are slower but test the full integration with PostgreSQL + TimescaleDB.
"""
import pytest
from datetime import datetime, date, timedelta
import pandas as pd
import psycopg2
from psycopg2 import sql

from src.database.market_data import MarketDataClient


@pytest.mark.integration
class TestDatabaseIntegration:
    """Test integration with real PostgreSQL database."""
    
    def test_client_initialization(self, test_market_data_client):
        """Test that client initializes correctly with real database."""
        assert test_market_data_client.table_name == "trading.market_data_raw"
        assert hasattr(test_market_data_client, 'logger')
        assert test_market_data_client._operation_count == 0
        assert test_market_data_client._total_time == 0.0
    
    def test_database_schema_exists(self, test_db_connection):
        """Test that the trading schema and market_data_raw table exist."""
        cursor = test_db_connection.cursor()
        try:
            # Check schema exists
            cursor.execute("""
                SELECT schema_name FROM information_schema.schemata 
                WHERE schema_name = 'trading'
            """)
            schema_result = cursor.fetchone()
            assert schema_result is not None, "trading schema should exist"
            
            # Check table exists
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'trading' AND table_name = 'market_data_raw'
            """)
            table_result = cursor.fetchone()
            assert table_result is not None, "market_data_raw table should exist"
            
        finally:
            cursor.close()


@pytest.mark.integration
class TestInsertBatchIntegration:
    """Test batch insert with real database operations."""
    
    def test_insert_batch_real_data(self, test_market_data_client, sample_market_data):
        """Test inserting real market data into database."""
        # Insert the sample data
        result = test_market_data_client.insert_batch(
            sample_market_data,
            timeframe='1d',
            data_source='test_integration'
        )
        
        # Verify results
        assert result['total_rows'] == len(sample_market_data)
        assert result['successful'] > 0
        assert result['failed'] == 0
        assert len(result['failed_details']) == 0
        assert result['duration_seconds'] > 0
        assert result['rows_per_second'] > 0
        
        # Verify data was actually inserted into database
        cursor = test_market_data_client._get_connection().cursor()
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM trading.market_data_raw 
                WHERE data_source = 'test_integration'
            """)
            count = cursor.fetchone()[0]
            assert count == len(sample_market_data)
        finally:
            cursor.close()
    
    def test_insert_duplicate_data_handling(self, test_market_data_client, sample_duplicate_data):
        """Test handling of duplicate data with real database constraints."""
        # First insertion should succeed
        result1 = test_market_data_client.insert_batch(
            sample_duplicate_data,
            timeframe='1d',
            data_source='test_duplicates'
        )
        
        assert result1['successful'] > 0
        
        # Second insertion of same data should handle duplicates
        result2 = test_market_data_client.insert_batch(
            sample_duplicate_data,
            timeframe='1d',
            data_source='test_duplicates'
        )
        
        # Should have some failures due to duplicates
        assert result2['failed'] > 0 or result2['successful'] == 0
    
    def test_insert_empty_dataframe(self, test_market_data_client):
        """Test inserting empty DataFrame with real database."""
        empty_df = pd.DataFrame()
        
        result = test_market_data_client.insert_batch(
            empty_df,
            timeframe='1d',
            data_source='test_empty'
        )
        
        assert result['total_rows'] == 0
        assert result['successful'] == 0
        assert result['failed'] == 0


@pytest.mark.integration
class TestGetLastTimestampIntegration:
    """Test get_last_timestamp with real database."""
    
    def test_get_last_timestamp_with_data(self, test_market_data_client, sample_market_data):
        """Test getting last timestamp when data exists."""
        # Insert sample data first
        test_market_data_client.insert_batch(
            sample_market_data,
            timeframe='1d',
            data_source='test_timestamp'
        )
        
        # Get last timestamp for AAPL
        last_timestamp = test_market_data_client.get_last_timestamp(
            ticker='AAPL',
            timeframe='1d',
            data_source='test_timestamp'
        )
        
        assert last_timestamp is not None
        assert isinstance(last_timestamp, datetime)
        
        # Should be the maximum timestamp for AAPL in our sample data
        aapl_data = sample_market_data[sample_market_data['ticker'] == 'AAPL']
        expected_max = aapl_data['timestamp'].max()
        assert last_timestamp == expected_max
    
    def test_get_last_timestamp_no_data(self, test_market_data_client):
        """Test getting last timestamp when no data exists."""
        last_timestamp = test_market_data_client.get_last_timestamp(
            ticker='NONEXISTENT',
            timeframe='1d',
            data_source='test_empty'
        )
        
        assert last_timestamp is None


@pytest.mark.integration
class TestGetDataGapsIntegration:
    """Test data gap detection with real database."""
    
    def test_get_data_gaps_with_gaps(self, test_market_data_client, sample_data_with_gaps):
        """Test detecting gaps in real data."""
        # Insert data with known gaps
        test_market_data_client.insert_batch(
            sample_data_with_gaps,
            timeframe='1d',
            data_source='test_gaps'
        )
        
        # Check for gaps
        gaps = test_market_data_client.get_data_gaps(
            ticker='AAPL',
            timeframe='1d',
            data_source='test_gaps',
            start_date=date(2024, 1, 2),
            end_date=date(2024, 1, 10)
        )
        
        assert isinstance(gaps, list)
        # Should find the missing Jan 4th (Thursday)
        gap_dates = [gap['date'] for gap in gaps]
        assert date(2024, 1, 4) in gap_dates
    
    def test_get_data_gaps_no_gaps(self, test_market_data_client, sample_market_data):
        """Test gap detection when no gaps exist."""
        # Insert continuous data
        continuous_data = sample_market_data[sample_market_data['ticker'] == 'AAPL'].copy()
        test_market_data_client.insert_batch(
            continuous_data,
            timeframe='1d',
            data_source='test_no_gaps'
        )
        
        # Check for gaps in a range that should have no gaps
        gaps = test_market_data_client.get_data_gaps(
            ticker='AAPL',
            timeframe='1d',
            data_source='test_no_gaps',
            start_date=date(2024, 1, 2),
            end_date=date(2024, 1, 5)
        )
        
        # Should find no gaps (excluding weekends)
        weekday_gaps = [gap for gap in gaps if gap['date'].weekday() < 5]
        assert len(weekday_gaps) == 0


@pytest.mark.integration
class TestGetTickerStatsIntegration:
    """Test ticker statistics with real database."""
    
    def test_get_ticker_stats_with_data(self, test_market_data_client, sample_market_data):
        """Test getting ticker statistics from real database."""
        # Insert sample data
        test_market_data_client.insert_batch(
            sample_market_data,
            timeframe='1d',
            data_source='test_stats'
        )
        
        # Get stats for AAPL
        stats = test_market_data_client.get_ticker_stats(
            ticker='AAPL',
            timeframe='1d',
            data_source='test_stats'
        )
        
        assert stats is not None
        assert 'record_count' in stats
        assert 'first_date' in stats
        assert 'last_date' in stats
        assert 'date_range_days' in stats
        
        # Should have the right number of AAPL records
        aapl_count = len(sample_market_data[sample_market_data['ticker'] == 'AAPL'])
        assert stats['record_count'] == aapl_count
        
        assert isinstance(stats['first_date'], date)
        assert isinstance(stats['last_date'], date)
        assert stats['date_range_days'] >= 0
    
    def test_get_ticker_stats_no_data(self, test_market_data_client):
        """Test getting stats when no data exists."""
        stats = test_market_data_client.get_ticker_stats(
            ticker='NONEXISTENT',
            timeframe='1d',
            data_source='test_no_stats'
        )
        
        assert stats is None


@pytest.mark.integration
class TestDeleteDateRangeIntegration:
    """Test date range deletion with real database."""
    
    def test_delete_date_range_partial(self, test_market_data_client, sample_market_data):
        """Test deleting a subset of data by date range."""
        # Insert sample data
        test_market_data_client.insert_batch(
            sample_market_data,
            timeframe='1d',
            data_source='test_delete'
        )
        
        # Count initial records
        cursor = test_market_data_client._get_connection().cursor()
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM trading.market_data_raw 
                WHERE data_source = 'test_delete'
            """)
            initial_count = cursor.fetchone()[0]
            
            # Delete data for a specific date range
            deleted_count = test_market_data_client.delete_date_range(
                ticker='AAPL',
                timeframe='1d',
                data_source='test_delete',
                start_date=date(2024, 1, 2),
                end_date=date(2024, 1, 3)
            )
            
            assert deleted_count > 0
            
            # Verify deletion
            cursor.execute("""
                SELECT COUNT(*) FROM trading.market_data_raw 
                WHERE data_source = 'test_delete'
            """)
            final_count = cursor.fetchone()[0]
            
            assert final_count == initial_count - deleted_count
            
        finally:
            cursor.close()
    
    def test_delete_date_range_no_matches(self, test_market_data_client):
        """Test deleting when no data matches the criteria."""
        deleted_count = test_market_data_client.delete_date_range(
            ticker='NONEXISTENT',
            timeframe='1d',
            data_source='test_no_delete',
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31)
        )
        
        assert deleted_count == 0


@pytest.mark.integration
class TestGetDataSummaryIntegration:
    """Test data summary with real database."""
    
    def test_get_data_summary_with_data(self, test_market_data_client, sample_market_data):
        """Test getting data summary from real database."""
        # Insert sample data
        test_market_data_client.insert_batch(
            sample_market_data,
            timeframe='1d',
            data_source='test_summary'
        )
        
        # Get summary
        summary = test_market_data_client.get_data_summary(
            timeframe='1d',
            data_source='test_summary'
        )
        
        assert isinstance(summary, list)
        assert len(summary) > 0
        
        # Check structure of summary records
        for record in summary:
            assert 'ticker' in record
            assert 'record_count' in record
            assert 'first_date' in record
            assert 'last_date' in record
            assert 'date_range_days' in record
            
            assert isinstance(record['ticker'], str)
            assert isinstance(record['record_count'], int)
            assert isinstance(record['first_date'], date)
            assert isinstance(record['last_date'], date)
            assert isinstance(record['date_range_days'], int)
        
        # Should include all unique tickers from sample data
        summary_tickers = {record['ticker'] for record in summary}
        sample_tickers = set(sample_market_data['ticker'].unique())
        assert summary_tickers == sample_tickers
    
    def test_get_data_summary_no_data(self, test_market_data_client):
        """Test getting summary when no data exists."""
        summary = test_market_data_client.get_data_summary(
            timeframe='1d',
            data_source='test_no_summary'
        )
        
        assert isinstance(summary, list)
        assert len(summary) == 0


@pytest.mark.integration
class TestPerformanceIntegration:
    """Test performance characteristics with real database."""
    
    def test_large_batch_performance(self, test_market_data_client):
        """Test performance with larger batches of data."""
        # Generate larger dataset
        large_data = []
        tickers = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'META'] * 20  # 100 tickers
        base_date = date(2024, 1, 2)
        
        for i, ticker in enumerate(tickers):
            current_date = base_date + timedelta(days=i % 30)  # Spread over 30 days
            
            large_data.append({
                'ticker': ticker,
                'timestamp': datetime.combine(current_date, datetime.min.time()),
                'open': 100.0 + i,
                'high': 102.0 + i,
                'low': 99.0 + i,
                'close': 101.0 + i,
                'volume': 1000000 + i,
                'transactions': 10000 + i
            })
        
        large_df = pd.DataFrame(large_data)
        
        # Test insertion performance
        result = test_market_data_client.insert_batch(
            large_df,
            timeframe='1d',
            data_source='test_performance'
        )
        
        assert result['total_rows'] == len(large_df)
        assert result['successful'] > 0
        assert result['duration_seconds'] > 0
        assert result['rows_per_second'] > 0
        
        # Performance should be reasonable (more than 100 rows per second)
        assert result['rows_per_second'] > 100
    
    def test_concurrent_operations(self, test_market_data_client, sample_market_data):
        """Test that multiple operations can work with same client."""
        # Insert data
        insert_result = test_market_data_client.insert_batch(
            sample_market_data,
            timeframe='1d',
            data_source='test_concurrent'
        )
        
        assert insert_result['successful'] > 0
        
        # Query operations should work after insert
        last_timestamp = test_market_data_client.get_last_timestamp(
            ticker='AAPL',
            timeframe='1d',
            data_source='test_concurrent'
        )
        
        assert last_timestamp is not None
        
        # Stats should work
        stats = test_market_data_client.get_ticker_stats(
            ticker='AAPL',
            timeframe='1d',
            data_source='test_concurrent'
        )
        
        assert stats is not None
        assert stats['record_count'] > 0


@pytest.mark.integration
class TestErrorHandlingIntegration:
    """Test error handling with real database constraints."""
    
    def test_invalid_ticker_constraint(self, test_market_data_client):
        """Test handling of data that violates database constraints."""
        # Create data with invalid ticker (too long)
        invalid_data = pd.DataFrame({
            'ticker': ['A' * 50],  # Too long for VARCHAR(20)
            'timestamp': [datetime(2024, 1, 2)],
            'open': [100.0],
            'high': [102.0],
            'low': [99.0],
            'close': [101.0],
            'volume': [1000000],
            'transactions': [10000]
        })
        
        # Should handle the constraint violation gracefully
        result = test_market_data_client.insert_batch(
            invalid_data,
            timeframe='1d',
            data_source='test_constraint'
        )
        
        # Should report failure
        assert result['failed'] > 0 or result['total_rows'] == 0
    
    def test_null_required_fields(self, test_market_data_client):
        """Test handling of null values in required fields."""
        # Create data with null ticker
        null_data = pd.DataFrame({
            'ticker': [None],
            'timestamp': [datetime(2024, 1, 2)],
            'open': [100.0],
            'high': [102.0],
            'low': [99.0],
            'close': [101.0],
            'volume': [1000000],
            'transactions': [10000]
        })
        
        # Should handle the null constraint violation
        result = test_market_data_client.insert_batch(
            null_data,
            timeframe='1d',
            data_source='test_null'
        )
        
        # Should report failure or skip null rows
        assert result['failed'] > 0 or result['total_rows'] == 0