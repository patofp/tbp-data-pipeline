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
            data_source='test_integration',
            on_conflict='update',
            batch_size=None,
            throttle_rows_per_second=None
        )
        
        # Verify results
        assert result['total_rows'] == len(sample_market_data)
        assert result['successful'] > 0
        assert result['failed'] == 0
        assert len(result['failed_details']) == 0
        assert result['duration_seconds'] > 0
        assert result['rows_per_second'] > 0
        
        # Verify data was actually inserted into database
        with test_market_data_client._get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    SELECT COUNT(*) FROM trading.market_data_raw 
                    WHERE data_source = 'test_integration'
                """)
                count = cursor.fetchone()[0]
                assert count == len(sample_market_data)
            finally:
                cursor.close()
    
    def test_insert_duplicate_data_handling(self, test_market_data_client, sample_market_data):
        """Test handling of duplicate data with real database constraints."""
        # Get a subset of data without duplicates within the batch
        unique_data = sample_market_data[sample_market_data['ticker'] == 'AAPL'].head(3)
        
        # First insertion should succeed
        result1 = test_market_data_client.insert_batch(
            unique_data,
            timeframe='1d',
            data_source='test_duplicates',
            on_conflict='update',
            batch_size=None,
            throttle_rows_per_second=None
        )
        
        assert result1['successful'] == len(unique_data)
        assert result1['failed'] == 0
        
        # Second insertion of same data should handle duplicates gracefully
        # The ON CONFLICT DO UPDATE should update the existing records
        result2 = test_market_data_client.insert_batch(
            unique_data,
            timeframe='1d',
            data_source='test_duplicates',
            on_conflict='update',
            batch_size=None,
            throttle_rows_per_second=None
        )
        
        # With ON CONFLICT DO UPDATE, the rows should be updated successfully
        assert result2['successful'] == len(unique_data)
        assert result2['failed'] == 0
    
    def test_insert_empty_dataframe(self, test_market_data_client):
        """Test inserting empty DataFrame with real database."""
        empty_df = pd.DataFrame()
        
        result = test_market_data_client.insert_batch(
            empty_df,
            timeframe='1d',
            data_source='test_empty',
            on_conflict='update',
            batch_size=None,
            throttle_rows_per_second=None
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
            data_source='test_timestamp',
            on_conflict='update',
            batch_size=None,
            throttle_rows_per_second=None
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
            data_source='test_gaps',
            on_conflict='update',
            batch_size=None,
            throttle_rows_per_second=None
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
        # get_data_gaps returns List[date], not List[Dict]
        assert date(2024, 1, 4) in gaps
    
    def test_get_data_gaps_no_gaps(self, test_market_data_client, sample_market_data):
        """Test gap detection when no gaps exist."""
        # Insert continuous data
        continuous_data = sample_market_data[sample_market_data['ticker'] == 'AAPL'].copy()
        test_market_data_client.insert_batch(
            continuous_data,
            timeframe='1d',
            data_source='test_no_gaps',
            on_conflict='update',
            batch_size=None,
            throttle_rows_per_second=None
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
        # get_data_gaps returns List[date], not List[Dict]
        weekday_gaps = [gap for gap in gaps if gap.weekday() < 5]
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
            data_source='test_stats',
            on_conflict='update',
            batch_size=None,
            throttle_rows_per_second=None
        )
        
        # Get stats for AAPL
        stats = test_market_data_client.get_ticker_stats(
            ticker='AAPL'
        )
        
        assert stats is not None
        assert 'total_records' in stats
        assert 'first_date' in stats
        assert 'last_date' in stats
        assert 'days_of_data' in stats
        
        # Should have the right number of AAPL records
        aapl_count = len(sample_market_data[sample_market_data['ticker'] == 'AAPL'])
        assert stats['total_records'] == aapl_count
        
        assert isinstance(stats['first_date'], datetime)
        assert isinstance(stats['last_date'], datetime)
        assert stats['days_of_data'] >= 0
    
    def test_get_ticker_stats_no_data(self, test_market_data_client):
        """Test getting stats when no data exists."""
        stats = test_market_data_client.get_ticker_stats(
            ticker='NONEXISTENT'
        )
        
        # Implementation returns a dict with error info, not None
        assert stats is not None
        assert stats['total_records'] == 0
        assert 'error' in stats


@pytest.mark.integration
class TestDeleteDateRangeIntegration:
    """Test date range deletion with real database."""
    
    def test_delete_date_range_partial(self, test_market_data_client, sample_market_data):
        """Test deleting a subset of data by date range."""
        # Insert sample data
        test_market_data_client.insert_batch(
            sample_market_data,
            timeframe='1d',
            data_source='test_delete',
            on_conflict='update',
            batch_size=None,
            throttle_rows_per_second=None
        )
        
        # Count initial records
        with test_market_data_client._get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    SELECT COUNT(*) FROM trading.market_data_raw 
                    WHERE data_source = 'test_delete'
                """)
                initial_count = cursor.fetchone()[0]
                
                # Delete data for a specific date range
                deleted_count = test_market_data_client.delete_date_range(
                    ticker='AAPL',
                    start_date=date(2024, 1, 2),
                    end_date=date(2024, 1, 3),
                    timeframe='1d',
                    data_source='test_delete',
                    dry_run=False
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
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
            timeframe='1d',
            data_source='test_no_delete',
            dry_run=False
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
            data_source='test_summary',
            on_conflict='update',
            batch_size=None,
            throttle_rows_per_second=None
        )
        
        # Get summary
        summary = test_market_data_client.get_data_summary(start_date=None, end_date=None)
        
        assert isinstance(summary, pd.DataFrame)
        assert len(summary) > 0
        
        # Check structure of summary DataFrame
        expected_columns = ['ticker', 'timeframe', 'data_source', 'record_count', 'first_date', 'last_date', 'trading_days']
        for col in expected_columns:
            assert col in summary.columns
        
        # Should include all unique tickers from sample data
        summary_tickers = set(summary['ticker'].unique())
        sample_tickers = set(sample_market_data['ticker'].unique())
        assert summary_tickers == sample_tickers
    
    def test_get_data_summary_no_data(self, test_market_data_client):
        """Test getting summary when no data exists."""
        summary = test_market_data_client.get_data_summary(start_date=None, end_date=None)
        
        assert isinstance(summary, pd.DataFrame)
        assert len(summary) == 0


@pytest.mark.integration
class TestPerformanceIntegration:
    """Test performance characteristics with real database."""
    
    def test_large_batch_performance(self, test_market_data_client):
        """Test performance with larger batches of data."""
        # Generate larger dataset - avoid duplicates by using unique combinations
        large_data = []
        base_tickers = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'META']
        base_date = date(2024, 1, 2)
        
        # Generate 500 unique records (5 tickers * 100 days each)
        for day_offset in range(100):  # 100 days
            for ticker_idx, ticker in enumerate(base_tickers):  # 5 tickers
                i = day_offset * len(base_tickers) + ticker_idx
                current_date = base_date + timedelta(days=day_offset)
                
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
            data_source='test_performance',
            on_conflict='update',
            batch_size=None,
            throttle_rows_per_second=None
        )
        
        assert result['total_rows'] == len(large_df)
        assert result['successful'] > 0
        assert result['duration_seconds'] > 0
        assert result['rows_per_second'] > 0
        
        # Performance should be reasonable (more than 50 rows per second)
        assert result['rows_per_second'] > 50
    
    def test_concurrent_operations(self, test_market_data_client, sample_market_data):
        """Test that multiple operations can work with same client."""
        # Insert data
        insert_result = test_market_data_client.insert_batch(
            sample_market_data,
            timeframe='1d',
            data_source='test_concurrent',
            on_conflict='update',
            batch_size=None,
            throttle_rows_per_second=None
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
            ticker='AAPL'
        )
        
        assert stats is not None
        assert stats['total_records'] > 0


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
            data_source='test_constraint',
            on_conflict='update',
            batch_size=None,
            throttle_rows_per_second=None
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
            data_source='test_null',
            on_conflict='update',
            batch_size=None,
            throttle_rows_per_second=None
        )
        
        # Should report failure or skip null rows
        assert result['failed'] > 0 or result['total_rows'] == 0