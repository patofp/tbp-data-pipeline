"""
Unit tests for MarketDataClient class.

These tests use mocks and stubs to test business logic in isolation.
They are fast and don't require external dependencies.
"""
import pytest
from datetime import datetime, date, timedelta
from unittest.mock import Mock, patch, MagicMock
import time
import logging

import pandas as pd
import psycopg2
from psycopg2 import sql

from src.database.market_data import MarketDataClient


@pytest.mark.unit
class TestInitialization:
    """Test client initialization and configuration."""
    
    def test_init(self, mock_db_config, mock_pool):
        """Test that client initializes correctly with config and pool."""
        client = MarketDataClient(mock_db_config, mock_pool)
        
        assert client.config == mock_db_config
        assert client._pool == mock_pool
        assert hasattr(client, 'logger')
        assert client._operation_count == 0
        assert client._total_time == 0.0
    
    def test_table_name(self, mock_db_config, mock_pool):
        """Test that client uses correct table name."""
        client = MarketDataClient(mock_db_config, mock_pool)
        assert client.table_name == "trading.market_data_raw"
    
    def test_default_columns(self, mock_db_config, mock_pool):
        """Test that client has expected columns."""
        client = MarketDataClient(mock_db_config, mock_pool)
        expected_columns = [
            'ticker', 'timestamp', 'timeframe', 'data_source',
            'open', 'high', 'low', 'close', 'volume', 
            'transactions', 'ingested_at'
        ]
        assert client.default_columns == expected_columns


@pytest.mark.unit
class TestInsertBatch:
    """Test batch insert business logic with mocks."""
    
    @pytest.fixture
    def market_data_client(self, mock_db_config, mock_pool):
        """Create client with mocked dependencies."""
        return MarketDataClient(mock_db_config, mock_pool)
    
    def test_insert_batch_success(self, market_data_client, sample_dataframe):
        """Test successful insertion of a small batch."""
        # Mock the internal method
        market_data_client._insert_batch_with_retry = Mock(
            return_value={'successful': 5, 'failed': []}
        )
        
        result = market_data_client.insert_batch(
            sample_dataframe,
            timeframe='1d',
            data_source='test',
            on_conflict='update',
            batch_size=None,
            throttle_rows_per_second=None
        )
        
        assert result['total_rows'] == 5
        assert result['successful'] == 5
        assert result['failed'] == 0
        assert len(result['failed_details']) == 0
        assert result['duration_seconds'] > 0
        assert result['rows_per_second'] > 0
    
    def test_insert_batch_empty_dataframe(self, market_data_client):
        """Test handling of empty DataFrame."""
        empty_df = pd.DataFrame()
        
        # Mock prepare_dataframe_for_insert for empty DataFrame
        with patch('src.database.market_data.prepare_dataframe_for_insert') as mock_prepare:
            mock_prepare.return_value = {
                'tuples': [],
                'tracking': [],
                'column_names': market_data_client.default_columns
            }
            
            result = market_data_client.insert_batch(
                empty_df,
                timeframe='1d',
                data_source='test',
                on_conflict='update',
                batch_size=None,
                throttle_rows_per_second=None
            )
        
        assert result['total_rows'] == 0
        assert result['successful'] == 0
        assert result['failed'] == 0
    
    def test_insert_batch_with_conflict_update(self, market_data_client, sample_dataframe):
        """Test ON CONFLICT DO UPDATE functionality."""
        market_data_client._insert_batch_with_retry = Mock(
            return_value={'successful': 5, 'failed': []}
        )
        
        result = market_data_client.insert_batch(
            sample_dataframe,
            timeframe='1d',
            data_source='polygon_s3',
            on_conflict='update',
            batch_size=None,
            throttle_rows_per_second=None
        )
        
        # Verify conflict config was passed correctly
        call_args = market_data_client._insert_batch_with_retry.call_args[0]
        conflict_config = call_args[3]
        
        assert conflict_config['action'] == 'update'
        assert conflict_config['columns'] == ['ticker', 'timestamp', 'timeframe', 'data_source']
        assert 'open' in conflict_config['update_columns']
    
    def test_insert_batch_with_conflict_nothing(self, market_data_client, sample_dataframe):
        """Test ON CONFLICT DO NOTHING functionality."""
        market_data_client._insert_batch_with_retry = Mock(
            return_value={'successful': 5, 'failed': []}
        )
        
        result = market_data_client.insert_batch(
            sample_dataframe,
            timeframe='1d',
            data_source='polygon_s3',
            on_conflict='nothing',
            batch_size=None,
            throttle_rows_per_second=None
        )
        
        # Verify conflict config
        call_args = market_data_client._insert_batch_with_retry.call_args[0]
        conflict_config = call_args[3]
        
        assert conflict_config['action'] == 'nothing'
    
    def test_insert_batch_with_conflict_error(self, market_data_client, sample_dataframe):
        """Test that duplicates fail without ON CONFLICT."""
        market_data_client._insert_batch_with_retry = Mock(
            return_value={'successful': 5, 'failed': []}
        )
        
        result = market_data_client.insert_batch(
            sample_dataframe,
            timeframe='1d',
            data_source='polygon_s3',
            on_conflict='error',
            batch_size=None,
            throttle_rows_per_second=None
        )
        
        # Verify no conflict config
        call_args = market_data_client._insert_batch_with_retry.call_args[0]
        conflict_config = call_args[3]
        
        assert conflict_config is None
    
    def test_insert_batch_auto_batch_size(self, market_data_client, sample_dataframe):
        """Test automatic batch size based on timeframe."""
        with patch('src.database.market_data.estimate_batch_size', return_value=1000) as mock_estimate:
            with patch('src.database.market_data.prepare_dataframe_for_insert') as mock_prepare:
                mock_prepare.return_value = {
                    'tuples': [('test_tuple',) * 11] * 5,
                    'tracking': [{'index': i, 'original_index': i, 'identifier': f'test_{i}'} for i in range(5)],
                    'column_names': market_data_client.default_columns
                }
                
                market_data_client._insert_batch_with_retry = Mock(
                    return_value={'successful': 5, 'failed': []}
                )
                
                result = market_data_client.insert_batch(
                    sample_dataframe,
                    timeframe='1m',
                    data_source='polygon_s3',
                    on_conflict='update',
                    batch_size=None,
                    throttle_rows_per_second=None
                )
                
                mock_estimate.assert_called_once_with('1m', 'insert')
    
    def test_insert_batch_custom_batch_size(self, market_data_client, sample_dataframe):
        """Test that custom batch_size is respected."""
        # Create larger dataframe to test batching
        large_df = pd.concat([sample_dataframe] * 20, ignore_index=True)
        
        market_data_client._insert_batch_with_retry = Mock(
            return_value={'successful': 10, 'failed': []}
        )
        
        result = market_data_client.insert_batch(
            large_df,
            timeframe='1d',
            data_source='polygon_s3',
            on_conflict='update',
            batch_size=10,
            throttle_rows_per_second=None
        )
        
        # Should be called multiple times with batch_size=10
        call_count = market_data_client._insert_batch_with_retry.call_count
        assert call_count == 10  # 100 rows / 10 per batch
    
    @patch('time.sleep')
    def test_insert_batch_throttling(self, mock_sleep, market_data_client, sample_dataframe):
        """Test that throttling works correctly."""
        # Create larger dataframe
        large_df = pd.concat([sample_dataframe] * 20, ignore_index=True)
        
        with patch('src.database.market_data.calculate_insert_throttle', return_value=0.1):
            market_data_client._insert_batch_with_retry = Mock(
                return_value={'successful': 10, 'failed': []}
            )
            
            result = market_data_client.insert_batch(
                large_df,
                timeframe='1d',
                data_source='polygon_s3',
                on_conflict='update',
                batch_size=10,
                throttle_rows_per_second=100
            )
            
            # Verify sleep was called for batches after the first
            assert mock_sleep.call_count > 0
    
    def test_insert_batch_partial_failure(self, market_data_client, sample_dataframe):
        """Test handling when some rows fail, others succeed."""
        failed_rows = [
            {
                'index': 0,
                'original_index': 2,
                'identifier': 'GOOGL_2024-01-02',
                'ticker': 'GOOGL',
                'date': '2024-01-02',
                'error': 'Constraint violation',
                'error_type': 'IntegrityError'
            }
        ]
        
        market_data_client._insert_batch_with_retry = Mock(
            return_value={'successful': 4, 'failed': failed_rows}
        )
        
        result = market_data_client.insert_batch(sample_dataframe, timeframe='1d', data_source='polygon_s3', on_conflict='update', batch_size=None, throttle_rows_per_second=None)
        
        assert result['successful'] == 4
        assert result['failed'] == 1
        assert len(result['failed_details']) == 1
        assert result['failed_details'][0]['ticker'] == 'GOOGL'
    
    def test_insert_batch_all_fail(self, market_data_client, sample_dataframe):
        """Test handling when all rows fail."""
        failed_rows = [
            {
                'index': i,
                'original_index': i,
                'identifier': f'{row.ticker}_{row.timestamp}',
                'ticker': row.ticker,
                'date': str(row.timestamp),
                'error': 'Database error',
                'error_type': 'DatabaseError'
            }
            for i, row in sample_dataframe.iterrows()
        ]
        
        market_data_client._insert_batch_with_retry = Mock(
            return_value={'successful': 0, 'failed': failed_rows}
        )
        
        result = market_data_client.insert_batch(sample_dataframe, timeframe='1d', data_source='polygon_s3', on_conflict='update', batch_size=None, throttle_rows_per_second=None)
        
        assert result['successful'] == 0
        assert result['failed'] == 5
        assert len(result['failed_details']) == 5
    
    def test_insert_batch_missing_columns(self, market_data_client):
        """Test handling of DataFrame with missing required columns."""
        # DataFrame missing 'open', 'high', 'low', 'close'
        incomplete_df = pd.DataFrame({
            'ticker': ['AAPL'],
            'timestamp': [datetime(2024, 1, 2)],
            'volume': [75000000]
        })
        
        # prepare_dataframe_for_insert should handle missing columns
        with patch('src.database.market_data.prepare_dataframe_for_insert') as mock_prepare:
            mock_prepare.return_value = {
                'tuples': [('AAPL', datetime(2024, 1, 2), '1d', 'test', None, None, None, None, 75000000, None, datetime.now())],
                'tracking': [{'index': 0, 'original_index': 0, 'identifier': 'AAPL_2024-01-02'}],
                'column_names': ['ticker', 'timestamp', 'timeframe', 'data_source', 'open', 'high', 'low', 'close', 'volume', 'transactions', 'ingested_at']
            }
            
            market_data_client._insert_batch_with_retry = Mock(
                return_value={'successful': 1, 'failed': []}
            )
            
            result = market_data_client.insert_batch(incomplete_df, timeframe='1d', data_source='polygon_s3', on_conflict='update', batch_size=None, throttle_rows_per_second=None)
            
            assert result['successful'] == 1
            assert result['failed'] == 0
    
    def test_insert_batch_adds_metadata(self, market_data_client, sample_dataframe):
        """Test that timeframe and data_source are added if missing."""
        # Remove metadata columns
        df = sample_dataframe.copy()
        
        with patch('src.database.utils.prepare_dataframe_for_insert') as mock_prepare:
            # Mock return value
            mock_prepare.return_value = {
                'tuples': [(row,) for _, row in df.iterrows()],
                'tracking': [{'index': i, 'original_index': i, 'identifier': f'test_{i}'} for i in range(len(df))],
                'column_names': market_data_client.default_columns
            }
            
            market_data_client._insert_batch_with_retry = Mock(
                return_value={'successful': 5, 'failed': []}
            )
            
            result = market_data_client.insert_batch(
                df,
                timeframe='1h',
                data_source='custom_source',
                on_conflict='update',
                batch_size=None,
                throttle_rows_per_second=None
            )
            
            # Verify the dataframe was modified to add metadata
            # This happens in insert_batch before calling prepare_dataframe_for_insert
            assert result['successful'] == 5
    
    def test_insert_batch_tracking_info(self, market_data_client, sample_dataframe):
        """Test that tracking info is correct for failed rows."""
        failed_rows = [
            {
                'index': 2,
                'original_index': 2,
                'identifier': 'GOOGL_2024-01-02',
                'ticker': 'GOOGL',
                'date': '2024-01-02',
                'error': 'Test error',
                'error_type': 'TestError'
            }
        ]
        
        market_data_client._insert_batch_with_retry = Mock(
            return_value={'successful': 4, 'failed': failed_rows}
        )
        
        result = market_data_client.insert_batch(sample_dataframe, timeframe='1d', data_source='polygon_s3', on_conflict='update', batch_size=None, throttle_rows_per_second=None)
        
        failed_detail = result['failed_details'][0]
        assert failed_detail['original_index'] == 2
        assert failed_detail['identifier'] == 'GOOGL_2024-01-02'
        assert failed_detail['ticker'] == 'GOOGL'


@pytest.mark.unit
class TestInsertBatchWithRetry:
    """Test the internal batch insert with retry logic."""
    
    @pytest.fixture
    def market_data_client(self, mock_db_config, mock_pool):
        """Create client with mocked dependencies."""
        return MarketDataClient(mock_db_config, mock_pool)
    
    def test_insert_batch_with_retry_multi_success(self, market_data_client):
        """Test successful multi-insert."""
        batch_tuples = [
            ('AAPL', datetime(2024, 1, 2), '1d', 'test', 180.0, 182.0, 179.5, 181.5, 75000000, 500000, datetime.now()),
            ('GOOGL', datetime(2024, 1, 2), '1d', 'test', 140.0, 142.5, 139.5, 141.5, 25000000, 300000, datetime.now())
        ]
        batch_tracking = [
            {'index': 0, 'original_index': 0, 'identifier': 'AAPL_2024-01-02', 'ticker': 'AAPL', 'date': '2024-01-02'},
            {'index': 1, 'original_index': 1, 'identifier': 'GOOGL_2024-01-02', 'ticker': 'GOOGL', 'date': '2024-01-02'}
        ]
        columns = market_data_client.default_columns
        
        # Mock successful execution
        market_data_client._execute_with_retry = Mock()
        
        with patch('src.database.utils.build_multi_insert_query', return_value='INSERT QUERY'):
            result = market_data_client._insert_batch_with_retry(
                batch_tuples,
                batch_tracking,
                columns,
                None
            )
        
        assert result['successful'] == 2
        assert result['failed'] == []
        assert market_data_client._execute_with_retry.call_count == 1
    
    def test_insert_batch_with_retry_fallback(self, market_data_client):
        """Test fallback to row-by-row when batch fails."""
        batch_tuples = [
            ('AAPL', datetime(2024, 1, 2), '1d', 'test', 180.0, 182.0, 179.5, 181.5, 75000000, 500000, datetime.now()),
            ('GOOGL', datetime(2024, 1, 2), '1d', 'test', 140.0, 142.5, 139.5, 141.5, 25000000, 300000, datetime.now())
        ]
        batch_tracking = [
            {'index': 0, 'original_index': 0, 'identifier': 'AAPL_2024-01-02', 'ticker': 'AAPL', 'date': '2024-01-02'},
            {'index': 1, 'original_index': 1, 'identifier': 'GOOGL_2024-01-02', 'ticker': 'GOOGL', 'date': '2024-01-02'}
        ]
        columns = market_data_client.default_columns
        
        # Mock batch insert failure, then individual success
        def execute_side_effect(query, params, **kwargs):
            if 'VALUES (%s' in query and query.count('%s') > 11:  # Multi-insert query
                raise psycopg2.Error("Batch insert failed")
            # Individual inserts succeed
            return None
        
        market_data_client._execute_with_retry = Mock(side_effect=execute_side_effect)
        
        with patch('src.database.utils.build_multi_insert_query', return_value='INSERT VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s), (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'):
            with patch('src.database.utils.build_insert_query', return_value='INSERT VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'):
                result = market_data_client._insert_batch_with_retry(
                    batch_tuples,
                    batch_tracking,
                    columns,
                    None
                )
        
        assert result['successful'] == 2
        assert result['failed'] == []
        # Should have tried batch once + 2 individual inserts
        assert market_data_client._execute_with_retry.call_count == 3
    
    def test_insert_batch_with_retry_single_row(self, market_data_client):
        """Test handling of single row (no multi-insert)."""
        batch_tuples = [
            ('AAPL', datetime(2024, 1, 2), '1d', 'test', 180.0, 182.0, 179.5, 181.5, 75000000, 500000, datetime.now())
        ]
        batch_tracking = [
            {'index': 0, 'original_index': 0, 'identifier': 'AAPL_2024-01-02', 'ticker': 'AAPL', 'date': '2024-01-02'}
        ]
        columns = market_data_client.default_columns
        
        market_data_client._execute_with_retry = Mock()
        
        with patch('src.database.utils.build_insert_query', return_value='INSERT QUERY'):
            result = market_data_client._insert_batch_with_retry(
                batch_tuples,
                batch_tracking,
                columns,
                None
            )
        
        assert result['successful'] == 1
        assert result['failed'] == []
        # Should only call single insert (no multi-insert for 1 row)
        assert market_data_client._execute_with_retry.call_count == 1


@pytest.mark.unit
class TestGetLastTimestamp:
    """Test fetching last timestamp functionality with mocks."""
    
    @pytest.fixture
    def market_data_client(self, mock_db_config, mock_pool):
        """Create client with mocked dependencies."""
        return MarketDataClient(mock_db_config, mock_pool)
    
    def test_get_last_timestamp_exists(self, market_data_client):
        """Test returning correct timestamp when data exists."""
        expected_timestamp = datetime(2024, 1, 5, 0, 0, 0)
        
        market_data_client._execute_with_retry = Mock(
            return_value=[(expected_timestamp,)]
        )
        
        result = market_data_client.get_last_timestamp(
            'AAPL',
            timeframe='1d',
            data_source='polygon_s3'
        )
        
        assert result == expected_timestamp
        
        # Verify query parameters
        call_args = market_data_client._execute_with_retry.call_args
        assert call_args.kwargs['params'] == ('AAPL', '1d', 'polygon_s3')
    
    def test_get_last_timestamp_no_data(self, market_data_client):
        """Test returning None when no data exists."""
        market_data_client._execute_with_retry = Mock(
            return_value=[(None,)]
        )
        
        result = market_data_client.get_last_timestamp('NEWSTOCK', timeframe='1d', data_source='polygon_s3')
        
        assert result is None
    
    def test_get_last_timestamp_multiple_sources(self, market_data_client):
        """Test filtering correctly by data_source."""
        market_data_client._execute_with_retry = Mock(
            return_value=[(datetime(2024, 1, 5),)]
        )
        
        result = market_data_client.get_last_timestamp(
            'AAPL',
            timeframe='1d',
            data_source='yahoo'
        )
        
        # Verify correct source was queried
        call_args = market_data_client._execute_with_retry.call_args
        assert call_args.kwargs['params'][2] == 'yahoo'
    
    def test_get_last_timestamp_multiple_timeframes(self, market_data_client):
        """Test filtering correctly by timeframe."""
        market_data_client._execute_with_retry = Mock(
            return_value=[(datetime(2024, 1, 5, 15, 30),)]
        )
        
        result = market_data_client.get_last_timestamp(
            'AAPL',
            timeframe='5m',
            data_source='polygon_s3'
        )
        
        # Verify correct timeframe was queried
        call_args = market_data_client._execute_with_retry.call_args
        assert call_args.kwargs['params'][1] == '5m'


@pytest.mark.unit
class TestGetDataGaps:
    """Test gap detection functionality with mocks."""
    
    @pytest.fixture
    def market_data_client(self, mock_db_config, mock_pool):
        """Create client with mocked dependencies."""
        return MarketDataClient(mock_db_config, mock_pool)
    
    def test_get_data_gaps_no_gaps(self, market_data_client):
        """Test returning empty list when no gaps exist."""
        # Mock no gaps (all dates have data)
        market_data_client._execute_with_retry = Mock(
            return_value=[]
        )
        
        result = market_data_client.get_data_gaps(
            'AAPL',
            date(2024, 1, 2),
            date(2024, 1, 5),
            timeframe='1d',
            data_source='polygon_s3'
        )
        
        assert result == []
    
    def test_get_data_gaps_with_gaps(self, market_data_client):
        """Test identifying gaps correctly."""
        # Mock gaps on Jan 3 and Jan 4
        market_data_client._execute_with_retry = Mock(
            return_value=[
                (date(2024, 1, 3),),
                (date(2024, 1, 4),)
            ]
        )
        
        result = market_data_client.get_data_gaps(
            'AAPL',
            date(2024, 1, 2),
            date(2024, 1, 5),
            timeframe='1d',
            data_source='polygon_s3'
        )
        
        assert len(result) == 2
        assert date(2024, 1, 3) in result
        assert date(2024, 1, 4) in result
    
    def test_get_data_gaps_excludes_weekends(self, market_data_client):
        """Test that weekends are not reported as gaps."""
        # Query should exclude weekends (DOW 0 and 6)
        market_data_client._execute_with_retry = Mock(
            return_value=[]
        )
        
        result = market_data_client.get_data_gaps(
            'AAPL',
            date(2024, 1, 1),  # Monday
            date(2024, 1, 7),   # Sunday
            timeframe='1d',
            data_source='polygon_s3'
        )
        
        # Verify query excluded weekends
        query_call = market_data_client._execute_with_retry.call_args.args[0]
        assert 'EXTRACT(DOW FROM d.expected_date) NOT IN (0, 6)' in query_call
    
    def test_get_data_gaps_empty_table(self, market_data_client):
        """Test returning all weekdays when no data exists."""
        # All weekdays between Jan 2-5 (Tue-Fri)
        market_data_client._execute_with_retry = Mock(
            return_value=[
                (date(2024, 1, 2),),
                (date(2024, 1, 3),),
                (date(2024, 1, 4),),
                (date(2024, 1, 5),)
            ]
        )
        
        result = market_data_client.get_data_gaps(
            'AAPL',
            date(2024, 1, 2),
            date(2024, 1, 5),
            timeframe='1d',
            data_source='polygon_s3'
        )
        
        assert len(result) == 4
    
    def test_get_data_gaps_different_timeframes(self, market_data_client):
        """Test that only specified timeframe is considered."""
        market_data_client._execute_with_retry = Mock(
            return_value=[(date(2024, 1, 3),)]
        )
        
        result = market_data_client.get_data_gaps(
            'AAPL',
            date(2024, 1, 2),
            date(2024, 1, 5),
            timeframe='5m',
            data_source='polygon_s3'
        )
        
        # Verify timeframe filter was applied
        call_args = market_data_client._execute_with_retry.call_args
        assert call_args.kwargs['params'][3] == '5m'


@pytest.mark.unit
class TestGetTickerStats:
    """Test ticker statistics functionality with mocks."""
    
    @pytest.fixture
    def market_data_client(self, mock_db_config, mock_pool):
        """Create client with mocked dependencies."""
        return MarketDataClient(mock_db_config, mock_pool)
    
    def test_get_ticker_stats_with_data(self, market_data_client):
        """Test returning correct statistics."""
        # Mock stats query result
        market_data_client._execute_with_retry = Mock(
            return_value=[(
                1000,                    # total_records
                2,                       # timeframes
                1,                       # sources
                datetime(2023, 1, 1),    # first_date
                datetime(2024, 1, 5),    # last_date
                50000000.0,              # avg_volume
                1.5,                     # avg_daily_change
                5.2                      # price_volatility
            )]
        )
        
        result = market_data_client.get_ticker_stats('AAPL')
        
        assert result['ticker'] == 'AAPL'
        assert result['total_records'] == 1000
        assert result['timeframes'] == 2
        assert result['sources'] == 1
        assert result['avg_volume'] == 50000000.0
        assert result['avg_daily_change'] == 1.5
        assert result['price_volatility'] == 5.2
        assert result['days_of_data'] == 369  # Approximate
    
    def test_get_ticker_stats_no_data(self, market_data_client):
        """Test handling ticker with no data."""
        market_data_client._execute_with_retry = Mock(
            return_value=[(None, None, None, None, None, None, None, None)]
        )
        
        result = market_data_client.get_ticker_stats('UNKNOWN')
        
        assert result['ticker'] == 'UNKNOWN'
        assert result['total_records'] == 0
        assert 'error' in result
    
    def test_get_ticker_stats_calculations(self, market_data_client):
        """Test that calculations are correct."""
        # Test with specific values
        first_date = datetime(2024, 1, 1)
        last_date = datetime(2024, 1, 31)
        
        market_data_client._execute_with_retry = Mock(
            return_value=[(
                100,                     # total_records
                1,                       # timeframes
                1,                       # sources
                first_date,              # first_date
                last_date,               # last_date
                1000000.0,               # avg_volume
                0.5,                     # avg_daily_change
                2.3                      # price_volatility
            )]
        )
        
        result = market_data_client.get_ticker_stats('AAPL')
        
        # Check days calculation
        expected_days = (last_date - first_date).days
        assert result['days_of_data'] == expected_days
    
    def test_get_ticker_stats_null_values(self, market_data_client):
        """Test handling NULL values in calculations."""
        market_data_client._execute_with_retry = Mock(
            return_value=[(
                10,                      # total_records
                1,                       # timeframes
                1,                       # sources
                datetime(2024, 1, 1),    # first_date
                datetime(2024, 1, 5),    # last_date
                None,                    # avg_volume (NULL)
                None,                    # avg_daily_change (NULL)
                None                     # price_volatility (NULL)
            )]
        )
        
        result = market_data_client.get_ticker_stats('AAPL')
        
        assert result['avg_volume'] == 0
        assert result['avg_daily_change'] == 0
        assert result['price_volatility'] == 0


@pytest.mark.unit
class TestDeleteDateRange:
    """Test date range deletion functionality with mocks."""
    
    @pytest.fixture
    def market_data_client(self, mock_db_config, mock_pool):
        """Create client with mocked dependencies."""
        return MarketDataClient(mock_db_config, mock_pool)
    
    def test_delete_date_range_dry_run(self, market_data_client):
        """Test dry run doesn't delete but reports correctly."""
        # Mock count query
        market_data_client._execute_with_retry = Mock(
            return_value=[(50,)]
        )
        
        result = market_data_client.delete_date_range(
            'AAPL',
            date(2024, 1, 2),
            date(2024, 1, 5),
            timeframe='1d',
            data_source='polygon_s3',
            dry_run=True
        )
        
        assert result == 50
        # Should only call count query, not delete
        assert market_data_client._execute_with_retry.call_count == 1
    
    def test_delete_date_range_actual_delete(self, market_data_client):
        """Test actual deletion of records."""
        # Mock count then delete
        market_data_client._execute_with_retry = Mock(
            side_effect=[
                [(30,)],  # Count query result
                None      # Delete query result
            ]
        )
        
        result = market_data_client.delete_date_range(
            'AAPL',
            date(2024, 1, 2),
            date(2024, 1, 5),
            timeframe='1d',
            data_source='polygon_s3',
            dry_run=False
        )
        
        assert result == 30
        # Should call count then delete
        assert market_data_client._execute_with_retry.call_count == 2
    
    def test_delete_date_range_no_matching_data(self, market_data_client):
        """Test returning 0 when no data to delete."""
        market_data_client._execute_with_retry = Mock(
            return_value=[(0,)]
        )
        
        result = market_data_client.delete_date_range(
            'AAPL',
            date(2024, 12, 1),
            date(2024, 12, 31),
            timeframe='1d',
            data_source='polygon_s3',
            dry_run=True
        )
        
        assert result == 0


@pytest.mark.unit
class TestGetDataSummary:
    """Test data summary functionality with mocks."""
    
    @pytest.fixture
    def market_data_client(self, mock_db_config, mock_pool):
        """Create client with mocked dependencies."""
        return MarketDataClient(mock_db_config, mock_pool)
    
    @patch('pandas.read_sql_query')
    def test_get_data_summary_no_filters(self, mock_read_sql, market_data_client):
        """Test summary without date filters."""
        # Mock DataFrame result
        expected_df = pd.DataFrame({
            'ticker': ['AAPL', 'GOOGL'],
            'timeframe': ['1d', '1d'],
            'data_source': ['polygon_s3', 'polygon_s3'],
            'record_count': [250, 250],
            'first_date': [date(2023, 1, 1), date(2023, 1, 1)],
            'last_date': [date(2024, 1, 5), date(2024, 1, 5)],
            'trading_days': [250, 250]
        })
        
        mock_read_sql.return_value = expected_df
        
        # Mock connection context manager
        mock_conn = Mock()
        mock_context_manager = Mock()
        mock_context_manager.__enter__ = Mock(return_value=mock_conn)
        mock_context_manager.__exit__ = Mock(return_value=False)
        market_data_client._get_connection = Mock(return_value=mock_context_manager)
        
        result = market_data_client.get_data_summary(start_date=None, end_date=None)
        
        assert len(result) == 2
        assert result.iloc[0]['ticker'] == 'AAPL'
        assert mock_read_sql.call_args[1]['params'] == []  # No params
    
    @patch('pandas.read_sql_query')
    def test_get_data_summary_with_date_filters(self, mock_read_sql, market_data_client):
        """Test summary with date filters."""
        expected_df = pd.DataFrame({
            'ticker': ['AAPL'],
            'timeframe': ['1d'],
            'data_source': ['polygon_s3'],
            'record_count': [5],
            'first_date': [date(2024, 1, 2)],
            'last_date': [date(2024, 1, 5)],
            'trading_days': [4]
        })
        
        mock_read_sql.return_value = expected_df
        
        # Mock connection context manager
        mock_conn = Mock()
        mock_context_manager = Mock()
        mock_context_manager.__enter__ = Mock(return_value=mock_conn)
        mock_context_manager.__exit__ = Mock(return_value=False)
        market_data_client._get_connection = Mock(return_value=mock_context_manager)
        
        result = market_data_client.get_data_summary(
            start_date=date(2024, 1, 2),
            end_date=date(2024, 1, 5)
        )
        
        # Check params included dates
        call_params = mock_read_sql.call_args[1]['params']
        assert date(2024, 1, 2) in call_params
        assert date(2024, 1, 5) in call_params


@pytest.mark.unit
class TestErrorHandling:
    """Test error handling scenarios with mocks."""
    
    @pytest.fixture
    def market_data_client(self, mock_db_config, mock_pool):
        """Create client with mocked dependencies."""
        return MarketDataClient(mock_db_config, mock_pool)
    
    def test_database_connection_error(self, market_data_client):
        """Test handling when database is unavailable."""
        # Mock connection error
        market_data_client._execute_with_retry = Mock(
            side_effect=psycopg2.OperationalError("Connection failed")
        )
        
        with pytest.raises(psycopg2.OperationalError):
            market_data_client.get_last_timestamp('AAPL', timeframe='1d', data_source='polygon_s3')
    
    def test_invalid_data_types(self, market_data_client):
        """Test handling of incorrect data types."""
        # DataFrame with invalid types
        invalid_df = pd.DataFrame({
            'ticker': ['AAPL'],
            'timestamp': ['not-a-date'],  # Invalid timestamp
            'open': ['not-a-number'],      # Invalid numeric
            'high': [182.0],
            'low': [179.5],
            'close': [181.5],
            'volume': [75000000],
            'transactions': [500000]
        })
        
        # Test that data validation errors are properly handled
        # Mock prepare_dataframe_for_insert to raise ValueError
        with patch('src.database.market_data.prepare_dataframe_for_insert') as mock_prepare:
            mock_prepare.side_effect = ValueError("Invalid data types")
            
            with pytest.raises(ValueError, match="Invalid data types"):
                market_data_client.insert_batch(invalid_df, timeframe='1d', data_source='polygon_s3', on_conflict='update', batch_size=None, throttle_rows_per_second=None)
    
    def test_constraint_violations(self, market_data_client, sample_dataframe):
        """Test handling of database constraint violations."""
        # Mock integrity error
        error_msg = 'duplicate key value violates unique constraint "market_data_raw_pkey"'
        
        market_data_client._insert_batch_with_retry = Mock(
            return_value={
                'successful': 3,
                'failed': [
                    {
                        'index': 0,
                        'original_index': 0,
                        'identifier': 'AAPL_2024-01-02',
                        'ticker': 'AAPL',
                        'date': '2024-01-02',
                        'error': error_msg,
                        'error_type': 'IntegrityError'
                    },
                    {
                        'index': 2,
                        'original_index': 2,
                        'identifier': 'GOOGL_2024-01-02',
                        'ticker': 'GOOGL',
                        'date': '2024-01-02',
                        'error': error_msg,
                        'error_type': 'IntegrityError'
                    }
                ]
            }
        )
        
        result = market_data_client.insert_batch(
            sample_dataframe,
            timeframe='1d',
            data_source='polygon_s3',
            on_conflict='error',  # No conflict handling
            batch_size=None,
            throttle_rows_per_second=None
        )
        
        assert result['successful'] == 3
        assert result['failed'] == 2
        assert 'unique constraint' in result['failed_details'][0]['error']


@pytest.mark.unit 
class TestPerformance:
    """Test performance-related functionality with mocks."""
    
    @pytest.fixture
    def market_data_client(self, mock_db_config, mock_pool):
        """Create client with mocked dependencies."""
        return MarketDataClient(mock_db_config, mock_pool)
    
    @patch('time.sleep')
    def test_throttling_rate(self, mock_sleep, market_data_client, sample_dataframe):
        """Test that rate limiting works correctly."""
        # Create larger dataset
        large_df = pd.concat([sample_dataframe] * 100, ignore_index=True)
        
        # Mock prepare_dataframe_for_insert to return proper number of rows
        with patch('src.database.utils.prepare_dataframe_for_insert') as mock_prepare:
            # Create tuples for 500 rows (5 * 100)
            mock_prepare.return_value = {
                'tuples': [('test_tuple',) * 11] * 500,  # 500 rows
                'tracking': [{'index': i, 'original_index': i, 'identifier': f'test_{i}'} for i in range(500)],
                'column_names': market_data_client.default_columns
            }
            
            market_data_client._insert_batch_with_retry = Mock(
                return_value={'successful': 10, 'failed': []}
            )
            
            with patch('src.database.market_data.calculate_insert_throttle', return_value=0.1) as mock_throttle:
                result = market_data_client.insert_batch(
                    large_df,
                    timeframe='1d',
                    data_source='polygon_s3',
                    on_conflict='update',
                    batch_size=10,
                    throttle_rows_per_second=100
                )
            
            # Verify throttling was calculated and sleep was called
            # Should throttle for batches 1-49 (49 times) since first batch (batch_start=0) is not throttled
            assert mock_throttle.call_count == 49
            assert mock_sleep.call_count == 49
            
            # Verify throttle calculation parameters
            throttle_calls = mock_throttle.call_args_list
            for call in throttle_calls:
                assert call[0][0] == 100  # rows_per_second
                assert call[0][1] == 10   # batch_size


@pytest.mark.unit
class TestMetrics:
    """Test metrics and logging functionality with mocks."""
    
    @pytest.fixture
    def market_data_client(self, mock_db_config, mock_pool):
        """Create client with mocked dependencies."""
        return MarketDataClient(mock_db_config, mock_pool)
    
    def test_metrics_logging(self, market_data_client, sample_dataframe, caplog):
        """Test that metrics are logged correctly."""
        market_data_client._insert_batch_with_retry = Mock(
            return_value={'successful': 5, 'failed': []}
        )
        
        with caplog.at_level(logging.INFO):
            result = market_data_client.insert_batch(sample_dataframe, timeframe='1d', data_source='polygon_s3', on_conflict='update', batch_size=None, throttle_rows_per_second=None)
        
        # Check for expected log messages
        log_messages = [record.message for record in caplog.records]
        
        # Should log start, progress, and completion
        assert any('Starting insert of 5 rows' in msg for msg in log_messages)
        assert any('Progress:' in msg for msg in log_messages)
        assert any('Insert completed: 5/5 successful' in msg for msg in log_messages)
    
    def test_performance_tracking(self, market_data_client, sample_dataframe):
        """Test tracking of operation count and total time."""
        # Access to _operation_count and _total_time from base class
        initial_count = market_data_client._operation_count
        initial_time = market_data_client._total_time
        
        market_data_client._insert_batch_with_retry = Mock(
            return_value={'successful': 5, 'failed': []}
        )
        
        # Perform operation
        result = market_data_client.insert_batch(sample_dataframe, timeframe='1d', data_source='polygon_s3', on_conflict='update', batch_size=None, throttle_rows_per_second=None)
        
        # Performance metrics should be in result
        assert 'duration_seconds' in result
        assert 'rows_per_second' in result
        assert result['duration_seconds'] > 0
        assert result['rows_per_second'] > 0