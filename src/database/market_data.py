"""Market data client for market_data_raw table operations."""
import logging
from datetime import datetime, date
from typing import Optional, List, Dict, Tuple, Any
import time

import pandas as pd
import psycopg2
from psycopg2 import sql

from .base import BaseDBClient
from .utils import (
    build_insert_query, 
    build_multi_insert_query,
    prepare_dataframe_for_insert,
    estimate_batch_size,
    calculate_insert_throttle
)

logger = logging.getLogger(__name__)


class MarketDataClient(BaseDBClient):
    """
    Client for market_data_raw table operations.
    
    Handles all operations related to market data storage including
    inserts, queries, and data management.
    """
    
    def __init__(self, *args, **kwargs):
        """Initialize market data client."""
        super().__init__(*args, **kwargs)
        self.table_name = "trading.market_data_raw"
        self.default_columns = [
            'ticker', 'timestamp', 'timeframe', 'data_source',
            'open', 'high', 'low', 'close', 'volume', 
            'transactions', 'ingested_at'
        ]
    
    def insert_batch(
        self, 
        df: pd.DataFrame,
        timeframe: str,
        data_source: str,
        on_conflict: str,
        batch_size: Optional[int] = None,
        throttle_rows_per_second: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Insert batch of market data with detailed error tracking.
        
        Args:
            df: DataFrame with market data
            timeframe: Time interval ('1d', '1m', etc.)
            data_source: Source of data
            on_conflict: How to handle conflicts ('update', 'nothing', 'error')
            batch_size: Number of rows per batch (None for auto)
            throttle_rows_per_second: Rate limit for inserts
            
        Returns:
            Dictionary with results:
            {
                'total_rows': int,
                'successful': int,
                'failed': int,
                'failed_details': List[Dict],
                'duration_seconds': float,
                'rows_per_second': float
            }
        """
        start_time = time.time()
        
        # Handle empty dataframe
        if df.empty:
            return {
                'total_rows': 0,
                'successful': 0,
                'failed': 0,
                'failed_details': [],
                'duration_seconds': 0,
                'rows_per_second': 0
            }
        
        # Add metadata columns if not present
        df_copy = df.copy()
        if 'timeframe' not in df_copy.columns:
            df_copy['timeframe'] = timeframe
        if 'data_source' not in df_copy.columns:
            df_copy['data_source'] = data_source
        
        # Prepare data with tracking
        columns = [col for col in self.default_columns if col != 'ingested_at']
        prepared = prepare_dataframe_for_insert(
            df_copy, 
            columns, 
            add_ingested_at=True,
            include_tracking=True
        )
        
        # Determine batch size
        if batch_size is None:
            batch_size = estimate_batch_size(timeframe, 'insert')
        
        # Configure ON CONFLICT behavior
        conflict_config = None
        if on_conflict == 'update':
            conflict_config = {
                'columns': ['ticker', 'timestamp', 'timeframe', 'data_source'],
                'action': 'update',
                'update_columns': ['open', 'high', 'low', 'close', 'volume', 'transactions']
            }
        elif on_conflict == 'nothing':
            conflict_config = {
                'columns': ['ticker', 'timestamp', 'timeframe', 'data_source'],
                'action': 'nothing'
            }
        
        # Process in batches
        total_rows = len(prepared['tuples'])
        successful_count = 0
        failed_rows = []
        
        self.logger.info(
            f"Starting insert of {total_rows} rows in batches of {batch_size}"
        )
        
        for batch_start in range(0, total_rows, batch_size):
            batch_end = min(batch_start + batch_size, total_rows)
            batch_tuples = prepared['tuples'][batch_start:batch_end]
            batch_tracking = prepared['tracking'][batch_start:batch_end]
            
            # Throttle if requested
            if throttle_rows_per_second and batch_start > 0:
                sleep_time = calculate_insert_throttle(
                    throttle_rows_per_second,
                    len(batch_tuples)
                )
                time.sleep(sleep_time)
            
            # Try batch insert first
            batch_result = self._insert_batch_with_retry(
                batch_tuples,
                batch_tracking,
                prepared['column_names'],
                conflict_config
            )
            
            successful_count += batch_result['successful']
            failed_rows.extend(batch_result['failed'])
            
            # Log progress
            progress = (batch_end / total_rows) * 100
            self.logger.info(
                f"Progress: {progress:.1f}% ({batch_end}/{total_rows} rows)"
            )
        
        # Calculate final metrics
        duration = time.time() - start_time
        rows_per_second = successful_count / duration if duration > 0 else 0
        
        # Log final results
        self.logger.info(
            f"Insert completed: {successful_count}/{total_rows} successful "
            f"in {duration:.2f}s ({rows_per_second:.0f} rows/sec)"
        )
        
        if failed_rows:
            self.logger.warning(
                f"{len(failed_rows)} rows failed to insert. "
                f"First failure: {failed_rows[0]['error']}"
            )
        
        return {
            'total_rows': total_rows,
            'successful': successful_count,
            'failed': len(failed_rows),
            'failed_details': failed_rows,
            'duration_seconds': duration,
            'rows_per_second': rows_per_second
        }
    
    def _insert_batch_with_retry(
        self,
        batch_tuples: List[Tuple],
        batch_tracking: List[Dict],
        columns: List[str],
        conflict_config: Optional[Dict]
    ) -> Dict[str, Any]:
        """
        Try to insert a batch, falling back to row-by-row on failure.
        
        Returns:
            Dictionary with successful count and failed rows
        """
        batch_size = len(batch_tuples)
        
        # First try: Insert entire batch at once
        if batch_size > 1:
            try:
                query = build_multi_insert_query(
                    self.table_name,
                    columns,
                    batch_size,
                    on_conflict=conflict_config
                )
                
                # Flatten tuples for multi-insert
                flat_params = []
                for row_tuple in batch_tuples:
                    flat_params.extend(row_tuple)
                
                self._execute_with_retry(
                    query,
                    tuple(flat_params),
                    fetch=False,
                    commit=True
                )
                
                # All successful
                return {
                    'successful': batch_size,
                    'failed': []
                }
                
            except Exception as e:
                self.logger.warning(
                    f"Batch insert failed, falling back to row-by-row: {e}"
                )
        
        # Fallback: Insert row by row to identify failures
        query = build_insert_query(
            self.table_name,
            columns,
            on_conflict=conflict_config
        )
        
        successful = 0
        failed = []
        
        for i, (row_tuple, tracking) in enumerate(zip(batch_tuples, batch_tracking)):
            try:
                self._execute_with_retry(
                    query,
                    row_tuple,
                    fetch=False,
                    commit=True,
                    max_retries=1  # Less retries for individual rows
                )
                successful += 1
                
            except Exception as e:
                failed.append({
                    'index': tracking['index'],
                    'original_index': tracking['original_index'],
                    'identifier': tracking['identifier'],
                    'ticker': tracking.get('ticker'),
                    'date': tracking.get('date'),
                    'error': str(e),
                    'error_type': type(e).__name__
                })
        
        return {
            'successful': successful,
            'failed': failed
        }
    
    def get_last_timestamp(
        self, 
        ticker: str, 
        timeframe: str,
        data_source: str
    ) -> Optional[datetime]:
        """
        Get the most recent timestamp for a ticker.
        
        Args:
            ticker: Stock symbol
            timeframe: Time interval
            data_source: Source of data
            
        Returns:
            Last timestamp or None if no data exists
        """
        query = """
        SELECT MAX(timestamp) 
        FROM trading.market_data_raw 
        WHERE ticker = %s 
          AND timeframe = %s 
          AND data_source = %s
        """
        
        try:
            result = self._execute_with_retry(
                query,
                (ticker, timeframe, data_source)
            )
            
            if result and result[0][0]:
                return result[0][0]
            
            self.logger.info(
                f"No data found for {ticker} ({timeframe}, {data_source})"
            )
            return None
            
        except Exception as e:
            self.logger.error(
                f"Error getting last timestamp for {ticker}: {e}"
            )
            raise
    
    def get_data_gaps(
        self, 
        ticker: str, 
        start_date: date, 
        end_date: date,
        timeframe: str,
        data_source: str
    ) -> List[date]:
        """
        Find missing dates in the data range.
        
        Uses a calendar table approach to identify gaps.
        
        Args:
            ticker: Stock symbol
            start_date: Start of range to check
            end_date: End of range to check
            timeframe: Time interval
            data_source: Source of data
            
        Returns:
            List of dates with missing data
        """
        # For daily data, we can use generate_series
        query = """
        WITH date_series AS (
            SELECT generate_series(
                %s::date,
                %s::date,
                '1 day'::interval
            )::date AS expected_date
        ),
        existing_data AS (
            SELECT DISTINCT DATE(timestamp) as data_date
            FROM trading.market_data_raw
            WHERE ticker = %s
              AND timeframe = %s
              AND data_source = %s
              AND timestamp >= %s
              AND timestamp < %s + INTERVAL '1 day'
        )
        SELECT d.expected_date
        FROM date_series d
        LEFT JOIN existing_data e ON d.expected_date = e.data_date
        WHERE e.data_date IS NULL
          AND EXTRACT(DOW FROM d.expected_date) NOT IN (0, 6)  -- Exclude weekends
        ORDER BY d.expected_date
        """
        
        try:
            result = self._execute_with_retry(
                query,
                (start_date, end_date, ticker, timeframe, 
                 data_source, start_date, end_date)
            )
            
            gaps = [row[0] for row in result]
            
            if gaps:
                self.logger.warning(
                    f"Found {len(gaps)} gaps for {ticker} between "
                    f"{start_date} and {end_date}"
                )
            
            return gaps
            
        except Exception as e:
            self.logger.error(f"Error finding data gaps for {ticker}: {e}")
            raise
    
    def get_ticker_stats(self, ticker: str) -> Dict[str, Any]:
        """
        Get comprehensive statistics for a ticker.
        
        Args:
            ticker: Stock symbol
            
        Returns:
            Dictionary with stats including count, date range, quality metrics
        """
        query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT timeframe) as timeframes,
            COUNT(DISTINCT data_source) as sources,
            MIN(timestamp) as first_date,
            MAX(timestamp) as last_date,
            AVG(volume) as avg_volume,
            AVG(close - open) as avg_daily_change,
            STDDEV(close) as price_volatility
        FROM trading.market_data_raw
        WHERE ticker = %s
        """
        
        try:
            result = self._execute_with_retry(query, (ticker,))
            
            if result and result[0][0]:
                row = result[0]
                stats = {
                    'ticker': ticker,
                    'total_records': row[0],
                    'timeframes': row[1],
                    'sources': row[2],
                    'first_date': row[3],
                    'last_date': row[4],
                    'avg_volume': float(row[5]) if row[5] else 0,
                    'avg_daily_change': float(row[6]) if row[6] else 0,
                    'price_volatility': float(row[7]) if row[7] else 0,
                    'days_of_data': (row[4] - row[3]).days if row[3] and row[4] else 0
                }
                
                self.logger.info(
                    f"Stats for {ticker}: {stats['total_records']} records "
                    f"from {stats['first_date']} to {stats['last_date']}"
                )
                
                return stats
            else:
                return {
                    'ticker': ticker,
                    'total_records': 0,
                    'error': 'No data found'
                }
                
        except Exception as e:
            self.logger.error(f"Error getting stats for {ticker}: {e}")
            raise
    
    def delete_date_range(
        self, 
        ticker: str, 
        start_date: date, 
        end_date: date,
        timeframe: str,
        data_source: str,
        dry_run: bool
    ) -> int:
        """
        Delete data for a date range (useful for reprocessing).
        
        Args:
            ticker: Stock symbol
            start_date: Start of range to delete
            end_date: End of range to delete (inclusive)
            timeframe: Time interval
            data_source: Source of data
            dry_run: If True, only show what would be deleted
            
        Returns:
            Number of rows deleted (or would be deleted if dry_run)
        """
        # First, count what would be deleted
        count_query = """
        SELECT COUNT(*)
        FROM trading.market_data_raw
        WHERE ticker = %s
          AND timestamp >= %s
          AND timestamp < %s + INTERVAL '1 day'
          AND timeframe = %s
          AND data_source = %s
        """
        
        try:
            result = self._execute_with_retry(
                count_query,
                (ticker, start_date, end_date, timeframe, data_source)
            )
            
            row_count = result[0][0] if result else 0
            
            if dry_run:
                self.logger.info(
                    f"DRY RUN: Would delete {row_count} rows for {ticker} "
                    f"from {start_date} to {end_date}"
                )
                return row_count
            
            if row_count == 0:
                self.logger.info(f"No rows to delete for {ticker}")
                return 0
            
            # Confirm and delete
            delete_query = """
            DELETE FROM trading.market_data_raw
            WHERE ticker = %s
              AND timestamp >= %s
              AND timestamp < %s + INTERVAL '1 day'
              AND timeframe = %s
              AND data_source = %s
            """
            
            self._execute_with_retry(
                delete_query,
                (ticker, start_date, end_date, timeframe, data_source),
                fetch=False,
                commit=True
            )
            
            self.logger.warning(
                f"Deleted {row_count} rows for {ticker} "
                f"from {start_date} to {end_date}"
            )
            
            return row_count
            
        except Exception as e:
            self.logger.error(
                f"Error deleting data for {ticker}: {e}"
            )
            raise
    
    def get_data_summary(
        self, 
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> pd.DataFrame:
        """
        Get summary of all data in the database.
        
        Args:
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            DataFrame with summary by ticker
        """
        query = """
        SELECT 
            ticker,
            timeframe,
            data_source,
            COUNT(*) as record_count,
            MIN(timestamp)::date as first_date,
            MAX(timestamp)::date as last_date,
            COUNT(DISTINCT DATE(timestamp)) as trading_days
        FROM trading.market_data_raw
        WHERE 1=1
        """
        
        params = []
        if start_date:
            query += " AND timestamp >= %s"
            params.append(start_date)
        if end_date:
            query += " AND timestamp <= %s"
            params.append(end_date)
            
        query += """
        GROUP BY ticker, timeframe, data_source
        ORDER BY ticker, timeframe, data_source
        """
        
        try:
            # Use pandas for nice DataFrame output
            with self._get_connection() as conn:
                df = pd.read_sql_query(query, conn, params=params)
                
            self.logger.info(
                f"Data summary: {len(df)} ticker/timeframe combinations"
            )
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error getting data summary: {e}")
            raise
