"""Base database client with shared functionality."""
import logging
import json
import time
from typing import Optional, Dict, Tuple, Any, Union
from contextlib import contextmanager
from datetime import datetime
import random

import psycopg2
import psycopg2.pool
from psycopg2.extensions import connection as Connection
from psycopg2.extensions import cursor as Cursor
from psycopg2 import sql

from config_loader import DatabaseConfig

logger = logging.getLogger(__name__)


class BaseDBClient:
    """
    Base class with shared functionality for all database clients.
    
    Provides:
    - Connection pool management
    - Retry logic with exponential backoff
    - Structured metrics logging
    - Error formatting and handling
    """
    
    def __init__(self, db_config: DatabaseConfig, pool: psycopg2.pool.ThreadedConnectionPool):
        """
        Initialize base client with shared pool.
        
        Args:
            db_config: Database configuration
            pool: Shared connection pool
        """
        self.config = db_config
        self._pool = pool
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Performance tracking
        self._operation_count = 0
        self._total_time = 0.0
        
    @contextmanager
    def _get_connection(self) -> Connection:
        """
        Get connection from pool with automatic cleanup.
        
        Yields:
            Active database connection
            
        Raises:
            psycopg2.pool.PoolError: If no connections available
            
        Example:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
        """
        conn = None
        start_time = time.time()
        
        try:
            # Get connection with timeout
            self.logger.debug("Requesting connection from pool")
            conn = self._pool.getconn()
            
            # Log pool stats in debug mode
            if self.logger.isEnabledFor(logging.DEBUG):
                self._log_pool_stats()
            
            yield conn
            
        except psycopg2.pool.PoolError as e:
            self.logger.error(f"Connection pool exhausted: {e}")
            self._log_metrics({
                'operation': 'get_connection',
                'status': 'pool_exhausted',
                'wait_time_ms': (time.time() - start_time) * 1000
            })
            raise
            
        except Exception as e:
            self.logger.error(f"Connection error: {e}")
            if conn:
                # Mark connection as broken
                self._pool.putconn(conn, close=True)
                conn = None
            raise
            
        finally:
            # Return connection to pool
            if conn:
                try:
                    self._pool.putconn(conn)
                    self.logger.debug("Connection returned to pool")
                except Exception as e:
                    self.logger.error(f"Error returning connection to pool: {e}")
    
    def _execute_with_retry(
        self, 
        query: str, 
        params: Optional[Union[Tuple, Dict]] = None,
        max_retries: int = 3,
        fetch: bool = True,
        commit: bool = False
    ) -> Any:
        """
        Execute query with retry logic and exponential backoff.
        
        Args:
            query: SQL query to execute
            params: Query parameters (tuple or dict)
            max_retries: Maximum number of retry attempts
            fetch: Whether to fetch results (True for SELECT, False for INSERT/UPDATE)
            commit: Whether to commit transaction
            
        Returns:
            Query results if fetch=True, None otherwise
            
        Raises:
            Exception: If all retries exhausted
            
        Example:
            result = self._execute_with_retry(
                "SELECT * FROM trading.market_data_raw WHERE ticker = %s",
                ('AAPL',)
            )
        """
        last_error = None
        
        for attempt in range(max_retries + 1):
            try:
                with self._get_connection() as conn:
                    with conn.cursor() as cur:
                        start_time = time.time()
                        
                        # Execute query
                        self.logger.debug(
                            f"Executing query (attempt {attempt + 1}/{max_retries + 1}): "
                            f"{query[:100]}..."
                        )
                        cur.execute(query, params)
                        
                        # Handle results based on operation type
                        result = None
                        if fetch:
                            result = cur.fetchall()
                        
                        # Commit if requested
                        if commit:
                            conn.commit()
                            
                        # Log success metrics
                        elapsed = time.time() - start_time
                        self._log_metrics({
                            'operation': 'execute_query',
                            'status': 'success',
                            'attempt': attempt + 1,
                            'duration_ms': elapsed * 1000,
                            'rows_affected': cur.rowcount
                        })
                        
                        return result
                        
            except psycopg2.OperationalError as e:
                # Connection issues - retry
                last_error = e
                if attempt < max_retries:
                    wait_time = self._calculate_backoff(attempt)
                    self.logger.warning(
                        f"Operational error on attempt {attempt + 1}, "
                        f"retrying in {wait_time}s: {e}"
                    )
                    time.sleep(wait_time)
                    
            except psycopg2.IntegrityError as e:
                # Data integrity issues - don't retry
                self.logger.error(f"Integrity error (not retrying): {e}")
                self._log_metrics({
                    'operation': 'execute_query',
                    'status': 'integrity_error',
                    'error': str(e)
                })
                raise
                
            except Exception as e:
                # Other errors - retry
                last_error = e
                if attempt < max_retries:
                    wait_time = self._calculate_backoff(attempt)
                    self.logger.warning(
                        f"Error on attempt {attempt + 1}, "
                        f"retrying in {wait_time}s: {e}"
                    )
                    time.sleep(wait_time)
        
        # All retries exhausted
        error_msg = f"Query failed after {max_retries + 1} attempts: {last_error}"
        self.logger.error(error_msg)
        self._log_metrics({
            'operation': 'execute_query',
            'status': 'max_retries_exceeded',
            'attempts': max_retries + 1,
            'final_error': str(last_error)
        })
        raise Exception(error_msg)
    
    def _log_metrics(self, metrics: Dict[str, Any]) -> None:
        """
        Log structured metrics for monitoring.
        
        Args:
            metrics: Dictionary of metrics to log
            
        Example:
            self._log_metrics({
                'operation': 'bulk_insert',
                'rows': 1000,
                'duration_ms': 150.5,
                'status': 'success'
            })
        """
        # Add common metadata
        metrics.update({
            'timestamp': datetime.utcnow().isoformat(),
            'client': self.__class__.__name__,
            'database': self.config.connection.database
        })
        
        # Log as structured JSON for easy parsing
        self.logger.info(f"METRIC::{json.dumps(metrics)}")
        
        # Update internal stats
        if 'operation' in metrics and 'duration_ms' in metrics:
            self._operation_count += 1
            self._total_time += metrics['duration_ms']
    
    def _format_error(self, error: Exception, context: Dict[str, Any]) -> str:
        """
        Format error messages with context for better debugging.
        
        Args:
            error: The exception that occurred
            context: Additional context about the operation
            
        Returns:
            Formatted error message
            
        Example:
            error_msg = self._format_error(e, {
                'operation': 'insert_batch',
                'ticker': 'AAPL',
                'rows': 100
            })
        """
        # Build context string
        context_str = ', '.join(f"{k}={v}" for k, v in context.items())
        
        # Get PostgreSQL specific error info if available
        pg_details = []
        if hasattr(error, 'pgcode'):
            pg_details.append(f"pgcode={error.pgcode}")
        if hasattr(error, 'pgerror'):
            pg_details.append(f"pgerror={error.pgerror}")
        
        pg_str = ', '.join(pg_details) if pg_details else ''
        
        # Format complete message
        parts = [
            f"Error: {str(error)}",
            f"Context: {context_str}" if context_str else None,
            f"PostgreSQL: {pg_str}" if pg_str else None
        ]
        
        return ' | '.join(filter(None, parts))
    
    def _calculate_backoff(self, attempt: int) -> float:
        """
        Calculate exponential backoff with jitter.
        
        Args:
            attempt: Current attempt number (0-based)
            
        Returns:
            Wait time in seconds
        """
        # Exponential backoff: 2^attempt * base
        base_wait = 0.5  # Start with 0.5 seconds
        max_wait = 30.0  # Cap at 30 seconds
        
        wait = min(base_wait * (2 ** attempt), max_wait)
        
        # Add jitter to prevent thundering herd
        jitter = random.uniform(0, wait * 0.1)  # Up to 10% jitter
        
        return wait + jitter
    
    def _log_pool_stats(self) -> None:
        """Log connection pool statistics for debugging."""
        try:
            # Note: These attributes might not be available in all pool implementations
            stats = {
                'pool_size': getattr(self._pool, 'maxconn', 'unknown'),
                'available': getattr(self._pool, '_idle', 'unknown'),
                'in_use': getattr(self._pool, '_used', 'unknown')
            }
            self.logger.debug(f"Connection pool stats: {stats}")
        except Exception:
            # Don't fail if we can't get pool stats
            pass
    
    def get_client_stats(self) -> Dict[str, Any]:
        """
        Get performance statistics for this client.
        
        Returns:
            Dictionary with operation count and average time
        """
        avg_time = (self._total_time / self._operation_count) if self._operation_count > 0 else 0
        
        return {
            'client': self.__class__.__name__,
            'operation_count': self._operation_count,
            'total_time_ms': self._total_time,
            'average_time_ms': avg_time
        }
    
    def execute_sql_file(self, filepath: str, commit: bool = True) -> None:
        """
        Execute SQL commands from a file.
        
        Args:
            filepath: Path to SQL file
            commit: Whether to commit after execution
            
        Useful for running schema creation scripts.
        """
        try:
            with open(filepath, 'r') as f:
                sql_content = f.read()
            
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    self.logger.info(f"Executing SQL file: {filepath}")
                    cur.execute(sql_content)
                    
                    if commit:
                        conn.commit()
                        self.logger.info("SQL file executed and committed successfully")
                    
        except Exception as e:
            error_msg = self._format_error(e, {'file': filepath})
            self.logger.error(f"Failed to execute SQL file: {error_msg}")
            raise
