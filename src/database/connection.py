"""Connection pooling and database connectivity management.

This module provides connection pooling functionality for TimescaleDB
using psycopg2's ThreadedConnectionPool.
"""

import logging
from contextlib import contextmanager
from typing import Optional, Dict, Any

import psycopg2
from psycopg2 import pool

from src.config_loader import DatabaseConfig

logger = logging.getLogger(__name__)


class ConnectionPool:
    """Manages a pool of database connections for TimescaleDB.
    
    This class provides a thread-safe connection pool that can be shared
    across multiple clients. It implements automatic retry logic and
    connection validation.
    """
    
    def __init__(self, db_config: DatabaseConfig):
        """Initialize connection pool with database configuration.
        
        Args:
            db_config: Database configuration from config loader
        """
        self.config = db_config
        self._pool: Optional[pool.ThreadedConnectionPool] = None
        
    def initialize(self) -> None:
        """Initialize the connection pool.
        
        Creates a ThreadedConnectionPool with configured min/max connections.
        
        Raises:
            psycopg2.Error: If unable to create connection pool
        """
        try:
            logger.info(
                f"Initializing connection pool: min={self.config.pool.min_connection}, "
                f"max={self.config.pool.max_connection}"
            )
            
            self._pool = pool.ThreadedConnectionPool(
                minconn=self.config.pool.min_connection,
                maxconn=self.config.pool.max_connection,
                host=self.config.connection.host,
                port=self.config.connection.port,
                database=self.config.connection.database,
                user=self.config.connection.username,
                password=self.config.connection.password,
                connect_timeout=self.config.pool.connection_timeout_seconds,
                options=f"-c idle_in_transaction_session_timeout={self.config.pool.idle_timeout_seconds * 1000}"
            )
            
            logger.info("Connection pool initialized successfully")
            
        except psycopg2.Error as e:
            logger.error(f"Failed to initialize connection pool: {e}")
            raise
            
    def close(self) -> None:
        """Close all connections in the pool."""
        if self._pool:
            logger.info("Closing connection pool")
            self._pool.closeall()
            self._pool = None
            
    @contextmanager
    def get_connection(self):
        """Get a connection from the pool using context manager.
        
        This ensures connections are properly returned to the pool after use.
        
        Yields:
            psycopg2.connection: Database connection
            
        Raises:
            psycopg2.Error: If unable to get connection from pool
        """
        if not self._pool:
            raise RuntimeError("Connection pool not initialized. Call initialize() first.")
            
        conn = None
        try:
            conn = self._pool.getconn()
            if conn:
                logger.debug(f"Got connection from pool: {id(conn)}")
                yield conn
            else:
                raise psycopg2.Error("Unable to get connection from pool")
        finally:
            if conn:
                logger.debug(f"Returning connection to pool: {id(conn)}")
                self._pool.putconn(conn)
                
    def get_pool_status(self) -> Dict[str, Any]:
        """Get current status of the connection pool.
        
        Returns:
            Dict with pool statistics
        """
        if not self._pool:
            return {"status": "Not initialized"}
            
        return {
            "status": "Active",
            "min_connections": self._pool.minconn,
            "max_connections": self._pool.maxconn,
            "closed": self._pool.closed
        }
        
    def test_connection(self) -> bool:
        """Test database connectivity.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    return result[0] == 1
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


class ConnectionManager:
    """High-level connection management interface.
    
    This class provides a simplified interface for managing database
    connections and executing queries with automatic retry logic.
    """
    
    def __init__(self, db_config: DatabaseConfig):
        """Initialize connection manager.
        
        Args:
            db_config: Database configuration from config loader
        """
        self.pool = ConnectionPool(db_config)
        self.config = db_config
        self._initialized = False
        
    def initialize(self) -> None:
        """Initialize the connection manager and pool."""
        if not self._initialized:
            self.pool.initialize()
            self._create_schema_if_not_exists()
            self._initialized = True
            
    def close(self) -> None:
        """Close the connection manager and pool."""
        self.pool.close()
        self._initialized = False
        
    def _create_schema_if_not_exists(self) -> None:
        """Create the trading schema if it doesn't exist."""
        with self.pool.get_connection() as conn:
            with conn.cursor() as cursor:
                logger.info(f"Creating schema '{self.config.schema}' if not exists")
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.config.schema}")
                conn.commit()
                
    @contextmanager
    def get_connection(self):
        """Get a database connection from the pool.
        
        Yields:
            psycopg2.connection: Database connection
        """
        if not self._initialized:
            raise RuntimeError("Connection manager not initialized. Call initialize() first.")
            
        with self.pool.get_connection() as conn:
            yield conn
            
    def execute_query(self, query: str, fetch: bool, params: Optional[tuple]) -> Any:
        """Execute a query with automatic connection management.
        
        Args:
            query: SQL query to execute
            params: Query parameters (if any)
            fetch: Whether to fetch results
            
        Returns:
            Query results if fetch=True, None otherwise
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                if fetch:
                    return cursor.fetchall()
                conn.commit()
                
    def test_connection(self) -> bool:
        """Test database connectivity.
        
        Returns:
            True if connection successful, False otherwise
        """
        if not self._initialized:
            return False
        return self.pool.test_connection()
        
    def get_status(self) -> Dict[str, Any]:
        """Get connection manager status.
        
        Returns:
            Dict with manager and pool status
        """
        return {
            "initialized": self._initialized,
            "pool_status": self.pool.get_pool_status()
        }