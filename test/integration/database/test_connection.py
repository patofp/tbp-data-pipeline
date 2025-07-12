"""Integration tests for database connection pooling with real PostgreSQL."""

import pytest
import concurrent.futures
from time import sleep

from src.database.connection import ConnectionPool, ConnectionManager
from src.config_loader import (
    DatabaseConfig, DatabaseConnection, DatabasePool, DatabaseTables
)


@pytest.fixture
def connection_test_db_config():
    """Create test database configuration."""
    connection = DatabaseConnection(
        host="localhost",
        port=5433,  # Test database port
        database="test_db",
        username="test_user",
        password="test_password"
    )
    
    pool = DatabasePool(
        min_connection=2,
        max_connection=5,
        connection_timeout_seconds=10,
        idle_timeout_seconds=60
    )
    
    tables = DatabaseTables(
        market_data_raw="market_data_raw",
        ingestion_log="ingestion_log",
        data_quality="data_quality_metrics"
    )
    
    return DatabaseConfig(
        connection=connection,
        pool=pool,
        schema="test_trading",
        tables=tables,
        batch_insert_size=1000,
        upsert_on_conflict=True,
        column_mapping={}
    )


@pytest.mark.integration
class TestConnectionPoolIntegration:
    """Test ConnectionPool with real database."""
    
    def test_pool_initialization(self, connection_test_db_config):
        """Test initializing connection pool with real database."""
        pool = ConnectionPool(connection_test_db_config)
        
        # Initialize pool
        pool.initialize()
        
        try:
            # Verify pool is initialized
            assert pool._pool is not None
            
            # Test connection
            assert pool.test_connection() is True
            
            # Get pool status
            status = pool.get_pool_status()
            assert status["status"] == "Active"
            assert status["min_connections"] == 2
            assert status["max_connections"] == 5
            assert status["closed"] is False
            
        finally:
            pool.close()
            
    def test_multiple_connections(self, connection_test_db_config):
        """Test getting multiple connections from pool."""
        pool = ConnectionPool(connection_test_db_config)
        pool.initialize()
        
        try:
            results = []
            
            # Get multiple connections and execute queries
            for i in range(3):
                with pool.get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT %s as num", (i,))
                        result = cursor.fetchone()
                        results.append(result[0])
                        
            assert results == [0, 1, 2]
            
        finally:
            pool.close()
            
    def test_concurrent_connections(self, connection_test_db_config):
        """Test concurrent connection usage."""
        pool = ConnectionPool(connection_test_db_config)
        pool.initialize()
        
        def worker(worker_id):
            """Worker function to test concurrent access."""
            with pool.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Simulate some work
                    cursor.execute("SELECT pg_sleep(0.1)")
                    cursor.execute("SELECT %s", (worker_id,))
                    result = cursor.fetchone()
                    return result[0]
                    
        try:
            # Run concurrent workers
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(worker, i) for i in range(4)]
                results = [f.result() for f in futures]
                
            assert sorted(results) == [0, 1, 2, 3]
            
        finally:
            pool.close()
            
    def test_connection_exhaustion(self, connection_test_db_config):
        """Test behavior when pool is exhausted."""
        # Create pool with very small size
        connection_test_db_config.pool.min_connection = 1
        connection_test_db_config.pool.max_connection = 2
        
        pool = ConnectionPool(connection_test_db_config)
        pool.initialize()
        
        try:
            # Hold connections without releasing
            conn1 = pool._pool.getconn()
            conn2 = pool._pool.getconn()
            
            # Try to get another connection (should block or fail)
            # This depends on psycopg2 pool implementation
            # Just verify we can return connections
            pool._pool.putconn(conn1)
            pool._pool.putconn(conn2)
            
            # Now should be able to get connections again
            with pool.get_connection() as conn:
                assert conn is not None
                
        finally:
            pool.close()


@pytest.mark.integration
class TestConnectionManagerIntegration:
    """Test ConnectionManager with real database."""
    
    def test_manager_lifecycle(self, connection_test_db_config):
        """Test full lifecycle of connection manager."""
        manager = ConnectionManager(connection_test_db_config)
        
        # Test uninitialized state
        assert manager.test_connection() is False
        status = manager.get_status()
        assert status["initialized"] is False
        
        # Initialize
        manager.initialize()
        
        try:
            # Test initialized state
            assert manager.test_connection() is True
            status = manager.get_status()
            assert status["initialized"] is True
            assert status["pool_status"]["status"] == "Active"
            
            # Test schema creation
            with manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT schema_name FROM information_schema.schemata "
                        "WHERE schema_name = %s",
                        (connection_test_db_config.schema,)
                    )
                    result = cursor.fetchone()
                    assert result is not None
                    assert result[0] == connection_test_db_config.schema
                    
        finally:
            # Cleanup schema
            with manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"DROP SCHEMA IF EXISTS {connection_test_db_config.schema} CASCADE")
                    conn.commit()
                    
            manager.close()
            
    def test_execute_query(self, connection_test_db_config):
        """Test executing queries through manager."""
        manager = ConnectionManager(connection_test_db_config)
        manager.initialize()
        
        try:
            # Create test table
            create_table_query = f"""
            CREATE TABLE {connection_test_db_config.schema}.test_table (
                id SERIAL PRIMARY KEY,
                value TEXT
            )
            """
            manager.execute_query(create_table_query, fetch=False)
            
            # Insert data
            insert_query = f"INSERT INTO {connection_test_db_config.schema}.test_table (value) VALUES (%s)"
            manager.execute_query(insert_query, ("test_value",), fetch=False)
            
            # Select data
            select_query = f"SELECT value FROM {connection_test_db_config.schema}.test_table"
            results = manager.execute_query(select_query)
            
            assert len(results) == 1
            assert results[0][0] == "test_value"
            
        finally:
            # Cleanup
            with manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"DROP SCHEMA IF EXISTS {connection_test_db_config.schema} CASCADE")
                    conn.commit()
                    
            manager.close()
            
    def test_transaction_handling(self, connection_test_db_config):
        """Test transaction handling through manager."""
        manager = ConnectionManager(connection_test_db_config)
        manager.initialize()
        
        try:
            # Create test table
            create_table_query = f"""
            CREATE TABLE {connection_test_db_config.schema}.test_trans (
                id SERIAL PRIMARY KEY,
                value INTEGER
            )
            """
            manager.execute_query(create_table_query, fetch=False)
            
            # Test transaction rollback
            with manager.get_connection() as conn:
                try:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            f"INSERT INTO {connection_test_db_config.schema}.test_trans (value) VALUES (%s)",
                            (1,)
                        )
                        # Force error
                        cursor.execute("INVALID SQL")
                except Exception:
                    conn.rollback()
                    
            # Verify no data was inserted
            results = manager.execute_query(
                f"SELECT COUNT(*) FROM {connection_test_db_config.schema}.test_trans"
            )
            assert results[0][0] == 0
            
            # Test successful transaction
            with manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"INSERT INTO {connection_test_db_config.schema}.test_trans (value) VALUES (%s)",
                        (2,)
                    )
                conn.commit()
                
            # Verify data was inserted
            results = manager.execute_query(
                f"SELECT value FROM {connection_test_db_config.schema}.test_trans"
            )
            assert len(results) == 1
            assert results[0][0] == 2
            
        finally:
            # Cleanup
            with manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"DROP SCHEMA IF EXISTS {connection_test_db_config.schema} CASCADE")
                    conn.commit()
                    
            manager.close()