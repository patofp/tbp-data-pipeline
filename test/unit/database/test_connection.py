"""Unit tests for database connection pooling."""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock

from src.database.connection import ConnectionPool, ConnectionManager
from src.config_loader import DatabaseConfig, DatabaseConnection, DatabasePool


@pytest.fixture
def mock_db_config():
    """Create mock database configuration."""
    config = MagicMock(spec=DatabaseConfig)
    
    # Mock connection settings
    config.connection = MagicMock(spec=DatabaseConnection)
    config.connection.host = "localhost"
    config.connection.port = 5432
    config.connection.database = "test_db"
    config.connection.username = "test_user"
    config.connection.password = "test_pass"
    
    # Mock pool settings
    config.pool = MagicMock(spec=DatabasePool)
    config.pool.min_connection = 2
    config.pool.max_connection = 10
    config.pool.connection_timeout_seconds = 30
    config.pool.idle_timeout_seconds = 300
    
    # Mock schema
    config.schema = "test_schema"
    
    return config


class TestConnectionPool:
    """Test ConnectionPool class."""
    
    @patch('src.database.connection.pool.ThreadedConnectionPool')
    def test_initialize_success(self, mock_pool_class, mock_db_config):
        """Test successful pool initialization."""
        # Setup
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool
        
        pool = ConnectionPool(mock_db_config)
        
        # Execute
        pool.initialize()
        
        # Verify
        mock_pool_class.assert_called_once_with(
            minconn=2,
            maxconn=10,
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass",
            connect_timeout=30,
            options="-c idle_in_transaction_session_timeout=300000"
        )
        assert pool._pool == mock_pool
        
    @patch('src.database.connection.pool.ThreadedConnectionPool')
    def test_initialize_failure(self, mock_pool_class, mock_db_config):
        """Test pool initialization failure."""
        # Setup
        mock_pool_class.side_effect = Exception("Connection failed")
        
        pool = ConnectionPool(mock_db_config)
        
        # Execute & Verify
        with pytest.raises(Exception, match="Connection failed"):
            pool.initialize()
            
    def test_close_pool(self, mock_db_config):
        """Test closing connection pool."""
        # Setup
        pool = ConnectionPool(mock_db_config)
        mock_pool = MagicMock()
        pool._pool = mock_pool
        
        # Execute
        pool.close()
        
        # Verify
        mock_pool.closeall.assert_called_once()
        assert pool._pool is None
        
    def test_get_connection_not_initialized(self, mock_db_config):
        """Test getting connection from uninitialized pool."""
        pool = ConnectionPool(mock_db_config)
        
        with pytest.raises(RuntimeError, match="Connection pool not initialized"):
            with pool.get_connection():
                pass
                
    @patch('src.database.connection.pool.ThreadedConnectionPool')
    def test_get_connection_success(self, mock_pool_class, mock_db_config):
        """Test successfully getting connection from pool."""
        # Setup
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_pool.getconn.return_value = mock_conn
        mock_pool_class.return_value = mock_pool
        
        pool = ConnectionPool(mock_db_config)
        pool.initialize()
        
        # Execute
        with pool.get_connection() as conn:
            assert conn == mock_conn
            
        # Verify
        mock_pool.getconn.assert_called_once()
        mock_pool.putconn.assert_called_once_with(mock_conn)
        
    @patch('src.database.connection.pool.ThreadedConnectionPool')
    def test_test_connection_success(self, mock_pool_class, mock_db_config):
        """Test successful connection test."""
        # Setup
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_pool.getconn.return_value = mock_conn
        mock_pool_class.return_value = mock_pool
        
        pool = ConnectionPool(mock_db_config)
        pool.initialize()
        
        # Execute
        result = pool.test_connection()
        
        # Verify
        assert result is True
        mock_cursor.execute.assert_called_once_with("SELECT 1")
        
    def test_get_pool_status_not_initialized(self, mock_db_config):
        """Test getting pool status when not initialized."""
        pool = ConnectionPool(mock_db_config)
        
        status = pool.get_pool_status()
        
        assert status == {"status": "Not initialized"}
        
    @patch('src.database.connection.pool.ThreadedConnectionPool')
    def test_get_pool_status_active(self, mock_pool_class, mock_db_config):
        """Test getting pool status when active."""
        # Setup
        mock_pool = MagicMock()
        mock_pool.minconn = 2
        mock_pool.maxconn = 10
        mock_pool.closed = False
        mock_pool_class.return_value = mock_pool
        
        pool = ConnectionPool(mock_db_config)
        pool.initialize()
        
        # Execute
        status = pool.get_pool_status()
        
        # Verify
        assert status == {
            "status": "Active",
            "min_connections": 2,
            "max_connections": 10,
            "closed": False
        }


class TestConnectionManager:
    """Test ConnectionManager class."""
    
    @patch('src.database.connection.ConnectionPool')
    def test_initialize_success(self, mock_pool_class, mock_db_config):
        """Test successful manager initialization."""
        # Setup
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool
        
        manager = ConnectionManager(mock_db_config)
        
        # Execute
        manager.initialize()
        
        # Verify
        mock_pool.initialize.assert_called_once()
        assert manager._initialized is True
        
    @patch('src.database.connection.ConnectionPool')
    def test_close(self, mock_pool_class, mock_db_config):
        """Test closing connection manager."""
        # Setup
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool
        
        manager = ConnectionManager(mock_db_config)
        manager._initialized = True
        
        # Execute
        manager.close()
        
        # Verify
        mock_pool.close.assert_called_once()
        assert manager._initialized is False
        
    def test_get_connection_not_initialized(self, mock_db_config):
        """Test getting connection when manager not initialized."""
        manager = ConnectionManager(mock_db_config)
        
        with pytest.raises(RuntimeError, match="Connection manager not initialized"):
            with manager.get_connection():
                pass
                
    @patch('src.database.connection.ConnectionPool')
    def test_execute_query_with_fetch(self, mock_pool_class, mock_db_config):
        """Test executing query with fetch."""
        # Setup
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1, 2), (3, 4)]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
        mock_pool_class.return_value = mock_pool
        
        manager = ConnectionManager(mock_db_config)
        manager._initialized = True
        
        # Execute
        result = manager.execute_query("SELECT * FROM test", fetch=True, params=None)
        
        # Verify
        assert result == [(1, 2), (3, 4)]
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test", None)
        mock_cursor.fetchall.assert_called_once()
        
    @patch('src.database.connection.ConnectionPool')
    def test_execute_query_without_fetch(self, mock_pool_class, mock_db_config):
        """Test executing query without fetch."""
        # Setup
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_pool.get_connection.return_value.__enter__.return_value = mock_conn
        mock_pool_class.return_value = mock_pool
        
        manager = ConnectionManager(mock_db_config)
        manager._initialized = True
        
        # Execute
        result = manager.execute_query("INSERT INTO test VALUES (1)", fetch=False, params=None)
        
        # Verify
        assert result is None
        mock_cursor.execute.assert_called_once_with("INSERT INTO test VALUES (1)", None)
        mock_conn.commit.assert_called_once()
        
    @patch('src.database.connection.ConnectionPool')
    def test_test_connection(self, mock_pool_class, mock_db_config):
        """Test connection testing through manager."""
        # Setup
        mock_pool = MagicMock()
        mock_pool.test_connection.return_value = True
        mock_pool_class.return_value = mock_pool
        
        manager = ConnectionManager(mock_db_config)
        manager._initialized = True
        
        # Execute
        result = manager.test_connection()
        
        # Verify
        assert result is True
        mock_pool.test_connection.assert_called_once()
        
    @patch('src.database.connection.ConnectionPool')
    def test_get_status(self, mock_pool_class, mock_db_config):
        """Test getting manager status."""
        # Setup
        mock_pool = MagicMock()
        mock_pool.get_pool_status.return_value = {"status": "Active"}
        mock_pool_class.return_value = mock_pool
        
        manager = ConnectionManager(mock_db_config)
        manager._initialized = True
        
        # Execute
        status = manager.get_status()
        
        # Verify
        assert status == {
            "initialized": True,
            "pool_status": {"status": "Active"}
        }