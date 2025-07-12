#!/usr/bin/env python3
"""
Integration tests for ConfigLoader.

These tests use real file system and environment but don't require Docker.
"""

import pytest
import os
import tempfile
from pathlib import Path

from src.config_loader import ConfigLoader


@pytest.mark.integration
class TestConfigLoaderFileSystem:
    """Test ConfigLoader with real file system operations."""
    
    def test_loading_from_actual_files(self):
        """Test loading configuration from actual project files."""
        # Change to project root
        original_cwd = os.getcwd()
        try:
            project_root = Path(__file__).parent.parent.parent
            os.chdir(project_root)
            
            # Set minimal required environment
            with pytest.MonkeyPatch.context() as m:
                m.setenv('POLYGON_S3_ACCESS_KEY', 'test_key')
                m.setenv('POLYGON_S3_SECRET_KEY', 'test_secret')
                m.setenv('POLYGON_API_KEY', 'test_api')
                m.setenv('DB_HOST', 'localhost')
                m.setenv('DB_PORT', '5432')
                m.setenv('DB_NAME', 'test_db')
                m.setenv('DB_USER', 'test_user')
                m.setenv('DB_PASSWORD', 'test_pass')
                
                config = ConfigLoader()
                
                # Test that we can load tickers if config file exists
                if (project_root / 'config' / 'tickers.yaml').exists():
                    tickers = config.get_all_tickers()
                    assert isinstance(tickers, list)
                    assert len(tickers) > 0
                    
                    # Test ticker groups
                    groups = config.get_ticker_groups()
                    assert isinstance(groups, dict)
                    assert len(groups) > 0
        finally:
            os.chdir(original_cwd)
    
    def test_config_file_missing(self):
        """Test behavior when config files are missing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Change to temp directory with no config files
            original_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                
                # Set required environment
                with pytest.MonkeyPatch.context() as m:
                    m.setenv('POLYGON_S3_ACCESS_KEY', 'test_key')
                    m.setenv('POLYGON_S3_SECRET_KEY', 'test_secret')
                    m.setenv('POLYGON_API_KEY', 'test_api')
                    m.setenv('DB_HOST', 'localhost')
                    m.setenv('DB_PORT', '5432')
                    m.setenv('DB_NAME', 'test_db')
                    m.setenv('DB_USER', 'test_user')
                    m.setenv('DB_PASSWORD', 'test_pass')
                    
                    config = ConfigLoader()
                    
                    # Should handle missing ticker file gracefully
                    tickers = config.get_all_tickers()
                    assert isinstance(tickers, list)
                    # Might be empty or have defaults depending on implementation
            finally:
                os.chdir(original_cwd)


@pytest.mark.integration
class TestConfigLoaderEnvironmentIntegration:
    """Test ConfigLoader with real environment variables."""
    
    def test_dotenv_loading(self):
        """Test loading environment from .env file if it exists."""
        # Create temporary .env file
        with tempfile.TemporaryDirectory() as tmpdir:
            env_file = Path(tmpdir) / '.env'
            env_file.write_text('''
POLYGON_S3_ACCESS_KEY=env_file_key
POLYGON_S3_SECRET_KEY=env_file_secret
POLYGON_API_KEY=env_file_api
DB_HOST=env_file_host
DB_PORT=5433
DB_NAME=env_file_db
DB_USER=env_file_user
DB_PASSWORD=env_file_pass
S3_ENDPOINT_URL=https://env.endpoint.com
LOG_LEVEL=INFO
''')
            
            original_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                
                # Clear any existing env vars to test .env loading
                env_vars_to_clear = [
                    'POLYGON_S3_ACCESS_KEY', 'POLYGON_S3_SECRET_KEY',
                    'POLYGON_API_KEY', 'DB_HOST', 'DB_PORT', 'DB_NAME',
                    'DB_USER', 'DB_PASSWORD', 'S3_ENDPOINT_URL', 'LOG_LEVEL'
                ]
                
                with pytest.MonkeyPatch.context() as m:
                    # Clear existing env vars
                    for var in env_vars_to_clear:
                        m.delenv(var, raising=False)
                    
                    # Force reload of environment
                    from dotenv import load_dotenv
                    load_dotenv(env_file, override=True)
                    
                    config = ConfigLoader()
                    
                    # Should load from .env file
                    s3_config = config.get_s3_config()
                    assert s3_config.endpoint == "https://env.endpoint.com"
                    
                    db_config = config.get_database_config()
                    assert db_config.connection.host == "env_file_host"
                    assert db_config.connection.port == "5433"
                    
            finally:
                os.chdir(original_cwd)
    
    def test_environment_override_priority(self):
        """Test that environment variables override .env file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create .env file with one set of values
            env_file = Path(tmpdir) / '.env'
            env_file.write_text('DB_HOST=file_host\nDB_PORT=5432')
            
            original_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                
                # Set environment variable to override
                with pytest.MonkeyPatch.context() as m:
                    # Set all required vars
                    m.setenv('POLYGON_S3_ACCESS_KEY', 'test_key')
                    m.setenv('POLYGON_S3_SECRET_KEY', 'test_secret')
                    m.setenv('POLYGON_API_KEY', 'test_api')
                    m.setenv('DB_HOST', 'env_host')  # Override file value
                    m.setenv('DB_PORT', '5433')      # Override file value
                    m.setenv('DB_NAME', 'test_db')
                    m.setenv('DB_USER', 'test_user')
                    m.setenv('DB_PASSWORD', 'test_pass')
                    
                    config = ConfigLoader()
                    db_config = config.get_database_config()
                    
                    # Should use environment value, not file value
                    assert db_config.connection.host == "env_host"
                    assert db_config.connection.port == "5433"
                    
            finally:
                os.chdir(original_cwd)