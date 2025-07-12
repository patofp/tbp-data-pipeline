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
                
                config = ConfigLoader("config")
                
                # Test that we can load tickers if config file exists
                if (project_root / 'config' / 'instruments.yml').exists():
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
        """Test that ConfigLoader fails fast when config directory is missing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Change to temp directory with no config files
            original_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                
                # Should fail fast when config directory doesn't exist
                with pytest.raises(FileNotFoundError, match="Configuration directory .* not found"):
                    ConfigLoader("config")
                    
            finally:
                os.chdir(original_cwd)


@pytest.mark.integration
class TestConfigLoaderEnvironmentIntegration:
    """Test ConfigLoader with environment variable interactions."""
    
    def test_environment_variable_substitution(self):
        """Test that YAML config files properly substitute environment variables."""
        # Test uses actual config files with env var substitution
        original_cwd = os.getcwd()
        try:
            project_root = Path(__file__).parent.parent.parent
            os.chdir(project_root)
            
            with pytest.MonkeyPatch.context() as m:
                # Set test environment variables
                m.setenv('POLYGON_S3_ACCESS_KEY', 'test_substitution_key')
                m.setenv('POLYGON_S3_SECRET_KEY', 'test_substitution_secret')
                m.setenv('POLYGON_API_KEY', 'test_api')
                m.setenv('DB_HOST', 'test_substitution_host')
                m.setenv('DB_PORT', '9999')
                m.setenv('DB_NAME', 'test_db')
                m.setenv('DB_USER', 'test_user')
                m.setenv('DB_PASSWORD', 'test_pass')
                
                config = ConfigLoader("config")
                
                # Verify environment variable substitution works in YAML files
                s3_config = config.get_s3_config()
                assert s3_config.credentials.access_key == "test_substitution_key"
                assert s3_config.credentials.secret_key == "test_substitution_secret"
                
                db_config = config.get_database_config()
                assert db_config.connection.host == "test_substitution_host"
                assert db_config.connection.port == "9999"
                
        finally:
            os.chdir(original_cwd)