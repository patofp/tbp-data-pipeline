#!/usr/bin/env python3
"""
Configuration loader for TBP Data Pipeline.
Handles YAML loading with template substitution for environment variables.
"""

import os
import yaml
from string import Template
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class TickerConfig:
    """Ticker configuration data class."""
    symbol: str
    name: str
    sector: str
    asset_class: str
    priority: int


@dataclass
class S3Credentials:
    """S3 credentials data class."""
    access_key: str
    secret_key: str


@dataclass
class S3PathStructure:
    """S3 path structure data class."""
    day_aggs: str
    minute_aggs: str
    trades: str
    quotes: str


@dataclass
class S3Config:
    """S3 configuration data class."""
    endpoint: str
    bucket_name: str
    region: str

    credentials: S3Credentials
    path_structure: S3PathStructure

    file_format: str
    compression: str
    encoding: str
    header_row: bool

    connect_timeout_seconds: int
    read_timeout_seconds: int
    max_retries: int
    multipart_threshold_mb: int


@dataclass
class DatabaseConnection:
    """Database connection data class."""
    host: str
    port: str
    database: str
    username: str
    password: str


@dataclass
class DatabasePool:
    """Database pool data class."""
    min_connection: int
    max_connection: int
    connection_timeout_seconds: int
    idle_timeout_seconds: int


@dataclass
class DatabaseTables:
    """Database tables data class."""
    market_data_raw: str
    ingestion_log: str
    data_quality: str


@dataclass
class DatabaseColumnMapping:
    """Database column mapping data class."""
    ticker: str
    volume: str
    open: str
    close: str
    high: str
    low: str
    window_start: str
    transactions: str
    vwap: str


@dataclass
class DatabaseConfig:
    """Database configuration data class."""
    connection: DatabaseConnection
    pool: DatabasePool
    schema: str
    tables: DatabaseTables
    batch_insert_size: float
    upsert_on_conflict: bool
    column_mapping: DatabaseColumnMapping


class ConfigLoader:
    """Load and validate configuration from YAML files with template substitution."""

    def __init__(self, config_dir: str = "config"):
        for file in os.listdir(config_dir):
            if file.endswith('.yaml'):
                file_path = Path(config_dir) / file
                setattr(self, file.stem,
                        self._load_yaml_with_substitution(file_path))
                

    def get_all_tickers(self) -> List[TickerConfig]:
        """Get all configured tickers."""
        pass

    def get_ticker_groups(self) -> Dict[str, List[str]]:
        """Get ticker groupings."""
        pass

    def get_s3_config(self) -> S3Config:
        """Get S3 configuration with resolved credentials."""
        pass

    def get_database_config(self) -> DatabaseConfig:
        """Get database configuration with resolved credentials."""
        pass

    def validate_environment(self) -> bool:
        """Validate required environment variables are set."""
        pass

    def _load_yaml_with_substitution(self, file_path: Path) -> Dict[str, Any]:
        """Load YAML file with environment variable substitution."""
        import re

        # Read the YAML file content
        with open(file_path, 'r') as f:
            content = f.read()

        # Find all ${VARIABLE} patterns and replace with env var values
        def replace_env_var(match):
            var_name = match.group(1)
            value = os.environ.get(var_name)
            if value is None:
                logger.warning(
                    f"Environment variable '{var_name}' not found, keeping placeholder")
                # Keep the original ${VARIABLE} if not found
                return match.group(0)
            return value

        # Replace all ${VARIABLE} patterns with environment variable values
        pattern = r'\$\{([^}]+)\}'
        substituted_content = re.sub(pattern, replace_env_var, content)

        # Parse the substituted YAML content
        config = yaml.safe_load(substituted_content)

        return config

    def _validate_required_vars(self, required_vars: List[str]) -> List[str]:
        """Validate required environment variables and return missing ones."""
        pass
