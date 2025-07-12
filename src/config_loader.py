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

    def __init__(self, config_dir: str):
        self.config_dir = Path(config_dir)
        self.configs = {}

        # Load all YAML/YML files in the config directory
        # Configuration MUST exist - fail fast if missing
        if not os.path.exists(config_dir):
            raise FileNotFoundError(f"Configuration directory '{config_dir}' not found. Configuration is required.")
        
        for file in os.listdir(config_dir):
            if file.endswith((".yaml", ".yml")):
                file_path = Path(config_dir) / file
                config_name = file.replace(".yml", "").replace(".yaml", "")
                self.configs[config_name] = self._load_yaml_with_substitution(file_path)
                setattr(self, config_name, self.configs[config_name])

    def get_all_tickers(self) -> List[TickerConfig]:
        """Get all configured tickers."""
        tickers = []
        if "instruments" in self.configs:
            for ticker_data in self.configs["instruments"].get("tickers", []):
                ticker = TickerConfig(
                    symbol=ticker_data["symbol"],
                    name=ticker_data["name"],
                    sector=ticker_data["sector"],
                    asset_class=ticker_data["asset_class"],
                    priority=ticker_data["priority"],
                )
                tickers.append(ticker)
        return tickers

    def get_ticker_groups(self) -> Dict[str, List[str]]:
        """Get ticker groupings."""
        groups = {}
        if "instruments" in self.configs:
            groups_data = self.configs["instruments"].get("ticker_groups", {})
            for group_name, group_info in groups_data.items():
                groups[group_name] = group_info.get("symbols", [])
        return groups

    def get_s3_config(self) -> S3Config:
        """Get S3 configuration with resolved credentials."""
        if "s3" not in self.configs:
            raise ValueError("S3 configuration not found")

        s3_data = self.configs["s3"]["s3_config"]

        # Create credentials object
        credentials = S3Credentials(
            access_key=s3_data["credentials"]["access_key"],
            secret_key=s3_data["credentials"]["secret_key"],
        )

        # Create path structure object
        path_structure = S3PathStructure(
            day_aggs=s3_data["path_structure"]["day_aggs"],
            minute_aggs=s3_data["path_structure"]["minute_aggs"],
            trades=s3_data["path_structure"]["trades"],
            quotes=s3_data["path_structure"]["quotes"],
        )

        # Create S3 config object
        return S3Config(
            endpoint=s3_data["endpoint"],
            bucket_name=s3_data["bucket_name"],
            region=s3_data["region"],
            credentials=credentials,
            path_structure=path_structure,
            file_format=s3_data["file_format"],
            compression=s3_data["compression"],
            encoding=s3_data["encoding"],
            header_row=s3_data["header_row"],
            connect_timeout_seconds=s3_data["connect_timeout_seconds"],
            read_timeout_seconds=s3_data["read_timeout_seconds"],
            max_retries=s3_data["max_retries"],
            multipart_threshold_mb=s3_data["multipart_threshold_mb"],
        )

    def get_database_config(self) -> DatabaseConfig:
        """Get database configuration with resolved credentials."""
        if "database" not in self.configs:
            raise ValueError("Database configuration not found")

        db_data = self.configs["database"]["database"]

        # Create connection object
        connection = DatabaseConnection(
            host=db_data["connection"]["host"],
            port=db_data["connection"]["port"],
            database=db_data["connection"]["database"],
            username=db_data["connection"]["username"],
            password=db_data["connection"]["password"],
        )

        # Create pool object
        pool = DatabasePool(
            min_connection=db_data["pool"]["min_connections"],
            max_connection=db_data["pool"]["max_connections"],
            connection_timeout_seconds=db_data["pool"]["connection_timeout_seconds"],
            idle_timeout_seconds=db_data["pool"]["idle_timeout_seconds"],
        )

        # Create tables object
        tables = DatabaseTables(
            market_data_raw=db_data["tables"]["market_data_raw"],
            ingestion_log=db_data["tables"]["ingestion_log"],
            data_quality=db_data["tables"]["data_quality"],
        )

        # Create column mapping object
        column_mapping = DatabaseColumnMapping(
            ticker=db_data["column_mapping"]["ticker"],
            volume=db_data["column_mapping"]["volume"],
            open=db_data["column_mapping"]["open"],
            close=db_data["column_mapping"]["close"],
            high=db_data["column_mapping"]["high"],
            low=db_data["column_mapping"]["low"],
            window_start=db_data["column_mapping"]["window_start"],
            transactions=db_data["column_mapping"]["transactions"],
        )

        # Create database config object
        return DatabaseConfig(
            connection=connection,
            pool=pool,
            schema=db_data["schema"],
            tables=tables,
            batch_insert_size=db_data["batch_insert_size"],
            upsert_on_conflict=db_data["upsert_on_conflict"],
            column_mapping=column_mapping,
        )

    def validate_environment(self) -> bool:
        """Validate required environment variables are set."""
        required_vars = [
            "POLYGON_S3_ACCESS_KEY",
            "POLYGON_S3_SECRET_KEY",
            "POLYGON_API_KEY",
            "DB_HOST",
            "DB_PORT",
            "DB_NAME",
            "DB_USER",
            "DB_PASSWORD",
            "LOG_LEVEL",
        ]

        missing_vars = self._validate_required_vars(required_vars)

        if missing_vars:
            logger.error(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )
            return False

        logger.info("All required environment variables are set")
        return True

    def _load_yaml_with_substitution(self, file_path: Path) -> Dict[str, Any]:
        """Load YAML file with environment variable substitution."""
        import re

        # Read the YAML file content
        with open(file_path, "r") as f:
            content = f.read()

        # Find all ${VARIABLE} patterns and replace with env var values
        def replace_env_var(match):
            var_expr = match.group(1)

            # Check if there's a default value (e.g., ${VAR:-default})
            if ":-" in var_expr:
                var_name, default_value = var_expr.split(":-", 1)
                value = os.environ.get(var_name)
                if value is None:
                    logger.debug(
                        f"Using default value '{default_value}' for '{var_name}'"
                    )
                    return default_value
                return value
            else:
                # No default value specified
                var_name = var_expr
                value = os.environ.get(var_name)
                if value is None:
                    logger.warning(
                        f"Environment variable '{var_name}' not found, keeping placeholder"
                    )
                    # Keep the original ${VARIABLE} if not found
                    return match.group(0)
                return value

        # Replace all ${VARIABLE} patterns with environment variable values
        pattern = r"\$\{([^}]+)\}"
        substituted_content = re.sub(pattern, replace_env_var, content)

        # Parse the substituted YAML content
        config = yaml.safe_load(substituted_content)

        return config

    def _validate_required_vars(self, required_vars: List[str]) -> List[str]:
        """Validate required environment variables and return missing ones."""
        missing_vars = []
        for var in required_vars:
            if var not in os.environ or not os.environ[var]:
                missing_vars.append(var)
        return missing_vars
