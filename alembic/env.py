"""Alembic environment script for TBP Data Pipeline.

This script handles database migrations for TimescaleDB.
"""
import os
import sys
from logging.config import fileConfig
from pathlib import Path
from urllib.parse import quote_plus

from sqlalchemy import engine_from_config
from sqlalchemy import pool
from sqlalchemy import text
from alembic import context

# Add project root to Python path
sys.path.append(str(Path(__file__).parents[1]))

# Import our config loader to get database configuration
from src.config_loader import ConfigLoader

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = None

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def get_database_url():
    """Get database URL from our configuration system."""
    config_loader = ConfigLoader("config")
    db_config = config_loader.get_database_config()
    
    # URL-encode the username and password to handle special characters
    username = quote_plus(db_config.connection.username)
    password = quote_plus(db_config.connection.password)
    
    # Build PostgreSQL URL with properly encoded credentials
    return (
        f"postgresql://{username}:"
        f"{password}@"
        f"{db_config.connection.host}:"
        f"{db_config.connection.port}/"
        f"{db_config.connection.database}"
    )


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.
    """
    url = get_database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        version_table_schema="alembic",
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.
    """
    # Override the sqlalchemy.url with our configuration
    configuration = config.get_section(config.config_ini_section) or {}
    configuration['sqlalchemy.url'] = get_database_url()
    
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        # Create alembic schema if it doesn't exist
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS alembic"))
        connection.commit()
        
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            # Enable TimescaleDB-specific features
            compare_type=True,
            compare_server_default=True,
            include_schemas=True,
            version_table_schema="alembic",
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
