#!/bin/bash
set -e

echo "🔧 Initializing test database..."

# Enable TimescaleDB extension
echo "📊 Enabling TimescaleDB extension..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
EOSQL

# For integration tests, we don't need migrations
echo "ℹ️  Skipping migrations for test environment..."

echo "✅ Database initialization complete!"