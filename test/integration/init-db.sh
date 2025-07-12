#!/bin/bash
set -e

echo "🔧 Initializing test database..."

# Enable TimescaleDB extension
echo "📊 Enabling TimescaleDB extension..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
    
    -- Verify TimescaleDB is working
    SELECT timescaledb_version();
EOSQL

# Check if we should run migrations (only if alembic directory exists)
if [ -d "/docker-entrypoint-initdb.d/alembic" ]; then
    echo "🚀 Running Alembic migrations..."
    
    # Install required Python packages
    echo "📦 Installing required packages..."
    pip install alembic psycopg2-binary pyyaml
    
    # Create a temporary alembic.ini for test environment
    cat > /tmp/alembic.ini <<EOF
[alembic]
script_location = /docker-entrypoint-initdb.d/alembic
sqlalchemy.url = postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:5432/${POSTGRES_DB}
prepend_sys_path = /docker-entrypoint-initdb.d/scripts

[loggers]
keys = root,sqlalchemy,alembic

[handlers]
keys = console

[formatters]
keys = generic

[logger_root]
level = WARN
handlers = console
qualname =

[logger_sqlalchemy]
level = WARN
handlers =
qualname = sqlalchemy.engine

[logger_alembic]
level = INFO
handlers =
qualname = alembic

[handler_console]
class = StreamHandler
args = (sys.stderr,)
level = NOTSET
formatter = generic

[formatter_generic]
format = %(levelname)-5.5s [%(name)s] %(message)s
datefmt = %H:%M:%S
EOF

    # Set Python path for imports
    export PYTHONPATH=/docker-entrypoint-initdb.d/scripts:$PYTHONPATH

    # Run migrations
    echo "🔄 Applying database migrations..."
    python -m alembic -c /tmp/alembic.ini upgrade head
else
    echo "⚠️  No Alembic migrations found, skipping..."
fi

echo "✅ Database initialization complete!"