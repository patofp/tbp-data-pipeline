# Database Setup Guide for TBP Data Pipeline

## Prerequisites

1. PostgreSQL with TimescaleDB extension installed
2. Database `trading` created
3. User with CREATE/ALTER permissions

## Initial Setup

### 1. Create the database (if not exists)

```sql
-- Connect as superuser
CREATE DATABASE trading;

-- Connect to the new database
\c trading

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;
```

### 2. Configure environment

```bash
# Copy example environment file
cp .env.example .env

# Edit with your database credentials
# DB_HOST=192.168.1.11
# DB_PORT=5432
# DB_NAME=trading
# DB_USER=your_user
# DB_PASSWORD=your_password
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run migrations

```bash
# Check current status
python scripts/run_migrations.py status

# View migration history
python scripts/run_migrations.py history

# Dry run (see SQL without executing)
python scripts/run_migrations.py upgrade --dry-run

# Apply all migrations
python scripts/run_migrations.py upgrade

# Or use alembic directly
alembic upgrade head
```

## Schema Overview

After migrations, you'll have:

1. **market_data_raw**: Main OHLC data table (hypertable)
2. **failed_downloads**: Track failed downloads for retry
3. **data_quality_metrics**: Quality metrics per ticker/date
4. **trades_raw**: Trade tick data (future use)
5. **quotes_raw**: Quote data (future use)
6. **alternative_bars**: Dollar bars, volume bars (López de Prado)

## Verification

```sql
-- Check tables
\dt trading.*

-- Check hypertables
SELECT * FROM timescaledb_information.hypertables;

-- Check indexes
\di trading.*

-- Verify schema
\d+ trading.market_data_raw
```

## Troubleshooting

### Connection Issues
- Verify `.env` file has correct credentials
- Check PostgreSQL is running
- Ensure network connectivity to database

### Permission Issues
```sql
-- Grant necessary permissions
GRANT CREATE ON SCHEMA trading TO your_user;
GRANT ALL ON ALL TABLES IN SCHEMA trading TO your_user;
```

### TimescaleDB Issues
```sql
-- Check if extension is available
SELECT * FROM pg_available_extensions WHERE name = 'timescaledb';

-- If not, install TimescaleDB first
```

## Next Steps

1. Run test insert to verify setup
2. Configure connection pooling in database.yml
3. Set up monitoring for table sizes
4. Plan compression policies for old data
