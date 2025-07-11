# Database Migrations with Alembic

This directory contains database migrations for the TBP Data Pipeline project using Alembic.

## Setup

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure database connection**:
   The connection is automatically loaded from your environment variables via the ConfigLoader.
   Make sure you have your `.env` file configured with:
   ```
   DB_HOST=your_host
   DB_PORT=5432
   DB_NAME=trading
   DB_USER=your_user
   DB_PASSWORD=your_password
   ```

## Usage

### Check current migration status
```bash
alembic current
```

### Apply all migrations
```bash
alembic upgrade head
```

### Apply specific migration
```bash
alembic upgrade 003
```

### Rollback one migration
```bash
alembic downgrade -1
```

### Create a new migration
```bash
# Auto-generate (requires SQLAlchemy models)
alembic revision --autogenerate -m "Description of changes"

# Manual migration
alembic revision -m "Description of changes"
```

## Migration History

### 001_initial
- Creates basic schema and market_data_raw table
- Sets up TimescaleDB hypertable

### 002_future_tables
- Placeholder for future tables

### 003_comprehensive_schema
- Adds data_source column to market_data_raw
- Creates failed_downloads table for retry tracking
- Creates data_quality_metrics table
- Creates trades_raw table (future use)
- Creates quotes_raw table (future use)
- Creates alternative_bars table for López de Prado methods
- Adds comprehensive indexes

## Best Practices

1. **Always test migrations** in development before production
2. **Backup database** before running migrations in production
3. **Review generated SQL** with `alembic upgrade --sql`
4. **Use transactions** - Alembic wraps migrations in transactions by default
5. **Document changes** in migration files

## TimescaleDB Specific

- Use `create_hypertable()` for time-series tables
- Consider compression policies for older data
- Plan for continuous aggregates in future migrations
- Use appropriate chunk_time_interval based on data volume

## Troubleshooting

1. **Connection errors**: Check your .env configuration
2. **Permission errors**: Ensure user has CREATE/ALTER permissions
3. **TimescaleDB errors**: Ensure extension is installed
4. **Migration conflicts**: Check alembic_version table

## Future Migrations Plan

- Add compression policies for data > 30 days
- Create continuous aggregates for common queries
- Add partitioning for alternative_bars by bar_type
- Implement data retention policies
