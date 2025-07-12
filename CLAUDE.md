# TBP Data Pipeline - Context for Claude Code

## 🎯 Project Overview
Part of TBP (Trading Bot Project) - Building an automated trading system with ML strategies.
This module handles data ingestion from Polygon.io S3 → TimescaleDB.

**Current Status**: MVP Phase (~65% complete)
**Scope**: 12 tickers (10 stocks + 2 ETFs), daily timeframe only

## 🏗️ Architecture Decisions
- **Data Source**: S3 Flat Files first, API for gaps only
- **Database**: TimescaleDB with single hypertable per data type
- **Pattern**: Configuration-driven, modular clients, connection pooling
- **Philosophy**: MVP first, auto-resilient, "better no data than bad data"

## 📋 Key Components
```
src/
├── config_loader.py    # YAML config with template substitution
├── s3_client.py        # Polygon S3 downloads with retry logic
├── database/
│   ├── connection.py   # Connection pool management
│   ├── market_data.py  # MarketDataClient for TimescaleDB
│   └── utils.py        # DB helpers (batch processing, COPY)
└── data_quality/       # Validation framework (in progress)
```

## 🚦 Development Guidelines

### When Making Changes:
1. **Configuration**: All settings in YAML, secrets via env vars
2. **Testing**: Write tests first (unit with mocks, integration with Docker)
3. **Error Handling**: Categorize errors, implement retry logic
4. **Logging**: Structured JSON logs with context
5. **Performance**: Target >1K rows/sec inserts, <1GB memory

### Code Standards:
- Python 3.11 with type hints everywhere
- Follow existing patterns (look at s3_client.py as reference)
- Document decisions and complex logic
- Handle weekends/holidays automatically (no trading calendar)

### Database Operations:
- Use COPY for bulk inserts (via utils.py helpers)
- ON CONFLICT → UPDATE (last value wins)
- Process data by day, not entire history
- Always use connection pool, never create direct connections

## 🧪 Testing Strategy
```bash
# Unit tests (fast, mocked) - RUN THESE OFTEN
pytest -m unit

# Integration tests (Docker required) - RUN BEFORE COMMITS
pytest -m integration

# Full test suite with coverage
pytest --cov=src --cov-report=term-missing
```

## 🎬 Common Tasks

### Add New Data Source:
1. Create client class following s3_client.py pattern
2. Add configuration in config/*.yml
3. Write unit tests with mocks
4. Write integration tests with real services
5. Update main.py orchestration

### Debug Database Issues:
```bash
# Connect to test DB
psql -h localhost -p 5433 -U test_user -d test_db

# View logs
docker logs tbp-data-pipeline-postgres-test-1
```

### Performance Testing:
- Use fixtures in test/integration/conftest.py
- Generate large datasets with sample_market_data fixture
- Monitor with: `result['rows_per_second']`

## ⚠️ Important Rules

1. **NEVER** commit without running tests
2. **NEVER** hardcode credentials or secrets
3. **NEVER** create database connections outside connection pool
4. **ALWAYS** handle missing data gracefully (weekends, holidays)
5. **ALWAYS** validate data before inserting to database

## 📊 Success Metrics
- Data Completeness: >95% coverage
- Daily Processing: <15 minutes for all tickers
- Test Coverage: >80%
- Insert Performance: >1K rows/second

## 🔗 Related Documentation
- Project docs: `/mnt/d/obsidian-vaults/patofp/tbp-docs/`
- Architecture decisions: `docs/architecture/`
- API references: `docs/api/`

Remember: This is financial data - accuracy and reliability are paramount!