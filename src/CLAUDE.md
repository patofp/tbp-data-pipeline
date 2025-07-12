# Source Code Guidelines for Claude Code

## 📁 Module Structure
```
src/
├── config_loader.py    # ✅ Complete - YAML config management
├── s3_client.py        # ✅ Complete - Polygon S3 downloads
├── database/           # 🚧 In Progress - TimescaleDB integration
├── data_quality/       # 📋 TODO - Validation framework
└── main.py            # 📋 TODO - Orchestration logic
```

## 🎯 Implementation Status

### ✅ Completed:
- **ConfigLoader**: Loads YAML with ${ENV_VAR} substitution
- **S3Client**: Downloads from Polygon.io with retry logic
- **Database migrations**: Alembic setup with TimescaleDB

### 🚧 In Progress:
- **MarketDataClient**: Bulk inserts, gap detection, stats
- **ConnectionManager**: Pool management (temporary mocks OK)

### 📋 TODO:
- Data quality validation (row & series level)
- Main orchestration with scheduling
- Monitoring and alerting

## 💡 Code Patterns to Follow

### Configuration Management:
```python
# Always use ConfigLoader for settings
config = ConfigLoader()
s3_config = config.get_s3_config()  # Returns typed config object
```

### Error Handling:
```python
# Categorize errors for proper retry logic
try:
    result = operation()
except RecoverableError:
    retry_with_backoff()
except DataQualityError:
    log_and_skip()
except FatalError:
    alert_and_stop()
```

### Database Operations:
```python
# Always use connection from pool
with self._get_connection() as conn:
    # Use COPY for bulk inserts
    result = utils.copy_from_dataframe(conn, df, table_name)
```

### Data Processing:
```python
# Process day by day, not entire history
for date in date_range:
    df = s3_client.download_daily_data(ticker, date)
    if df is not None:
        validated_df = validate_data(df)
        db_client.insert_batch(validated_df)
```

## 🏗️ Architecture Principles

1. **Modular Clients**: Each external service has its own client
2. **Typed Configs**: Use dataclasses for configuration objects
3. **Connection Pooling**: Share pools, never create connections
4. **Batch Processing**: Optimize for bulk operations
5. **Graceful Degradation**: Continue on partial failures

## 🧪 Testing Requirements

### For New Code:
1. Write unit tests with mocks in `test/unit/`
2. Write integration tests in `test/integration/`
3. Aim for >80% coverage
4. Test error conditions and edge cases

### Test Patterns:
```python
# Unit test - mock everything external
@patch('boto3.client')
def test_download(mock_boto3):
    mock_boto3.return_value = mock_s3
    # Test business logic only

# Integration test - use real services
@pytest.mark.integration
def test_real_download(test_s3_client):
    # Test with LocalStack
    df = test_s3_client.download_daily_data(...)
    assert df is not None
```

## 🚀 Performance Guidelines

### Database Inserts:
- Target: >1000 rows/second
- Use COPY, not INSERT
- Batch size: 5000-10000 rows
- Monitor: `result['rows_per_second']`

### Memory Management:
- Process data in chunks
- Use generators for large datasets
- Target: <1GB memory usage
- Profile with: `memory_profiler`

### S3 Downloads:
- Concurrent downloads for multiple dates
- Retry with exponential backoff
- Cache for development (optional)

## ⚠️ Common Pitfalls

1. **DON'T** create connections outside pool
2. **DON'T** load entire history into memory
3. **DON'T** assume dates are trading days
4. **DON'T** ignore data validation
5. **DON'T** use print() - use logging

## 📝 Coding Standards

### Imports:
```python
# Standard library
import os
from datetime import datetime

# Third party
import pandas as pd
import psycopg2

# Local
from src.config_loader import ConfigLoader
```

### Type Hints:
```python
def process_data(
    ticker: str,
    date: date,
    timeframe: str = "1d"
) -> Optional[pd.DataFrame]:
```

### Logging:
```python
logger = logging.getLogger(__name__)
logger.info("Processing ticker", extra={
    "ticker": ticker,
    "date": date.isoformat(),
    "rows": len(df)
})
```

## 🔍 Debugging Tips

1. **Enable debug logging**: `LOG_LEVEL=DEBUG`
2. **Check test database**: Port 5433, not 5432
3. **View S3 contents**: Use AWS CLI with LocalStack
4. **Profile queries**: Use `EXPLAIN ANALYZE`
5. **Memory leaks**: Use `tracemalloc`

Remember: This handles financial data - reliability > speed!