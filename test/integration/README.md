# Integration Tests Guide

This directory contains integration tests that use real services (Docker containers) to verify system behavior end-to-end.

## 🎯 Purpose of Integration Tests

Integration tests verify that components work correctly when integrated with real services. They:
- Test actual database operations and constraints
- Verify S3 file operations with LocalStack
- Catch integration issues that unit tests miss
- Validate configuration and environment setup
- Test performance under realistic conditions

## 📋 When to Write Integration Tests

Write integration tests for:
- **Database operations** - CRUD, transactions, constraints, migrations
- **External APIs** - Real HTTP calls to test services
- **File operations** - S3 uploads/downloads, file processing
- **End-to-end workflows** - Complete data pipelines
- **Configuration** - Environment-specific settings
- **Performance** - Load testing, concurrent operations

## 🚫 When NOT to Write Integration Tests

Don't write integration tests for:
- Pure business logic (use unit tests)
- Simple data transformations (use unit tests)
- Third-party service behavior (mock in unit tests)
- Tests requiring production credentials
- Tests that take more than 30 seconds

## 🐳 Docker Services

### Available Services

Integration tests use Docker containers defined in `docker-compose.test.yml`:

1. **PostgreSQL + TimescaleDB** (port 5433)
   - Database: `test_db`
   - User: `test_user`
   - Password: `test_password`
   - Auto-creates schema and tables

2. **LocalStack** (port 4566)
   - S3 service mock
   - Bucket: `flatfiles`
   - No authentication required

### Service Lifecycle

```
pytest starts → Fixtures start Docker → Run tests → Clean between tests → Stop Docker
```

Services are:
- ✅ Started automatically before first test
- ✅ Health-checked before use
- ✅ Stopped automatically after all tests
- ✅ Isolated per test session

## 🗄️ Database State Management

### Clean State Strategy

Each test starts with a clean database using TRUNCATE:

```python
@pytest.fixture(autouse=True)
def clean_db_per_test(test_db_connection, setup_test_schema):
    """Auto-clean database before each test to ensure isolation."""
    yield  # Run the test
    
    # Clean after test
    cursor = test_db_connection.cursor()
    try:
        cursor.execute("TRUNCATE TABLE trading.market_data_raw RESTART IDENTITY CASCADE")
        test_db_connection.commit()
    except Exception:
        test_db_connection.rollback()
    finally:
        cursor.close()
```

### Why TRUNCATE instead of transactions?

- ✅ **TRUNCATE** - Fast, resets sequences, works with all operations
- ❌ **Transactions** - Can't test COMMIT/ROLLBACK, issues with concurrent tests
- ❌ **DROP/CREATE** - Slow, loses indexes and constraints

## 📁 Directory Structure

```
test/integration/
├── conftest.py              # Docker fixtures, database setup
├── README.md                # This file
├── docker-compose.test.yml  # Service definitions
├── init-db.sh              # Database initialization script
├── database/               # Database integration tests
│   ├── __init__.py
│   └── test_market_data.py
├── test_config_loader.py    # Config integration tests
└── test_s3_client.py        # S3 integration tests
```

## 🔨 Writing Integration Tests

### Test Structure

```python
@pytest.mark.integration
class TestFeatureIntegration:
    """Test [feature] with real services."""
    
    def test_database_operation(self, test_market_data_client, sample_data):
        """Test real database operations."""
        # Act - Use real database
        result = test_market_data_client.insert_batch(
            sample_data,
            timeframe='1d',
            data_source='integration_test'
        )
        
        # Assert - Verify actual database state
        assert result['successful'] == len(sample_data)
        
        # Verify data was persisted
        cursor = test_market_data_client._get_connection().cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM trading.market_data_raw WHERE data_source = %s",
            ('integration_test',)
        )
        count = cursor.fetchone()[0]
        assert count == len(sample_data)
    
    def test_s3_operation(self, test_s3_client, test_data_dates):
        """Test real S3 operations with LocalStack."""
        # Download from LocalStack S3
        df = test_s3_client.download_daily_data(
            'AAPL',
            test_data_dates['trading_date'],
            DataType.DAY_AGGS
        )
        
        # Verify actual download
        assert df is not None
        assert not df.empty
        assert all(df['ticker'] == 'AAPL')
```

### Common Fixtures (from conftest.py)

```python
# Docker services
@pytest.fixture(scope="session")
def postgres_container():
    """Start PostgreSQL container for tests."""
    # Automatically starts/stops PostgreSQL

@pytest.fixture(scope="session")
def localstack_container():
    """Start LocalStack for S3 testing."""
    # Automatically starts/stops LocalStack

# Database fixtures
@pytest.fixture
def test_market_data_client(test_db_config, test_db_connection):
    """MarketDataClient with real database."""
    # Returns client connected to test database

# S3 fixtures
@pytest.fixture
def test_s3_client(test_s3_config):
    """S3Client configured for LocalStack."""
    # Returns client connected to LocalStack
```

## ✅ Best Practices

1. **Test Complete Workflows**
   ```python
   def test_end_to_end_pipeline(self):
       # Download from S3
       data = s3_client.download_daily_data(...)
       
       # Process data
       processed = transform_data(data)
       
       # Insert to database
       result = db_client.insert_batch(processed)
       
       # Verify final state
       assert verify_data_integrity()
   ```

2. **Use Realistic Test Data**
   - Real market data structure
   - Edge cases from production
   - Performance test datasets

3. **Test Error Scenarios**
   - Database constraint violations
   - Network timeouts
   - Invalid data handling
   - Concurrent operations

4. **Verify Side Effects**
   - Check database state
   - Verify files created
   - Confirm logs written

5. **Keep Tests Independent**
   - Don't depend on test order
   - Clean state between tests
   - Use unique identifiers

## 🏃 Running Integration Tests

```bash
# Run all integration tests
pytest -m integration

# Run specific test file
pytest test/integration/database/test_market_data.py

# Run with Docker cleanup
pytest -m integration --docker-cleanup

# Show Docker logs
pytest -m integration -s

# Run specific test
pytest -m integration -k "test_insert_batch"

# Run with coverage
pytest -m integration --cov=src

# Verbose output
pytest -m integration -v
```

## 🐛 Debugging Integration Tests

### View Docker Logs
```bash
# During test execution
docker logs tbp-data-pipeline-postgres-test-1
docker logs tbp-data-pipeline-localstack-1

# Or use pytest -s to see print statements
```

### Connect to Test Database
```bash
psql -h localhost -p 5433 -U test_user -d test_db
# Password: test_password
```

### Inspect LocalStack S3
```bash
aws --endpoint-url=http://localhost:4566 s3 ls s3://flatfiles/
```

### Common Issues

1. **Docker not running**
   ```
   Error: Cannot connect to Docker daemon
   Solution: Start Docker Desktop
   ```

2. **Port conflicts**
   ```
   Error: Port 5433 already in use
   Solution: Stop conflicting service or change port
   ```

3. **Slow tests**
   - Use session-scoped fixtures for Docker
   - Reuse database connections
   - Batch operations when possible

## 🚀 Example: Complete Integration Test

```python
@pytest.mark.integration
class TestMarketDataPipeline:
    """Test complete market data pipeline."""
    
    def test_s3_to_database_pipeline(
        self, 
        test_s3_client, 
        test_market_data_client,
        test_data_dates
    ):
        """Test downloading from S3 and inserting to database."""
        # Step 1: Download from S3 (LocalStack)
        df = test_s3_client.download_daily_data(
            'AAPL',
            test_data_dates['trading_date'],
            DataType.DAY_AGGS
        )
        
        assert df is not None, "Should download data from S3"
        
        # Step 2: Insert to database
        result = test_market_data_client.insert_batch(
            df,
            timeframe='1d',
            data_source='s3_import'
        )
        
        assert result['successful'] == len(df)
        assert result['failed'] == 0
        
        # Step 3: Verify data integrity
        stats = test_market_data_client.get_ticker_stats(
            'AAPL',
            timeframe='1d',
            data_source='s3_import'
        )
        
        assert stats['record_count'] == len(df)
        assert stats['first_date'] <= test_data_dates['trading_date']
        assert stats['last_date'] >= test_data_dates['trading_date']
        
        # Step 4: Test data gaps
        gaps = test_market_data_client.get_data_gaps(
            'AAPL',
            timeframe='1d',
            data_source='s3_import',
            start_date=test_data_dates['trading_date'],
            end_date=test_data_dates['trading_date']
        )
        
        assert len(gaps) == 0, "Should have no gaps for imported data"
```

## 📊 Performance Testing

```python
@pytest.mark.integration
@pytest.mark.slow
def test_large_batch_performance(test_market_data_client):
    """Test performance with large datasets."""
    # Generate 10,000 rows
    large_data = generate_large_dataset(10000)
    
    start_time = time.time()
    result = test_market_data_client.insert_batch(large_data)
    duration = time.time() - start_time
    
    assert result['successful'] == 10000
    assert duration < 30, f"Insert took {duration}s, should be < 30s"
    assert result['rows_per_second'] > 300
```

Remember: Integration tests should be REALISTIC, COMPREHENSIVE, and RELIABLE!