# Integration Testing Rules for Claude Code

## 🎯 Golden Rule
**NEVER mock services - Use real Docker containers!**

## ✅ What to Test Here
- Database operations (INSERT, UPDATE, SELECT)
- S3 file uploads/downloads with LocalStack
- End-to-end data pipelines
- Configuration with real files
- Performance under load
- Database constraints and transactions

## 🐳 Available Services

### PostgreSQL + TimescaleDB (port 5433):
```python
# Automatically provided by fixtures
def test_database(test_market_data_client):
    # Real database operations
    result = test_market_data_client.insert_batch(data)
```

### LocalStack S3 (port 4566):
```python
# Real S3 operations with LocalStack
def test_s3(test_s3_client):
    df = test_s3_client.download_daily_data(...)
```

## 🧹 Database Cleanup Strategy

**Between Tests**: Automatic TRUNCATE
```python
# This happens automatically via fixture
TRUNCATE TABLE trading.market_data_raw RESTART IDENTITY CASCADE
```

**Why TRUNCATE?**
- ✅ Fast cleanup
- ✅ Resets sequences
- ✅ Preserves schema
- ❌ NOT transactions (can't test commits)

## 📝 Test Structure

```python
@pytest.mark.integration  # REQUIRED!
class TestYourFeatureIntegration:
    """Test [feature] with real services."""
    
    def test_database_workflow(self, test_market_data_client):
        """Test complete database workflow."""
        # 1. Insert data
        result = test_market_data_client.insert_batch(data)
        
        # 2. Verify in database
        stats = test_market_data_client.get_ticker_stats('AAPL')
        assert stats['record_count'] == len(data)
        
        # 3. Test constraints
        # Re-insert should handle duplicates
        result2 = test_market_data_client.insert_batch(data)
        assert result2['failed'] > 0
```

## ⚡ Common Patterns

### End-to-End Pipeline:
```python
def test_s3_to_database(test_s3_client, test_market_data_client):
    # Download from S3
    df = test_s3_client.download_daily_data(...)
    assert df is not None
    
    # Insert to database
    result = test_market_data_client.insert_batch(df)
    assert result['successful'] == len(df)
    
    # Verify integrity
    gaps = test_market_data_client.get_data_gaps(...)
    assert len(gaps) == 0
```

### Performance Testing:
```python
@pytest.mark.slow
def test_large_batch_performance(test_market_data_client):
    # Generate 10k rows
    large_data = generate_large_dataset(10000)
    
    start = time.time()
    result = test_market_data_client.insert_batch(large_data)
    duration = time.time() - start
    
    assert result['rows_per_second'] > 1000
    assert duration < 30
```

## 🐛 Debugging Integration Tests

### View Docker Logs:
```bash
# While tests running
docker logs tbp-data-pipeline-postgres-test-1 -f
docker logs tbp-data-pipeline-localstack-1 -f
```

### Connect to Test DB:
```bash
psql -h localhost -p 5433 -U test_user -d test_db
# password: test_password

# Useful queries
\dt trading.*  # List tables
SELECT COUNT(*) FROM trading.market_data_raw;
```

### Inspect LocalStack S3:
```bash
aws --endpoint-url=http://localhost:4566 \
    s3 ls s3://flatfiles/ --recursive
```

## ⚠️ Integration Test Checklist

Before marking test complete:
- [ ] Marked with `@pytest.mark.integration`
- [ ] Uses real services (no mocks)
- [ ] Verifies actual state changes
- [ ] Handles cleanup (automatic)
- [ ] Tests error scenarios
- [ ] Reasonable timeout (<30s)

## 🚨 Common Issues & Solutions

### Docker not running:
```
Error: Cannot connect to Docker daemon
Fix: Start Docker Desktop
```

### Port conflicts:
```
Error: bind: address already in use
Fix: docker compose -f test/integration/docker-compose.test.yml down
```

### Slow tests:
- Use session-scoped fixtures
- Batch operations
- Don't test same thing multiple times

### Flaky tests:
- Add proper waits for async operations
- Don't rely on specific timing
- Use deterministic test data

## 🎨 Good vs Bad Examples

### ❌ BAD - Using mocks:
```python
@pytest.mark.integration
def test_insert(mock_connection):  # NO!
    # Don't mock in integration tests
```

### ✅ GOOD - Real services:
```python
@pytest.mark.integration
def test_insert(test_market_data_client):
    # Use real database
    result = test_market_data_client.insert_batch(data)
    # Verify actual database state
```

### ❌ BAD - No verification:
```python
def test_workflow():
    insert_data(data)
    # Where's the verification?
```

### ✅ GOOD - Verify state:
```python
def test_workflow(test_market_data_client):
    result = test_market_data_client.insert_batch(data)
    
    # Verify in database
    cursor = test_market_data_client._get_connection().cursor()
    cursor.execute("SELECT COUNT(*) FROM trading.market_data_raw")
    assert cursor.fetchone()[0] == len(data)
```

Remember: Integration tests = REAL services + REAL data!