# Testing Guidelines for Claude Code

## 🎯 Testing Philosophy
**Unit Tests**: Fast, isolated, with mocks - for business logic
**Integration Tests**: Real services, end-to-end - for workflows

## 📁 Test Structure
```
test/
├── unit/           # Fast tests with mocks
├── integration/    # Tests with Docker services
├── fixtures/raw/   # Shared test data (CSV files)
└── conftest.py     # Global fixtures
```

## 🚦 Decision Tree for New Tests

```mermaid
Is it testing business logic only?
├─ YES → Write UNIT test
│   ├─ Place in: test/unit/[module]/
│   ├─ Use mocks for ALL external deps
│   └─ Mark with: @pytest.mark.unit
│
└─ NO → Write INTEGRATION test
    ├─ Place in: test/integration/[module]/
    ├─ Use real Docker services
    └─ Mark with: @pytest.mark.integration
```

## 🧪 Running Tests

```bash
# ALWAYS run before commits
pytest -m unit          # Fast feedback (< 30s)
pytest -m integration   # Full validation (2-3 min)

# During development
pytest -k "test_name" -v  # Run specific test
pytest --lf              # Run last failed
pytest -x                # Stop on first failure
```

## 📋 Test Patterns

### Unit Test Template:
```python
@pytest.mark.unit
class TestFeature:
    def test_business_logic(self, mock_dependency):
        # Arrange - Setup mocks
        mock_dependency.return_value = expected
        
        # Act - Execute code
        result = function_under_test()
        
        # Assert - Verify behavior
        assert result == expected
        mock_dependency.assert_called_once()
```

### Integration Test Template:
```python
@pytest.mark.integration
class TestWorkflow:
    def test_end_to_end(self, test_db_client, test_s3_client):
        # Use real services (Docker handles setup)
        data = test_s3_client.download_daily_data(...)
        result = test_db_client.insert_batch(data)
        
        # Verify actual state
        assert verify_in_database(result)
```

## 🔧 Available Fixtures

### Global (test/conftest.py):
- `test_data_dates` - Common test dates
- `test_tickers` - Valid/invalid tickers
- `fixtures_path` - Path to test data

### Unit Tests (test/unit/conftest.py):
- `mock_db_config` - Mock database config
- `mock_pool` - Mock connection pool
- `sample_dataframe` - Test market data

### Integration Tests (test/integration/conftest.py):
- `postgres_container` - Real PostgreSQL
- `localstack_container` - Mock AWS S3
- `test_market_data_client` - Real DB client
- `test_s3_client` - LocalStack S3 client

## ⚡ Quick Decisions

### Mock or Real?
- **Mock**: External APIs, network calls, file I/O
- **Real**: Business logic, data transformations, calculations

### Where to Place?
- `test/unit/database/` → Database logic with mocks
- `test/integration/database/` → Real database operations
- Follow source structure: `src/module/` → `test/*/module/`

### How to Name?
- File: `test_[module_name].py`
- Class: `Test[Feature]`
- Method: `test_[scenario]_[expected_result]`

## 🚨 Common Mistakes to Avoid

1. **Unit test calling real database** → Use mocks!
2. **Integration test with mocks** → Use real services!
3. **Tests depending on order** → Each test independent!
4. **No cleanup between tests** → Use fixtures!
5. **Testing third-party code** → Test your logic only!

## 📊 Coverage Guidelines

Target Coverage:
- Business logic: >90%
- Database operations: >80%
- Error handling: 100%
- Configuration: >70%

Check coverage:
```bash
pytest --cov=src --cov-report=html
# Open htmlcov/index.html
```

## 🐛 Debugging Failed Tests

1. **See print outputs**: `pytest -s`
2. **More details**: `pytest -vv`
3. **Drop to debugger**: `pytest --pdb`
4. **Check Docker logs**: `docker logs [container]`
5. **Inspect test DB**: `psql -h localhost -p 5433`

## ✅ Before Committing

1. Run unit tests: `pytest -m unit`
2. Run integration tests: `pytest -m integration`
3. Check coverage: `pytest --cov=src`
4. No skipped tests without reason
5. All new code has tests

Remember: Tests are documentation - make them clear!