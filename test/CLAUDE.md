# Testing Guidelines for Claude Code

## 🎯 Testing Philosophy
**Unit Tests**: Fast, isolated, with mocks - for business logic
**Integration Tests**: Real services, end-to-end - for workflows

## 🔄 Recent Updates (January 2025)
- ✅ **Connection pooling implemented**: Real `ConnectionPool` and `ConnectionManager` classes
- ✅ **Integration tests updated**: Now use real connection pools, no more mocking ConnectionManager
- ✅ **Docker automation**: Tests automatically start/stop containers via fixtures

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

## 🚫 CRITICAL RULE: NEVER CHANGE IMPLEMENTATION TO MAKE TESTS PASS

**FUNDAMENTAL PRINCIPLE**: Tests validate correctness, not define behavior!

### ✅ CORRECT Approach - Fix Real Implementation Bugs:
- NULL handling causing crashes → Fix implementation
- Missing error handling → Add proper error handling  
- Performance issues → Optimize implementation
- Security vulnerabilities → Fix implementation
- Logic errors → Correct the logic

### ❌ FORBIDDEN - Changing Implementation for Test Convenience:
- Test expects List[Dict] but implementation returns List[str] → Fix test expectation
- Test passes wrong parameters → Fix test parameters
- Test has wrong assertions → Fix test assertions
- Test expects different return type → Fix test expectations

### Decision Framework:
**When a test fails, ask these questions IN ORDER:**

1. **Is the test scenario legitimate?**
   - Does the test represent a real-world use case?
   - Should the application handle this scenario gracefully?
   - Is this a valid requirement for robust software?

2. **If test scenario is legitimate:**
   - Is there a real bug in the implementation? → Fix implementation
   - Is the implementation missing required functionality? → Add functionality

3. **If test scenario is illegitimate:**
   - Is the test expectation wrong? → Fix test
   - Is the test using wrong parameters? → Fix test
   - Is the test testing internal details? → Fix test

**Never ask**: "How can I change the code to make this test pass?"

### Example Analysis:
```
Test fails: "ConfigLoader crashes when config directory doesn't exist"

1. Is this scenario legitimate?
   ❌ NO - In production, configuration MUST exist. Fail fast is better.
   
2. Is this a test of an impossible scenario?
   ✅ YES - Production deployment always includes configuration
   
3. Action: Remove test - this scenario should never occur
```

```
Test fails: "Method expects List[Dict] but implementation returns List[date]"

1. Is this scenario legitimate?
   ❌ NO - Test expects wrong return type
   
2. Action: Fix test expectation to match correct implementation
```

### Examples:

```python
# ❌ BAD - Changing implementation for test
def get_data_gaps() -> List[str]:  # Test expects this
    return ["2024-01-01"]  # Wrong! Dates should be date objects

# ✅ GOOD - Fix test expectation
def get_data_gaps() -> List[date]:  # Correct return type
    return [date(2024, 1, 1)]

# In test: assert gaps[0] == date(2024, 1, 1)  # Fix test
```

**Remember**: Implementation defines business logic. Tests verify it works correctly.

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