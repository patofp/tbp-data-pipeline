# Unit Tests Guide

This directory contains unit tests that are fast, isolated, and use mocks for all external dependencies.

## 🎯 Purpose of Unit Tests

Unit tests verify that individual components work correctly in isolation. They:
- Test business logic without external dependencies
- Provide fast feedback during development
- Catch regressions early
- Document expected behavior
- Enable safe refactoring

## 📋 When to Write Unit Tests

Write unit tests for:
- **Business logic** - Data transformations, calculations, validations
- **Error handling** - Exception cases, edge conditions
- **Class methods** - Individual method behavior
- **Utility functions** - Helper functions, formatters, parsers
- **Configuration** - Settings validation, default values

## 🚫 When NOT to Write Unit Tests

Don't write unit tests for:
- Database queries (use integration tests)
- External API calls (use integration tests)
- File I/O operations (use integration tests)
- Simple getters/setters
- Third-party library behavior

## 🔧 Mocking Strategy

### What to Mock
```python
# Mock external services
@patch('boto3.client')  # S3 client
@patch('psycopg2.connect')  # Database connection
@patch('requests.get')  # HTTP requests

# Mock file operations
@patch('builtins.open', new_callable=mock_open)
@patch('os.path.exists')

# Mock datetime for consistent tests
@patch('datetime.datetime.now')
```

### Mock Patterns

1. **Database Mocks**
```python
# Mock connection and cursor
mock_conn = Mock()
mock_cursor = Mock()
mock_conn.cursor.return_value = mock_cursor
mock_cursor.fetchall.return_value = [(1, 'AAPL', 100.0)]
```

2. **S3 Mocks**
```python
# Mock S3 client with proper config
mock_config = create_mock_s3_config()  # Helper function
mock_s3 = MagicMock()
mock_s3.get_object.return_value = {'Body': data}
```

3. **Configuration Mocks**
```python
# Mock complex configuration objects
mock_config = Mock()
mock_config.database.host = "localhost"
mock_config.database.port = 5432
```

## 📁 Directory Structure

```
test/unit/
├── conftest.py              # Shared fixtures for unit tests
├── README.md                # This file
├── database/               # Database-related unit tests
│   ├── __init__.py
│   └── test_market_data.py  # Tests for MarketDataClient
├── test_config_loader.py    # Tests for configuration
└── test_s3_client.py        # Tests for S3 operations
```

## 🔨 Writing Unit Tests

### Test Structure
```python
@pytest.mark.unit
class TestFeatureName:
    """Test [feature] functionality."""
    
    def test_success_case(self, mock_dependency):
        """Test successful [operation]."""
        # Arrange
        mock_dependency.return_value = expected_data
        
        # Act
        result = function_under_test()
        
        # Assert
        assert result == expected_result
        mock_dependency.assert_called_once_with(expected_args)
    
    def test_error_case(self, mock_dependency):
        """Test handling of [error condition]."""
        # Arrange
        mock_dependency.side_effect = Exception("Error")
        
        # Act & Assert
        with pytest.raises(Exception):
            function_under_test()
```

### Common Fixtures (from conftest.py)

```python
# Mock database configuration
@pytest.fixture
def mock_db_config():
    """Mock database configuration."""
    config = Mock()
    config.connection.host = "localhost"
    config.connection.port = 5432
    return config

# Mock connection pool
@pytest.fixture
def mock_pool():
    """Mock connection pool."""
    return Mock()

# Sample data
@pytest.fixture
def sample_dataframe():
    """Sample market data DataFrame."""
    return pd.DataFrame({
        'ticker': ['AAPL', 'GOOGL'],
        'timestamp': pd.to_datetime(['2024-01-02', '2024-01-02']),
        'open': [180.0, 140.0],
        # ... more columns
    })
```

## ✅ Best Practices

1. **Test One Thing**
   - Each test method should verify one specific behavior
   - Use descriptive test names that explain what is being tested

2. **Use Fixtures**
   - Share common setup using pytest fixtures
   - Keep fixtures simple and focused

3. **Mock at Boundaries**
   - Mock external dependencies at the boundary of your code
   - Don't mock internal implementation details

4. **Test Edge Cases**
   - Empty collections
   - None/null values
   - Boundary values
   - Invalid inputs

5. **Assertion Messages**
   ```python
   assert len(result) == 5, f"Expected 5 items, got {len(result)}"
   ```

## 🏃 Running Unit Tests

```bash
# Run all unit tests
pytest -m unit

# Run specific test file
pytest test/unit/test_s3_client.py

# Run specific test class
pytest test/unit/test_s3_client.py::TestS3ClientInitialization

# Run tests matching pattern
pytest -m unit -k "test_download"

# Run with coverage
pytest -m unit --cov=src --cov-report=html

# Run in parallel (requires pytest-xdist)
pytest -m unit -n auto

# Show print statements
pytest -m unit -s

# Verbose output
pytest -m unit -v
```

## 📊 Coverage Guidelines

- Aim for 80%+ coverage of business logic
- 90%+ coverage for critical paths
- Don't test:
  - Simple getters/setters
  - Third-party code
  - Boilerplate code

## 🐛 Debugging Tips

1. **Use pytest.set_trace()**
   ```python
   def test_complex_logic():
       result = function()
       import pytest; pytest.set_trace()  # Debugger breakpoint
       assert result == expected
   ```

2. **Print with -s flag**
   ```python
   def test_with_print():
       print(f"Debug: {variable}")  # Visible with pytest -s
   ```

3. **Check mock calls**
   ```python
   print(mock.call_args_list)  # See all calls to mock
   mock.assert_called_with(expected_args)
   ```

## 🚀 Example: Complete Unit Test

```python
@pytest.mark.unit
class TestMarketDataClient:
    """Test MarketDataClient business logic."""
    
    @pytest.fixture
    def client(self, mock_db_config, mock_pool):
        """Create client with mocked dependencies."""
        return MarketDataClient(mock_db_config, mock_pool)
    
    def test_insert_batch_success(self, client, sample_dataframe):
        """Test successful batch insertion."""
        # Mock internal method
        client._insert_batch_with_retry = Mock(
            return_value={'successful': 5, 'failed': []}
        )
        
        # Execute
        result = client.insert_batch(
            sample_dataframe,
            timeframe='1d',
            data_source='test'
        )
        
        # Verify
        assert result['total_rows'] == 5
        assert result['successful'] == 5
        assert result['failed'] == 0
        assert result['duration_seconds'] > 0
        
        # Verify mock was called correctly
        client._insert_batch_with_retry.assert_called_once()
```

## 🎨 Test Patterns

### Testing Exceptions
```python
def test_invalid_input_raises_error(self):
    with pytest.raises(ValueError, match="Invalid ticker"):
        process_ticker(None)
```

### Testing Logging
```python
def test_logs_warning(self, caplog):
    with caplog.at_level(logging.WARNING):
        function_that_logs()
    assert "Expected warning" in caplog.text
```

### Parametrized Tests
```python
@pytest.mark.parametrize("input,expected", [
    ("AAPL", True),
    ("INVALID", False),
    ("", False),
    (None, False),
])
def test_validate_ticker(self, input, expected):
    assert validate_ticker(input) == expected
```

Remember: Unit tests should be FAST, ISOLATED, and REPEATABLE!