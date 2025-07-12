# Unit Testing Rules for Claude Code

## 🎯 Golden Rule
**ALWAYS mock external dependencies - NO exceptions!**

## ✅ What to Test Here
- Business logic and algorithms
- Data transformations
- Error handling
- Configuration validation
- Utility functions

## 🚫 What NOT to Test Here
- Database queries (→ integration tests)
- S3 downloads (→ integration tests)
- File I/O (→ integration tests)
- External APIs (→ integration tests)

## 🔧 Mocking Cheat Sheet

### Database Mocks:
```python
# Mock connection and cursor
mock_conn = Mock()
mock_cursor = Mock()
mock_conn.cursor.return_value = mock_cursor
mock_cursor.fetchall.return_value = [(row,data)]
```

### S3 Mocks:
```python
# Use create_mock_s3_config() helper
@patch('boto3.client')
def test_s3_operation(mock_boto3):
    mock_s3 = MagicMock()
    mock_boto3.return_value = mock_s3
```

### File Operations:
```python
@patch('builtins.open', new_callable=mock_open, read_data='content')
@patch('os.path.exists', return_value=True)
```

## 📝 Test Structure

```python
@pytest.mark.unit  # REQUIRED!
class TestYourFeature:
    """Test [feature] in isolation."""
    
    @pytest.fixture
    def setup(self):
        """Common setup for tests."""
        return create_test_objects()
    
    def test_success_case(self, setup, mock_dep):
        """Test normal operation."""
        # Single assertion per test
        
    def test_edge_case(self, setup):
        """Test boundary conditions."""
        # Empty, None, max values
        
    def test_error_case(self, setup):
        """Test error handling."""
        # Exceptions, invalid input
```

## ⚡ Quick Patterns

### Testing Exceptions:
```python
with pytest.raises(ValueError, match="Invalid ticker"):
    process_invalid_ticker(None)
```

### Testing Logging:
```python
def test_logs_warning(caplog):
    with caplog.at_level(logging.WARNING):
        function()
    assert "Expected warning" in caplog.text
```

### Parametrized Tests:
```python
@pytest.mark.parametrize("input,expected", [
    ("AAPL", True),
    ("", False),
    (None, False),
])
def test_validation(input, expected):
    assert validate(input) == expected
```

## 🏃 Fast Feedback Loop

```bash
# Run single test
pytest test/unit/test_file.py::test_function -v

# Run tests matching pattern
pytest -m unit -k "insert" 

# Run last failed
pytest --lf

# With print statements
pytest -m unit -s
```

## ⚠️ Unit Test Checklist

Before marking test complete:
- [ ] Marked with `@pytest.mark.unit`
- [ ] All external deps mocked
- [ ] Runs in < 100ms
- [ ] Tests one thing only
- [ ] Clear test name
- [ ] No hardcoded dates/times
- [ ] No real file/network I/O

## 🎨 Good vs Bad Examples

### ❌ BAD - Real database call:
```python
def test_insert():
    conn = psycopg2.connect(...)  # NO!
    result = insert_data(conn, data)
```

### ✅ GOOD - Mocked database:
```python
def test_insert(mock_conn):
    mock_conn.cursor().rowcount = 5
    result = insert_data(mock_conn, data)
    assert result['inserted'] == 5
```

### ❌ BAD - Testing implementation:
```python
def test_uses_copy_from():
    # Don't test HOW it works
    mock.assert_called_with('COPY FROM')
```

### ✅ GOOD - Testing behavior:
```python
def test_inserts_all_rows():
    # Test WHAT it does
    assert result['successful'] == len(data)
```

Remember: Unit tests = FAST + ISOLATED + RELIABLE!