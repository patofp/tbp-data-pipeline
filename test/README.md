# TBP Data Pipeline - Test Suite

This directory contains all tests for the TBP Data Pipeline project. Tests are organized into two main categories: unit tests and integration tests.

## 📁 Directory Structure

```
test/
├── conftest.py          # Global fixtures shared between unit and integration tests
├── fixtures/            # Shared test data files
│   └── raw/            # Raw CSV data files for S3 testing
├── unit/               # Unit tests (fast, isolated, with mocks)
│   ├── conftest.py     # Unit test specific fixtures
│   ├── README.md       # Unit test guidelines
│   └── database/       # Database unit tests
│       └── test_market_data.py
└── integration/        # Integration tests (slower, real services)
    ├── conftest.py     # Integration test fixtures (Docker, LocalStack)
    ├── README.md       # Integration test guidelines
    ├── docker-compose.test.yml
    └── database/       # Database integration tests
        └── test_market_data.py
```

## 🎯 Test Philosophy

### When to Write Unit Tests vs Integration Tests

**Unit Tests** (`test/unit/`):
- Testing business logic in isolation
- Testing data transformations and calculations
- Testing error handling and edge cases
- Testing individual methods and functions
- When you need fast feedback during development

**Integration Tests** (`test/integration/`):
- Testing actual database operations
- Testing S3 file uploads/downloads
- Testing end-to-end workflows
- Testing configuration loading from real files
- Verifying system behavior with real dependencies

## 🔧 Running Tests

```bash
# Run all tests
pytest

# Run only unit tests (fast, no Docker required)
pytest -m unit

# Run only integration tests (requires Docker)
pytest -m integration

# Run tests for a specific module
pytest test/unit/database/
pytest test/integration/database/

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test file
pytest test/unit/test_s3_client.py

# Run specific test class or method
pytest test/unit/test_s3_client.py::TestS3ClientInitialization
pytest test/unit/test_s3_client.py::TestS3ClientInitialization::test_s3_client_initialization
```

## 🏷️ Test Markers

Tests use pytest markers defined in `pytest.ini`:

- `@pytest.mark.unit` - Unit tests (fast, mocked dependencies)
- `@pytest.mark.integration` - Integration tests (real services)
- `@pytest.mark.slow` - Tests that take more than a few seconds
- `@pytest.mark.database` - Tests requiring database connection

## 📦 Shared Fixtures

The `test/conftest.py` file contains fixtures available to all tests:

- `test_data_dates` - Common test dates (trading days, weekends, holidays)
- `test_tickers` - Lists of valid/invalid ticker symbols
- `fixtures_path` - Path to test data files
- `raw_data_fixtures_path` - Path to raw CSV test files
- `test_output_dir` - Temporary directory for test outputs

## 🐳 Docker Services

Integration tests use Docker containers managed by `test/integration/docker-compose.test.yml`:

- **PostgreSQL + TimescaleDB** - Port 5433 (test database)
- **LocalStack** - Port 4566 (mock AWS S3)

Services are automatically started/stopped by pytest fixtures.

## 📝 Test Data

### Raw Data Fixtures (`test/fixtures/raw/`)
- Compressed CSV files mimicking Polygon.io S3 structure
- Contains sample market data for testing S3 operations
- Files named by date: `2024-01-02.csv.gz`, etc.

### Database Test Data
- Created dynamically in tests using pandas DataFrames
- Includes edge cases: duplicates, gaps, invalid data
- Cleaned between tests using TRUNCATE

## 🚀 Best Practices

1. **Test Isolation**
   - Each test should be independent
   - Use fixtures for setup/teardown
   - Don't rely on test execution order

2. **Naming Conventions**
   - Test files: `test_<module_name>.py`
   - Test classes: `Test<Feature>`
   - Test methods: `test_<what_it_tests>`

3. **Assertions**
   - Be specific in assertions
   - Test both success and failure cases
   - Include edge cases

4. **Performance**
   - Keep unit tests under 1 second
   - Mock external dependencies in unit tests
   - Use `@pytest.mark.slow` for longer tests

## 🔍 Debugging Tests

```bash
# Run with verbose output
pytest -v

# Show print statements
pytest -s

# Drop into debugger on failure
pytest --pdb

# Show local variables on failure
pytest -l

# Run specific tests matching pattern
pytest -k "test_insert"

# Run tests in parallel (requires pytest-xdist)
pytest -n auto
```

## 📊 Test Coverage

Aim for:
- 80%+ coverage for business logic
- 90%+ coverage for critical paths
- Focus on quality over quantity

Check coverage with:
```bash
pytest --cov=src --cov-report=term-missing
```

## 🎨 Writing New Tests

1. **Determine test type** (unit vs integration)
2. **Place in correct directory** following module structure
3. **Use appropriate fixtures** from conftest files
4. **Add proper markers** (@pytest.mark.unit, etc.)
5. **Follow existing patterns** in similar tests
6. **Document complex test logic** with comments
7. **Run tests locally** before committing

See `test/unit/README.md` and `test/integration/README.md` for specific guidelines.