# Database Module Guidelines for Claude Code

## 🎯 Module Purpose
Handles all PostgreSQL/TimescaleDB operations for market data storage.

## 📁 Current Structure
```
database/
├── __init__.py
├── connection.py      # ✅ DONE: Connection pool management
├── market_data.py     # ✅ DONE: MarketDataClient implementation
├── base.py           # ✅ DONE: Base client with retry logic
├── utils.py          # ✅ DONE: Helper functions (COPY, batch ops)
├── client.py         # 📋 TODO: Main coordinator client
├── failed_download.py # 📋 TODO: Failed download tracking
└── data_quality.py   # 📋 TODO: Data quality metrics
```

## 🏗️ Architecture Decisions

### Connection Management:
- **Pool-based**: Share connections, never create directly
- **Context managers**: Ensure proper cleanup
- **Retry logic**: Handle transient failures

### Data Insertion Strategy:
- **COPY > INSERT**: Use PostgreSQL COPY for bulk operations
- **Batch size**: 5000-10000 rows optimal
- **ON CONFLICT**: UPDATE (last value wins)
- **Target**: >1000 rows/second

### Table Design:
- **Hypertables**: One per data type, not per ticker
- **Partitioning**: By time (1 month chunks)
- **Indexes**: ticker, timestamp, (ticker, timestamp)

## 💡 Implementation Patterns

### Using Connection Pool:
```python
# ✅ IMPLEMENTED - Connection pooling is now available!

# 1. Initialize ConnectionManager
from src.database.connection import ConnectionManager
conn_manager = ConnectionManager(db_config)
conn_manager.initialize()

# 2. Pass internal pool to clients (they expect psycopg2 pool)
client = MarketDataClient(db_config, conn_manager.pool._pool)

# 3. Use context manager for connections
with client._get_connection() as conn:
    cursor = conn.cursor()
    # Execute queries
```

### Bulk Insert Pattern:
```python
def insert_batch(self, df: pd.DataFrame, **kwargs) -> Dict:
    """Insert batch using COPY."""
    with self._get_connection() as conn:
        # Prepare data
        prepared = utils.prepare_dataframe_for_insert(df, **kwargs)
        
        # Use COPY for performance
        result = utils.execute_batch_insert(
            conn,
            self.table_name,
            prepared['tuples'],
            prepared['column_names']
        )
        
        return self._process_result(result, prepared['tracking'])
```

### Error Handling:
```python
try:
    result = operation()
except psycopg2.IntegrityError as e:
    if "duplicate key" in str(e):
        # Handle duplicates based on conflict strategy
    else:
        raise
except psycopg2.OperationalError:
    # Retry with backoff
```

## 🧪 Testing Requirements

### Unit Tests:
- Mock psycopg2 connections
- Test business logic only
- Verify SQL generation
- Test error handling

### Integration Tests:
- Use real PostgreSQL (Docker)
- Test constraints and indexes
- Verify performance metrics
- Test transaction handling

## 🚫 NO DEFAULT PARAMETERS - INCLUDING OPTIONAL

**CRITICAL**: No default values for ANY parameters, including Optional types!

### ❌ FORBIDDEN:
```python
def execute_query(query: str, params: Optional[tuple] = None):  # NO!
def insert_batch(df: pd.DataFrame, batch_size: Optional[int] = None):  # NO!
def get_data_summary(start_date: Optional[date] = None):  # NO!
```

### ✅ REQUIRED:
```python
def execute_query(query: str, params: Optional[tuple]):  # YES!
def insert_batch(df: pd.DataFrame, batch_size: Optional[int]):  # YES!
def get_data_summary(start_date: Optional[date]):  # YES!
```

Callers must explicitly pass None when needed:
```python
# Explicit None passing
execute_query("SELECT * FROM table", params=None)
insert_batch(df, batch_size=None)  # Use default calculation
get_data_summary(start_date=None)  # No date filter
```

## ⚡ Performance Guidelines

### Batch Processing:
```python
# Process in chunks to manage memory
for chunk in pd.read_csv(file, chunksize=10000):
    processed = prepare_chunk(chunk)
    insert_batch(processed)
```

### Monitoring:
```python
# Track performance metrics
start_time = time.time()
rows_inserted = execute_copy(...)
duration = time.time() - start_time

metrics = {
    'rows_per_second': rows_inserted / duration,
    'batch_size': len(data),
    'duration': duration
}
```

## 🚨 Common Pitfalls

1. **Using INSERT instead of COPY** → 100x slower
2. **Not handling duplicates** → Integrity errors
3. **Creating connections per operation** → Pool exhaustion
4. **Loading all data in memory** → OOM errors
5. **Ignoring timezones** → Data inconsistency

## 📊 SQL Patterns

### Gap Detection:
```sql
WITH expected_dates AS (
    SELECT generate_series(
        %(start_date)s::date,
        %(end_date)s::date,
        '1 day'::interval
    )::date AS date
)
SELECT date FROM expected_dates
WHERE EXTRACT(DOW FROM date) NOT IN (0, 6)  -- Exclude weekends
AND date NOT IN (
    SELECT DISTINCT timestamp::date 
    FROM trading.market_data_raw 
    WHERE ticker = %(ticker)s
)
```

### Performance Stats:
```sql
SELECT 
    COUNT(*) as record_count,
    MIN(timestamp)::date as first_date,
    MAX(timestamp)::date as last_date,
    MAX(timestamp)::date - MIN(timestamp)::date as date_range_days
FROM trading.market_data_raw
WHERE ticker = %(ticker)s
```

## ✅ Before Committing

1. Test with 10k+ rows for performance
2. Verify indexes are used (EXPLAIN ANALYZE)
3. Check connection pool usage
4. Test duplicate handling
5. Verify timezone handling

Remember: This is financial data - correctness > speed!