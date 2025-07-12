# Database Module Guidelines for Claude Code

## 🎯 Module Purpose
Handles all PostgreSQL/TimescaleDB operations for market data storage.

## 📁 Current Structure
```
database/
├── __init__.py
├── connection.py      # 📋 TODO: Connection pool management
├── market_data.py     # 🚧 MarketDataClient implementation
└── utils.py          # ✅ Helper functions (COPY, batch ops)
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
def _get_connection(self) -> Connection:
    """Get connection from pool."""
    # TODO: When ConnectionManager exists:
    # return self.connection_manager.get_connection()
    
    # For now, using mock or direct connection
    return self._connection
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