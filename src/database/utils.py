"""Database utility functions for TBP Data Pipeline."""
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from urllib.parse import quote_plus

import pandas as pd
from psycopg2.extensions import connection as Connection

from config_loader import DatabaseConfig

logger = logging.getLogger(__name__)


def create_database_url(db_config: DatabaseConfig) -> str:
    """
    Create PostgreSQL connection URL from config.
    
    Args:
        db_config: DatabaseConfig object with connection details
        
    Returns:
        PostgreSQL connection URL string
        
    Example:
        >>> url = create_database_url(config)
        >>> # Returns: postgresql://user:pass@localhost:5432/trading
    """
    # Extract connection components
    username = db_config.connection.username
    password = db_config.connection.password
    host = db_config.connection.host
    port = db_config.connection.port
    database = db_config.connection.database
    
    # Validate required fields
    if not all([username, host, database]):
        raise ValueError(
            "Missing required database configuration: "
            "username, host, and database are required"
        )
    
    # Build URL with proper password escaping
    if password:
        # Escape special characters in password
        escaped_password = quote_plus(password)
        url = f"postgresql://{username}:{escaped_password}@{host}:{port}/{database}"
    else:
        url = f"postgresql://{username}@{host}:{port}/{database}"
    
    logger.debug(f"Created database URL for host: {host}:{port}/{database}")
    return url


def verify_timescaledb_extension(conn: Connection) -> bool:
    """
    Verify TimescaleDB extension is installed and enabled.
    
    Args:
        conn: Active PostgreSQL connection
        
    Returns:
        True if TimescaleDB is properly installed and enabled
        
    Raises:
        Exception: If connection is invalid
    """
    try:
        with conn.cursor() as cursor:
            # Check if extension exists
            cursor.execute(
                "SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'"
            )
            if not cursor.fetchone():
                logger.error("TimescaleDB extension not found in database")
                return False
            
            # Get and log version
            cursor.execute(
                "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"
            )
            version = cursor.fetchone()
            if version:
                logger.info(f"TimescaleDB version: {version[0]}")
            
            # Verify it's actually working
            try:
                cursor.execute("SELECT timescaledb_version()")
                full_version = cursor.fetchone()
                if full_version:
                    logger.debug(f"TimescaleDB full version info: {full_version[0]}")
                return True
            except Exception as e:
                logger.error(f"TimescaleDB function check failed: {e}")
                return False
                
    except Exception as e:
        logger.error(f"Error verifying TimescaleDB extension: {e}")
        raise


def build_insert_query(
    table_name: str,
    columns: List[str],
    on_conflict: Optional[Dict[str, Any]] = None,
    returning: Optional[List[str]] = None
) -> str:
    """
    Build INSERT query with optional ON CONFLICT and RETURNING clauses.
    
    Args:
        table_name: Full table name (e.g., 'trading.market_data_raw')
        columns: List of column names
        on_conflict: Dict with conflict handling:
            {
                'columns': ['ticker', 'timestamp', 'timeframe'],
                'action': 'update',  # or 'nothing'
                'update_columns': ['open', 'high', 'low', 'close', 'volume']
            }
        returning: List of columns to return
        
    Returns:
        SQL query string with placeholders
        
    Example:
        >>> query = build_insert_query(
        ...     'trading.market_data_raw',
        ...     ['ticker', 'timestamp', 'open', 'close'],
        ...     on_conflict={'columns': ['ticker', 'timestamp'], 'action': 'update'}
        ... )
    """
    # Base INSERT
    placeholders = ', '.join(['%s'] * len(columns))
    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    
    # Add ON CONFLICT clause if specified
    if on_conflict:
        conflict_cols = ', '.join(on_conflict['columns'])
        query += f" ON CONFLICT ({conflict_cols})"
        
        if on_conflict.get('action') == 'update':
            # Build UPDATE SET clause
            update_cols = on_conflict.get('update_columns', columns)
            # Exclude conflict columns from update
            update_cols = [col for col in update_cols if col not in on_conflict['columns']]
            
            if update_cols:
                set_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])
                query += f" DO UPDATE SET {set_clause}, ingested_at = NOW()"
            else:
                query += " DO NOTHING"
        else:
            query += " DO NOTHING"
    
    # Add RETURNING clause if specified
    if returning:
        query += f" RETURNING {', '.join(returning)}"
    
    return query


def build_multi_insert_query(
    table_name: str,
    columns: List[str],
    row_count: int,
    on_conflict: Optional[Dict[str, Any]] = None
) -> str:
    """
    Build multi-row INSERT query for batch operations.
    
    Args:
        table_name: Full table name
        columns: List of column names
        row_count: Number of rows to insert
        on_conflict: Conflict handling configuration
        
    Returns:
        SQL query string with placeholders for multiple rows
        
    Example:
        >>> query = build_multi_insert_query(
        ...     'trading.market_data_raw',
        ...     ['ticker', 'timestamp', 'close'],
        ...     row_count=3
        ... )
        >>> # Returns: INSERT INTO trading.market_data_raw (ticker, timestamp, close) 
        >>> #          VALUES (%s, %s, %s), (%s, %s, %s), (%s, %s, %s)
    """
    # Build value placeholders for each row
    single_row = f"({', '.join(['%s'] * len(columns))})"
    all_rows = ', '.join([single_row] * row_count)
    
    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES {all_rows}"
    
    # Add ON CONFLICT clause if specified
    if on_conflict:
        conflict_cols = ', '.join(on_conflict['columns'])
        query += f" ON CONFLICT ({conflict_cols})"
        
        if on_conflict.get('action') == 'update':
            update_cols = on_conflict.get('update_columns', columns)
            update_cols = [col for col in update_cols if col not in on_conflict['columns']]
            
            if update_cols:
                set_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])
                query += f" DO UPDATE SET {set_clause}, ingested_at = NOW()"
            else:
                query += " DO NOTHING"
        else:
            query += " DO NOTHING"
    
    return query


def prepare_dataframe_for_insert(
    df: pd.DataFrame,
    columns: List[str],
    add_ingested_at: bool = True,
    include_tracking: bool = True
) -> Dict[str, Any]:
    """
    Prepare DataFrame rows for INSERT operations with tracking info.
    
    Args:
        df: DataFrame to prepare
        columns: List of columns in order for INSERT
        add_ingested_at: Whether to add current timestamp
        include_tracking: Whether to include row tracking information
        
    Returns:
        Dictionary with:
        {
            'tuples': List[Tuple],  # Data ready for INSERT
            'tracking': List[Dict],  # Metadata for each row
            'column_names': List[str]  # Column order
        }
        
    Where tracking contains:
        [
            {
                'index': 0,  # Original DataFrame index
                'ticker': 'AAPL',
                'date': date(2024, 1, 1),
                'identifier': 'AAPL_2024-01-01'  # For logging
            },
            ...
        ]
        
    Raises:
        ValueError: If required columns are missing
    """
    # Validate columns exist
    missing_cols = set(columns) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Reset index to ensure we can track back
    df_indexed = df.reset_index(drop=False)
    
    # Create copy with only required columns
    df_copy = df_indexed[columns].copy()
    
    # Add ingested_at if requested and not present
    if add_ingested_at and 'ingested_at' in columns and 'ingested_at' not in df.columns:
        df_copy['ingested_at'] = datetime.utcnow()
    
    # Convert DataFrame to list of tuples
    # Replace NaN with None for proper NULL handling
    df_copy = df_copy.where(pd.notnull(df_copy), None)
    
    # Convert to tuples
    tuples = [tuple(row) for row in df_copy.itertuples(index=False)]
    
    result = {
        'tuples': tuples,
        'column_names': columns
    }
    
    # Add tracking info if requested
    if include_tracking:
        tracking = []
        for idx, row in df_indexed.iterrows():
            track_info = {
                'index': idx,
                'original_index': row.get('index', idx)
            }
            
            # Add identifying fields if present
            if 'ticker' in row:
                track_info['ticker'] = row['ticker']
            if 'timestamp' in row:
                track_info['date'] = row['timestamp'].date() if hasattr(row['timestamp'], 'date') else row['timestamp']
            if 'date' in row:
                track_info['date'] = row['date']
                
            # Create identifier for logging
            parts = []
            if 'ticker' in track_info and track_info['ticker'] is not None:
                parts.append(str(track_info['ticker']))
            if 'date' in track_info and track_info['date'] is not None:
                parts.append(str(track_info['date']))
            track_info['identifier'] = '_'.join(parts) if parts else f"row_{idx}"
            
            tracking.append(track_info)
        
        result['tracking'] = tracking
    
    logger.debug(f"Prepared {len(tuples)} rows for INSERT with tracking")
    return result


def estimate_batch_size(timeframe: str, operation: str = 'insert') -> int:
    """
    Estimate optimal batch size based on timeframe and operation.
    
    Args:
        timeframe: Timeframe string ('1d', '1m', etc.)
        operation: Type of operation ('insert', 'update', etc.)
        
    Returns:
        Recommended batch size
        
    Example:
        >>> size = estimate_batch_size('1d', 'insert')
        >>> # Returns: 500
    """
    # Conservative batch sizes for INSERT operations
    # Smaller than COPY to avoid overwhelming the database
    batch_sizes = {
        '1d': 500,     # ~2 years of daily data
        '4h': 300,     # ~50 days of 4-hour data
        '1h': 200,     # ~8 days of hourly data
        '30m': 100,    # ~2 days of 30-min data
        '15m': 100,    # ~1 day of 15-min data
        '5m': 100,     # ~8 hours of 5-min data
        '1m': 100,     # ~1.5 hours of minute data
    }
    
    size = batch_sizes.get(timeframe, 100)  # Default conservative size
    logger.debug(f"Batch size for {operation} with timeframe '{timeframe}': {size}")
    return size


def parse_pg_timestamp(timestamp_str: str) -> datetime:
    """
    Parse PostgreSQL timestamp to Python datetime.
    
    Args:
        timestamp_str: Timestamp string from PostgreSQL
        
    Returns:
        datetime object with timezone info
        
    Raises:
        ValueError: If timestamp format is not recognized
        
    Example:
        >>> dt = parse_pg_timestamp('2024-01-01 09:30:00+00')
        >>> # Returns: datetime(2024, 1, 1, 9, 30, 0, tzinfo=UTC)
    """
    # Common PostgreSQL timestamp formats
    formats = [
        '%Y-%m-%d %H:%M:%S%z',           # With timezone
        '%Y-%m-%d %H:%M:%S.%f%z',        # With microseconds and timezone
        '%Y-%m-%d %H:%M:%S',             # Without timezone
        '%Y-%m-%d %H:%M:%S.%f',          # With microseconds, no timezone
        '%Y-%m-%d',                      # Date only
    ]
    
    # Try each format
    for fmt in formats:
        try:
            return datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue
    
    # If no format worked, try pandas as fallback
    try:
        return pd.to_datetime(timestamp_str)
    except Exception:
        pass
    
    # All parsing attempts failed
    raise ValueError(
        f"Unable to parse timestamp '{timestamp_str}'. "
        f"Tried formats: {formats}"
    )


def validate_dataframe_schema(
    df: pd.DataFrame, 
    expected_schema: Dict[str, Any],
    strict: bool = False
) -> Tuple[bool, List[str]]:
    """
    Validate DataFrame has correct columns and compatible types.
    
    Args:
        df: DataFrame to validate
        expected_schema: Dict mapping column names to expected types
        strict: If True, reject extra columns
        
    Returns:
        Tuple of (is_valid, list_of_errors)
        
    Example:
        >>> schema = {
        ...     'ticker': 'object',
        ...     'timestamp': 'datetime64[ns]',
        ...     'close': 'float64'
        ... }
        >>> valid, errors = validate_dataframe_schema(df, schema)
    """
    errors = []
    
    # Check for missing columns
    expected_cols = set(expected_schema.keys())
    actual_cols = set(df.columns)
    missing = expected_cols - actual_cols
    
    if missing:
        errors.append(f"Missing columns: {sorted(missing)}")
    
    # Check data types for existing columns
    for col, expected_type in expected_schema.items():
        if col in df.columns:
            actual_type = str(df[col].dtype)
            
            # Type compatibility checks
            compatible = False
            
            if expected_type == 'numeric':
                # Any numeric type is acceptable
                compatible = pd.api.types.is_numeric_dtype(df[col])
            elif expected_type == 'datetime':
                # Any datetime type is acceptable
                compatible = pd.api.types.is_datetime64_any_dtype(df[col])
            elif expected_type == 'object' or expected_type == 'string':
                # String/object types
                compatible = pd.api.types.is_object_dtype(df[col]) or \
                           pd.api.types.is_string_dtype(df[col])
            else:
                # Exact match
                compatible = actual_type == expected_type
            
            if not compatible:
                errors.append(
                    f"Column '{col}': expected {expected_type}, "
                    f"got {actual_type}"
                )
    
    # Check for unexpected columns if strict mode
    if strict:
        extra = actual_cols - expected_cols
        if extra:
            errors.append(f"Unexpected columns: {sorted(extra)}")
    
    # Log validation results
    is_valid = len(errors) == 0
    if not is_valid:
        logger.warning(f"DataFrame validation failed: {'; '.join(errors)}")
    else:
        logger.debug("DataFrame validation passed")
    
    return is_valid, errors


def get_table_size_stats(conn: Connection, schema: str = 'trading') -> pd.DataFrame:
    """
    Get size statistics for all tables in schema.
    
    Args:
        conn: Database connection
        schema: Schema name to analyze
        
    Returns:
        DataFrame with table sizes and row counts
    """
    query = """
    SELECT 
        schemaname,
        relname as tablename,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname)) as total_size,
        pg_size_pretty(pg_relation_size(schemaname||'.'||relname)) as table_size,
        pg_size_pretty(pg_indexes_size(schemaname||'.'||relname)) as indexes_size,
        n_live_tup as row_count,
        n_dead_tup as dead_rows
    FROM pg_stat_user_tables
    WHERE schemaname = %s
    ORDER BY pg_total_relation_size(schemaname||'.'||relname) DESC;
    """
    
    return pd.read_sql_query(query, conn, params=(schema,))


def calculate_insert_throttle(
    rows_per_second: float,
    batch_size: int,
    current_rate: Optional[float] = None
) -> float:
    """
    Calculate sleep time to throttle insert rate.
    
    Args:
        rows_per_second: Target rate
        batch_size: Number of rows per batch
        current_rate: Current actual rate (for adjustment)
        
    Returns:
        Sleep time in seconds between batches
        
    Example:
        >>> sleep_time = calculate_insert_throttle(100, 50)
        >>> # Returns: 0.5 (50 rows / 100 rows/sec = 0.5 sec)
    """
    # Calculate base sleep time
    target_time_per_batch = batch_size / rows_per_second
    
    # Adjust based on current rate if provided
    if current_rate and current_rate > rows_per_second:
        # We're going too fast, increase sleep time
        adjustment = (current_rate / rows_per_second) - 1
        target_time_per_batch *= (1 + adjustment * 0.5)
    
    # Ensure minimum sleep time for system stability
    min_sleep = 0.01  # 10ms minimum
    
    return max(target_time_per_batch, min_sleep)
