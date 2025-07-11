"""Add comprehensive schema with all tables

Revision ID: 003
Revises: 002_future_tables
Create Date: 2025-01-11

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '003'
down_revision = '002_future_tables'
branch_labels = None
depends_on = None


def upgrade():
    """Create comprehensive schema with all tables for TBP Data Pipeline."""
    
    # Create schema if not exists
    op.execute("CREATE SCHEMA IF NOT EXISTS trading")
    
    # Update market_data_raw table to include data_source column if not exists
    op.execute("""
        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT 1 
                FROM information_schema.columns 
                WHERE table_schema = 'trading' 
                AND table_name = 'market_data_raw' 
                AND column_name = 'data_source'
            ) THEN
                ALTER TABLE trading.market_data_raw 
                ADD COLUMN data_source VARCHAR(20) NOT NULL DEFAULT 'polygon_s3';
            END IF;
        END $$;
    """)
    
    # Create failed_downloads table
    op.create_table(
        'failed_downloads',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('ticker', sa.String(length=10), nullable=False),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('data_type', sa.String(length=20), nullable=False),
        sa.Column('error_type', sa.String(length=50), nullable=False),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('attempts', sa.Integer(), nullable=False, server_default='1'),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('last_attempt_at', sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('resolved_at', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        schema='trading'
    )
    
    # Create indexes for failed_downloads
    op.create_index(
        'idx_failed_downloads_ticker_date',
        'failed_downloads',
        ['ticker', 'date'],
        schema='trading'
    )
    op.create_index(
        'idx_failed_downloads_unresolved',
        'failed_downloads',
        ['ticker', 'date'],
        schema='trading',
        postgresql_where=sa.text('resolved_at IS NULL')
    )
    
    # Create data_quality_metrics table
    op.create_table(
        'data_quality_metrics',
        sa.Column('ticker', sa.String(length=10), nullable=False),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('data_type', sa.String(length=20), nullable=False),
        sa.Column('total_rows', sa.Integer(), nullable=True),
        sa.Column('accepted_rows', sa.Integer(), nullable=True),
        sa.Column('rejected_rows', sa.Integer(), nullable=True),
        sa.Column('modified_rows', sa.Integer(), nullable=True),
        sa.Column('rejection_reasons', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('quality_score', sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('NOW()')),
        sa.PrimaryKeyConstraint('ticker', 'date', 'data_type'),
        schema='trading'
    )
    
    # Create trades_raw table (for future use)
    op.create_table(
        'trades_raw',
        sa.Column('ticker', sa.String(length=10), nullable=False),
        sa.Column('timestamp', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('data_source', sa.String(length=20), nullable=False),
        sa.Column('price', sa.Numeric(precision=12, scale=4), nullable=False),
        sa.Column('size', sa.BigInteger(), nullable=False),
        sa.Column('conditions', sa.String(length=50), nullable=True),
        sa.Column('exchange', sa.String(length=10), nullable=True),
        sa.Column('ingested_at', sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('NOW()')),
        sa.PrimaryKeyConstraint('ticker', 'timestamp', 'data_source'),
        schema='trading'
    )
    
    # Convert trades_raw to hypertable
    op.execute("""
        SELECT create_hypertable(
            'trading.trades_raw',
            'timestamp',
            if_not_exists => TRUE
        )
    """)
    
    # Create quotes_raw table (for future use)
    op.create_table(
        'quotes_raw',
        sa.Column('ticker', sa.String(length=10), nullable=False),
        sa.Column('timestamp', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('data_source', sa.String(length=20), nullable=False),
        sa.Column('bid_price', sa.Numeric(precision=12, scale=4), nullable=True),
        sa.Column('bid_size', sa.BigInteger(), nullable=True),
        sa.Column('ask_price', sa.Numeric(precision=12, scale=4), nullable=True),
        sa.Column('ask_size', sa.BigInteger(), nullable=True),
        sa.Column('exchange', sa.String(length=10), nullable=True),
        sa.Column('ingested_at', sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('NOW()')),
        sa.PrimaryKeyConstraint('ticker', 'timestamp', 'data_source'),
        schema='trading'
    )
    
    # Convert quotes_raw to hypertable
    op.execute("""
        SELECT create_hypertable(
            'trading.quotes_raw',
            'timestamp',
            if_not_exists => TRUE
        )
    """)
    
    # Create alternative_bars table (for López de Prado methods)
    op.create_table(
        'alternative_bars',
        sa.Column('ticker', sa.String(length=10), nullable=False),
        sa.Column('bar_type', sa.String(length=20), nullable=False),
        sa.Column('bar_number', sa.BigInteger(), nullable=False),
        sa.Column('start_timestamp', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('end_timestamp', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('open', sa.Numeric(precision=12, scale=4), nullable=False),
        sa.Column('high', sa.Numeric(precision=12, scale=4), nullable=False),
        sa.Column('low', sa.Numeric(precision=12, scale=4), nullable=False),
        sa.Column('close', sa.Numeric(precision=12, scale=4), nullable=False),
        sa.Column('volume', sa.BigInteger(), nullable=False),
        sa.Column('dollar_volume', sa.Numeric(precision=15, scale=2), nullable=True),
        sa.Column('transactions', sa.Integer(), nullable=True),
        sa.Column('bar_metric', sa.Numeric(precision=15, scale=2), nullable=True),
        sa.Column('ingested_at', sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('NOW()')),
        sa.PrimaryKeyConstraint('ticker', 'bar_type', 'bar_number'),
        schema='trading'
    )
    
    # Convert alternative_bars to hypertable on end_timestamp
    op.execute("""
        SELECT create_hypertable(
            'trading.alternative_bars',
            'end_timestamp',
            if_not_exists => TRUE
        )
    """)
    
    # Create indexes for alternative_bars
    op.create_index(
        'idx_alternative_bars_ticker_type',
        'alternative_bars',
        ['ticker', 'bar_type'],
        schema='trading'
    )
    op.create_index(
        'idx_alternative_bars_end_timestamp',
        'alternative_bars',
        ['end_timestamp'],
        schema='trading'
    )
    
    # Add composite index for market_data_raw if not exists
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_market_data_ticker_timeframe 
        ON trading.market_data_raw (ticker, timeframe);
    """)


def downgrade():
    """Drop the comprehensive schema tables."""
    
    # Drop indexes first
    op.drop_index('idx_alternative_bars_end_timestamp', schema='trading')
    op.drop_index('idx_alternative_bars_ticker_type', schema='trading')
    op.drop_index('idx_failed_downloads_unresolved', schema='trading')
    op.drop_index('idx_failed_downloads_ticker_date', schema='trading')
    
    # Drop tables
    op.drop_table('alternative_bars', schema='trading')
    op.drop_table('quotes_raw', schema='trading')
    op.drop_table('trades_raw', schema='trading')
    op.drop_table('data_quality_metrics', schema='trading')
    op.drop_table('failed_downloads', schema='trading')
    
    # Remove data_source column from market_data_raw if we added it
    op.execute("""
        ALTER TABLE trading.market_data_raw 
        DROP COLUMN IF EXISTS data_source;
    """)
