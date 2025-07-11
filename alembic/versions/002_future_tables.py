"""Add trades and alternative bars tables

Revision ID: 002
Revises: 001
Create Date: 2025-01-10 12:05:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '002'
down_revision: Union[str, None] = '001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add tables for trades data and alternative bars."""
    
    # Create trades table (for future use)
    op.create_table(
        'trades_raw',
        sa.Column('ticker', sa.String(10), nullable=False),
        sa.Column('timestamp', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('data_source', sa.String(20), nullable=False),
        sa.Column('price', sa.Numeric(12, 4), nullable=False),
        sa.Column('size', sa.BigInteger(), nullable=False),
        sa.Column('conditions', sa.String(50), nullable=True),
        sa.Column('exchange', sa.String(10), nullable=True),
        sa.Column('ingested_at', sa.TIMESTAMP(timezone=True), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint('ticker', 'timestamp', 'data_source'),
    )
    
    # Convert to hypertable
    op.execute("""
        SELECT create_hypertable(
            'trades_raw',
            'timestamp',
            if_not_exists => TRUE
        )
    """)
    
    # Create quotes table (for future use)
    op.create_table(
        'quotes_raw',
        sa.Column('ticker', sa.String(10), nullable=False),
        sa.Column('timestamp', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('data_source', sa.String(20), nullable=False),
        sa.Column('bid_price', sa.Numeric(12, 4), nullable=True),
        sa.Column('bid_size', sa.BigInteger(), nullable=True),
        sa.Column('ask_price', sa.Numeric(12, 4), nullable=True),
        sa.Column('ask_size', sa.BigInteger(), nullable=True),
        sa.Column('exchange', sa.String(10), nullable=True),
        sa.Column('ingested_at', sa.TIMESTAMP(timezone=True), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint('ticker', 'timestamp', 'data_source'),
    )
    
    # Convert to hypertable
    op.execute("""
        SELECT create_hypertable(
            'quotes_raw',
            'timestamp',
            if_not_exists => TRUE
        )
    """)
    
    # Create alternative bars table for López de Prado methods
    op.create_table(
        'alternative_bars',
        sa.Column('ticker', sa.String(10), nullable=False),
        sa.Column('bar_type', sa.String(20), nullable=False),  # 'dollar', 'volume', 'tick', 'imbalance'
        sa.Column('bar_number', sa.BigInteger(), nullable=False),
        sa.Column('start_timestamp', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('end_timestamp', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('open', sa.Numeric(12, 4), nullable=False),
        sa.Column('high', sa.Numeric(12, 4), nullable=False),
        sa.Column('low', sa.Numeric(12, 4), nullable=False),
        sa.Column('close', sa.Numeric(12, 4), nullable=False),
        sa.Column('volume', sa.BigInteger(), nullable=False),
        sa.Column('dollar_volume', sa.Numeric(15, 2), nullable=True),
        sa.Column('transactions', sa.Integer(), nullable=True),
        sa.Column('bar_metric', sa.Numeric(15, 2), nullable=True),  # Dollar amount, volume count, etc.
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint('ticker', 'bar_type', 'bar_number', 'end_timestamp'),
    )
    
    # Convert to hypertable on end_timestamp for time-based queries
    op.execute("""
        SELECT create_hypertable(
            'alternative_bars',
            'end_timestamp',
            if_not_exists => TRUE
        )
    """)
    
    # Create indexes for trades
    op.create_index(
        'idx_trades_timestamp',
        'trades_raw',
        ['timestamp'],
    )
    
    op.create_index(
        'idx_trades_ticker',
        'trades_raw',
        ['ticker'],
    )
    
    # Create indexes for quotes
    op.create_index(
        'idx_quotes_timestamp',
        'quotes_raw',
        ['timestamp'],
    )
    
    op.create_index(
        'idx_quotes_ticker',
        'quotes_raw',
        ['ticker'],
    )
    
    # Create indexes for alternative bars
    op.create_index(
        'idx_alt_bars_end_timestamp',
        'alternative_bars',
        ['end_timestamp'],
    )
    
    op.create_index(
        'idx_alt_bars_ticker_type',
        'alternative_bars',
        ['ticker', 'bar_type'],
    )
    
    op.create_index(
        'idx_alt_bars_ticker_type_timestamp',
        'alternative_bars',
        ['ticker', 'bar_type', 'end_timestamp'],
    )


def downgrade() -> None:
    """Drop trades, quotes, and alternative bars tables."""
    
    # Drop indexes
    op.drop_index('idx_alt_bars_ticker_type_timestamp')
    op.drop_index('idx_alt_bars_ticker_type')
    op.drop_index('idx_alt_bars_end_timestamp')
    op.drop_index('idx_quotes_ticker')
    op.drop_index('idx_quotes_timestamp')
    op.drop_index('idx_trades_ticker')
    op.drop_index('idx_trades_timestamp')
    
    # Drop tables
    op.drop_table('alternative_bars')
    op.drop_table('quotes_raw')
    op.drop_table('trades_raw')
