"""Initial schema creation for TBP Data Pipeline

Revision ID: 001
Revises: 
Create Date: 2025-01-10 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '001'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create initial database schema with TimescaleDB hypertables."""
    
    # Create schema
    op.execute("CREATE SCHEMA IF NOT EXISTS trading")
    
    # Create main market data table
    op.create_table(
        'market_data_raw',
        sa.Column('ticker', sa.String(10), nullable=False),
        sa.Column('timestamp', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('timeframe', sa.String(10), nullable=False),
        sa.Column('data_source', sa.String(20), nullable=False),
        sa.Column('open', sa.Numeric(12, 4), nullable=False),
        sa.Column('high', sa.Numeric(12, 4), nullable=False),
        sa.Column('low', sa.Numeric(12, 4), nullable=False),
        sa.Column('close', sa.Numeric(12, 4), nullable=False),
        sa.Column('volume', sa.BigInteger(), nullable=False),
        sa.Column('transactions', sa.Integer(), nullable=True),
        sa.Column('ingested_at', sa.TIMESTAMP(timezone=True), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint('ticker', 'timestamp', 'timeframe', 'data_source'),
        schema='trading'
    )
    
    # Convert to hypertable
    op.execute("""
        SELECT create_hypertable(
            'trading.market_data_raw',
            'timestamp',
            if_not_exists => TRUE
        )
    """)
    
    # Create indexes
    op.create_index(
        'idx_market_data_timestamp',
        'market_data_raw',
        ['timestamp'],
        schema='trading',
        postgresql_using='btree',
        postgresql_concurrently=False
    )
    
    op.create_index(
        'idx_market_data_ticker',
        'market_data_raw',
        ['ticker'],
        schema='trading'
    )
    
    op.create_index(
        'idx_market_data_ticker_timeframe',
        'market_data_raw',
        ['ticker', 'timeframe'],
        schema='trading'
    )
    
    # Create failed downloads tracking table
    op.create_table(
        'failed_downloads',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('ticker', sa.String(10), nullable=False),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('data_type', sa.String(20), nullable=False),
        sa.Column('error_type', sa.String(50), nullable=False),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('attempts', sa.Integer(), server_default='1'),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.func.now()),
        sa.Column('last_attempt_at', sa.TIMESTAMP(timezone=True), server_default=sa.func.now()),
        sa.Column('resolved_at', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        schema='trading'
    )
    
    # Create data quality metrics table
    op.create_table(
        'data_quality_metrics',
        sa.Column('ticker', sa.String(10), nullable=False),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('data_type', sa.String(20), nullable=False),
        sa.Column('total_rows', sa.Integer(), nullable=True),
        sa.Column('accepted_rows', sa.Integer(), nullable=True),
        sa.Column('rejected_rows', sa.Integer(), nullable=True),
        sa.Column('modified_rows', sa.Integer(), nullable=True),
        sa.Column('rejection_reasons', postgresql.JSONB(), nullable=True),
        sa.Column('quality_score', sa.Numeric(5, 2), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint('ticker', 'date', 'data_type'),
        schema='trading'
    )
    
    # Create indexes for failed downloads
    op.create_index(
        'idx_failed_downloads_ticker_date',
        'failed_downloads',
        ['ticker', 'date'],
        schema='trading'
    )
    
    op.create_index(
        'idx_failed_downloads_unresolved',
        'failed_downloads',
        ['resolved_at'],
        schema='trading',
        postgresql_where=sa.text('resolved_at IS NULL')
    )


def downgrade() -> None:
    """Drop all tables and schema."""
    
    # Drop indexes first
    op.drop_index('idx_failed_downloads_unresolved', schema='trading')
    op.drop_index('idx_failed_downloads_ticker_date', schema='trading')
    op.drop_index('idx_market_data_ticker_timeframe', schema='trading')
    op.drop_index('idx_market_data_ticker', schema='trading')
    op.drop_index('idx_market_data_timestamp', schema='trading')
    
    # Drop tables
    op.drop_table('data_quality_metrics', schema='trading')
    op.drop_table('failed_downloads', schema='trading')
    op.drop_table('market_data_raw', schema='trading')
    
    # Drop schema
    op.execute("DROP SCHEMA IF EXISTS trading CASCADE")
