import boto3
import boto3.session
import pandas as pd
import gzip
import io
from datetime import datetime, date
from typing import Optional, List, Dict
from pathlib import Path
from enum import Enum
import logging

from botocore.exceptions import ClientError
from config_loader import S3Config


class DataType(Enum):
    DAY_AGGS = "day_aggs"
    MINUTE_AGGS = "minute_aggs" 
    TRADES = "trades"
    QUOTES = "quotes"

class PolygonS3Client:
    def __init__(self, s3_config: S3Config):
        """Initialize S3 client with configuration"""

        # Initialize the configuration
        self.config = s3_config

        # Create S3 resource with provided endpoint and credentials
        self.s3_client = boto3.resource(
            's3',
            aws_access_key_id=self.config.credentials.access_key,
            aws_secret_access_key=self.config.credentials.secret_key,
            region_name=s3_config.region,
            endpoint_url=s3_config.endpoint,
            config=boto3.session.Config(
                connect_timeout=s3_config.connect_timeout_seconds,
                read_timeout=s3_config.read_timeout_seconds,
                retries={
                    'max_attempts': s3_config.max_retries}
            )
        )

        self.bucket_name = s3_config.bucket_name
        self.path_structure = s3_config.path_structure

        self.logger = logging.getLogger(__name__)

        # Test connection with a simple operation
        try:
            self.s3_client.list_objects_v2(Bucket=self.bucket_name, MaxKeys=1)
            self.logger.info(
                f"Successfully connected to S3 bucket: {self.bucket_name}")
        except Exception as e:
            self.logger.error(
                f"Failed to connect to S3 bucket {self.bucket_name}: {e}")
            raise

    def download_daily_data(self, ticker: str, date: date) -> Optional[pd.DataFrame]:
        """Download daily data for a specific ticker and date"""

    def download_date_range(self, ticker: str, start_date: date, end_date: date) -> pd.DataFrame:
        """Download data for a ticker across multiple dates"""

    def check_file_exists(self, ticker: str, date: date, data_type: DataType) -> bool:
        """Check if a file exists in S3 for given ticker and date"""
    
        # Generate S3 path using _generate_s3_path helper
        s3_path = self._generate_s3_path(date, data_type)
        
        # Use boto3 head_object to check if file exists
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_path)
            self.logger.debug(f"File exists: {s3_path}")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                self.logger.debug(f"File not found: {s3_path}")
                return False
            else:
                self.logger.error(f"Error checking file {s3_path}: {e}")
                raise

    def get_available_dates(self, ticker: str, start_date: date, end_date: date) -> List[date]:
        """Get list of available dates for a ticker in date range"""

    def _generate_s3_path(self, date: date, data_type: DataType) -> str:
        """Generate S3 path for a given date"""
        
        # Use the day_aggs path structure from config
        path_template = getattr(self.path_structure, data_type.value)
        
        # Format the path with date components
        s3_path = path_template.format(
            year=date.year,
            month=date.month,
            day=date.day
        )
        
        return s3_path

    def _parse_csv_data(self, csv_content: str, ticker: str, date: date) -> pd.DataFrame:
        """Parse CSV content and filter for specific ticker with quality validation"""
        
        # Create StringIO from CSV content for pandas
        csv_io = io.StringIO(csv_content)
        
        # Read CSV with pandas
        df = pd.read_csv(csv_io)
        
        # Validate required columns exist
        required_cols = ['ticker', 'open', 'high', 'low', 'close', 'volume', 'vwap']
        if not set(required_cols).issubset(set(df.columns)):
            missing = set(required_cols) - set(df.columns)
            raise ValueError(f"Missing required columns: {missing}")
        
        # Filter for specific ticker only
        df_filtered = df[df['ticker'] == ticker].copy()
        
        # Check if we found any data for this ticker
        if df_filtered.empty:
            self.logger.warning(f"No data found for ticker {ticker} on {date}")
            return pd.DataFrame()
        
        # Data type conversions
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'vwap']
        for col in numeric_cols:
            if col in df_filtered.columns:
                df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce')
        
        # Apply row-level quality validation
        validated_rows = []
        quality_metrics = {
            'total_rows': len(df_filtered),
            'accepted': 0,
            'rejected_ohlc_nan': 0,
            'volume_set_zero': 0,
            'vwap_kept_nan': 0,
            'invalid_ohlc_relationships': 0,
            'price_sanity_failures': 0
        }
        
        for idx, row in df_filtered.iterrows():
            # Apply NaN handling strategy
            row_valid = True
            row_modified = row.copy()
            
            # OHLC NaN -> reject row
            ohlc_cols = ['open', 'high', 'low', 'close']
            if row[ohlc_cols].isna().any():
                quality_metrics['rejected_ohlc_nan'] += 1
                row_valid = False
                continue
            
            # Volume NaN -> set to 0
            if pd.isna(row['volume']):
                row_modified['volume'] = 0
                quality_metrics['volume_set_zero'] += 1
            
            # VWAP NaN -> keep as NaN (will be NULL in DB)
            if pd.isna(row['vwap']):
                quality_metrics['vwap_kept_nan'] += 1
            
            # OHLC relationship validation
            if (row['high'] < row['low'] or 
                row['high'] < row['open'] or 
                row['high'] < row['close'] or
                row['low'] > row['open'] or 
                row['low'] > row['close']):
                quality_metrics['invalid_ohlc_relationships'] += 1
                row_valid = False
                continue
            
            # Price sanity checks
            if (any(row[col] <= 0 for col in ohlc_cols) or
                any(row[col] > 10000 for col in ohlc_cols) or
                row_modified['volume'] < 0):
                quality_metrics['price_sanity_failures'] += 1
                row_valid = False
                continue
            
            if row_valid:
                # Add metadata columns
                row_modified['ingestion_date'] = date
                row_modified['data_source'] = 'polygon_s3'
                validated_rows.append(row_modified)
                quality_metrics['accepted'] += 1
        
        # Log quality metrics
        self._log_quality_metrics(ticker, date, quality_metrics)
        
        # Return validated DataFrame
        if validated_rows:
            return pd.DataFrame(validated_rows)
        else:
            self.logger.warning(f"All rows rejected for {ticker} on {date}")
            return pd.DataFrame()

    def _log_quality_metrics(self, ticker: str, date: date, metrics: Dict):
        """Log quality metrics for monitoring"""
        rejection_rate = (metrics['total_rows'] - metrics['accepted']) / metrics['total_rows'] * 100
        
        self.logger.info(
            f"Quality metrics for {ticker} on {date}: "
            f"Total: {metrics['total_rows']}, "
            f"Accepted: {metrics['accepted']}, "
            f"Rejection rate: {rejection_rate:.1f}%"
        )
        
        if metrics['rejected_ohlc_nan'] > 0:
            self.logger.warning(f"Rejected {metrics['rejected_ohlc_nan']} rows due to OHLC NaN")
        
        if metrics['invalid_ohlc_relationships'] > 0:
            self.logger.warning(f"Rejected {metrics['invalid_ohlc_relationships']} rows due to invalid OHLC relationships")
        
        if rejection_rate > 5:  # Alert threshold
            self.logger.error(f"HIGH REJECTION RATE: {rejection_rate:.1f}% for {ticker} on {date}")
