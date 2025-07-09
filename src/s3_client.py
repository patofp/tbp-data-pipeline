import boto3
import pandas as pd
import gzip
import io
from datetime import datetime, date
from typing import Optional, List, Dict
from pathlib import Path
import logging

from config_loader import S3Config


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
            self.s3_client.head_bucket(Bucket=self.bucket_name)
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

    def check_file_exists(self, ticker: str, date: date) -> bool:
        """Check if a file exists in S3 for given ticker and date"""

    def get_available_dates(self, ticker: str, start_date: date, end_date: date) -> List[date]:
        """Get list of available dates for a ticker in date range"""

    def _generate_s3_path(self, date: date) -> str:
        """Generate S3 path for a given date"""

    def _parse_csv_data(self, csv_content: str, ticker: str, date: date) -> pd.DataFrame:
        """Parse CSV content and filter for specific ticker"""
