import boto3
import boto3.session
import pandas as pd
import gzip
import io
import time
from datetime import datetime, date
from typing import Optional, List, Dict
from pathlib import Path
from enum import Enum
import logging

from botocore.exceptions import ClientError
from config_loader import S3Config
from typing import Tuple, List
from dataclasses import dataclass
from datetime import datetime


@dataclass
class FailedDownload:
    """Record of failed download for retry later"""

    ticker: str
    date: date
    data_type: str
    error_type: str  # 'download_error', 'parse_error', 'rate_limit'
    error_message: str
    attempts: int
    timestamp: datetime


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

        # Create S3 client with provided endpoint and credentials
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=self.config.credentials.access_key,
            aws_secret_access_key=self.config.credentials.secret_key,
            region_name=s3_config.region,
            endpoint_url=s3_config.endpoint,
            config=boto3.session.Config(
                connect_timeout=s3_config.connect_timeout_seconds,
                read_timeout=s3_config.read_timeout_seconds,
                retries={"max_attempts": s3_config.max_retries},
            ),
        )

        self.bucket_name = s3_config.bucket_name
        self.path_structure = s3_config.path_structure

        self.logger = logging.getLogger(__name__)

        # Test connection with a simple operation
        try:
            self.s3_client.list_objects_v2(Bucket=self.bucket_name, MaxKeys=1)
            self.logger.info(f"Successfully connected to S3 bucket: {self.bucket_name}")
        except Exception as e:
            self.logger.error(f"Failed to connect to S3 bucket {self.bucket_name}: {e}")
            raise

    def download_date_range(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
        data_type: DataType = DataType.DAY_AGGS,
    ) -> Tuple[pd.DataFrame, List[FailedDownload]]:
        """Download data for a ticker across multiple dates

        Returns:
            Tuple of (successful_data_df, failed_downloads_list)
        """

        # Validate date range
        if start_date > end_date:
            raise ValueError(
                f"start_date {start_date} cannot be after end_date {end_date}"
            )

        # Generate date range (business days only to skip weekends)
        date_range = pd.bdate_range(start=start_date, end=end_date)

        # Initialize results
        all_data = []
        failed_downloads = []
        successful_downloads = 0

        self.logger.info(
            f"Starting download for {ticker} from {start_date} to {end_date} ({len(date_range)} business days)"
        )

        # Loop through each date
        for current_date in date_range:
            try:
                df = self.download_daily_data(ticker, current_date.date(), data_type)
                if df is not None and not df.empty:
                    all_data.append(df)
                    successful_downloads += 1
                # Note: None return means file doesn't exist (normal for holidays/market closures)

            except Exception as e:
                # Unexpected error at the download_daily_data level - record for retry
                failed_download = FailedDownload(
                    ticker=ticker,
                    date=current_date.date(),
                    data_type=data_type.value,
                    error_type="unexpected_error",
                    error_message=str(e),
                    attempts=1,
                    timestamp=datetime.now(),
                )
                failed_downloads.append(failed_download)
                self.logger.error(
                    f"Unexpected error downloading {ticker} for {current_date.date()}: {e}"
                )

        # Log summary
        total_dates = len(date_range)
        failed_count = len(failed_downloads)

        self.logger.info(
            f"Download summary for {ticker} ({start_date} to {end_date}): "
            f"Success: {successful_downloads}/{total_dates}, "
            f"Failed: {failed_count}/{total_dates}"
        )

        if failed_downloads:
            self.logger.warning(
                f"Failed downloads for {ticker}: {[f.date for f in failed_downloads]}"
            )

        # Combine all DataFrames
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df = combined_df.sort_values("timestamp")  # Sort by date
            self.logger.info(f"Combined {len(combined_df)} total rows for {ticker}")
            return combined_df, failed_downloads
        else:
            self.logger.warning(
                f"No data downloaded for {ticker} in range {start_date} to {end_date}"
            )
            return pd.DataFrame(), failed_downloads

    def download_daily_data(
        self, ticker: str, date: date, data_type: DataType = DataType.DAY_AGGS
    ) -> Optional[pd.DataFrame]:
        """Download daily data for a specific ticker and date

        Returns:
            DataFrame with data if successful
            None if file doesn't exist (normal for holidays/weekends)

        Raises:
            Exception if download/parsing fails after all retries
        """

        # Fast check - if file doesn't exist, return None (not an error)
        if not self.check_file_exists(ticker, date, data_type):
            self.logger.info(
                f"No file available for {ticker} on {date} (market closed/holiday)"
            )
            return None

        # Generate S3 path
        s3_path = self._generate_s3_path(date, data_type)

        # File exists - download with retry logic, RAISE exception if all retries fail
        for attempt in range(self.config.max_retries + 1):
            try:
                # Get object from S3
                s3_object = self.s3_client.get_object(
                    Bucket=self.bucket_name, Key=s3_path
                )

                # Read compressed content
                if s3_path.endswith(".gz"):
                    content = gzip.decompress(s3_object["Body"].read()).decode("utf-8")
                else:
                    content = s3_object["Body"].read().decode("utf-8")

                # Parse CSV content with quality validation
                df = self._parse_csv_data(content, ticker, date)
                if df.empty:
                    self.logger.warning(
                        f"No data found for {ticker} on {date} after parsing"
                    )
                    return None

                self.logger.info(
                    f"Successfully downloaded {len(df)} rows for {ticker} on {date}"
                )
                return df

            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                if error_code == "404":
                    # File disappeared between check and download - not an error
                    self.logger.info(f"File no longer available for {ticker} on {date}")
                    return None
                elif error_code in [
                    "429",
                    "503",
                ]:  # Rate limiting / Service unavailable
                    if attempt < self.config.max_retries:
                        wait_time = 60 + (30 * attempt)  # 60s, 90s, 120s
                        self.logger.warning(
                            f"Rate limited for {ticker} on {date}. Attempt {attempt + 1}, waiting {wait_time}s..."
                        )
                        time.sleep(wait_time)
                    else:
                        # All retries exhausted - raise exception for download_date_range to catch
                        raise Exception(
                            f"Rate limiting persists for {ticker} on {date} after {self.config.max_retries} retries"
                        )
                else:
                    # Other S3 errors - retry
                    if attempt < self.config.max_retries:
                        wait_time = 2**attempt
                        self.logger.warning(
                            f"S3 error {error_code} for {ticker} on {date}. Attempt {attempt + 1}, retrying in {wait_time}s..."
                        )
                        time.sleep(wait_time)
                    else:
                        # All retries exhausted - raise exception
                        raise Exception(
                            f"S3 error {error_code} persists for {ticker} on {date} after {self.config.max_retries} retries"
                        )

            except Exception as e:
                # Parsing or other errors - retry
                if attempt < self.config.max_retries:
                    wait_time = 2**attempt
                    self.logger.warning(
                        f"Processing attempt {attempt + 1} failed for {ticker} on {date}: {e}. Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    # All retries exhausted - raise exception
                    raise Exception(
                        f"Processing failed for {ticker} on {date} after {self.config.max_retries} retries: {e}"
                    )

        # Should never reach here, but just in case
        raise Exception(
            f"Unexpected error: all retry attempts completed without success or failure for {ticker} on {date}"
        )

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
            if e.response["Error"]["Code"] == "404":
                self.logger.debug(f"File not found: {s3_path}")
                return False
            else:
                self.logger.error(f"Error checking file {s3_path}: {e}")
                raise

    def get_available_dates(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
        data_type: DataType = DataType.DAY_AGGS,
    ) -> List[date]:
        """Get list of available dates for a ticker in date range"""

        # Validate date range
        if start_date > end_date:
            raise ValueError(
                f"start_date {start_date} cannot be after end_date {end_date}"
            )

        # Generate date range - use all dates for crypto compatibility
        date_range = pd.date_range(start=start_date, end=end_date, freq="D")

        available_dates = []

        self.logger.info(
            f"Checking availability for {ticker} from {start_date} to {end_date} ({len(date_range)} total days)"
        )

        # Check each date for file existence
        for current_date in date_range:
            try:
                if self.check_file_exists(ticker, current_date.date(), data_type):
                    available_dates.append(current_date.date())
            except Exception as e:
                # Log error but continue checking other dates
                self.logger.warning(
                    f"Error checking file existence for {ticker} on {current_date.date()}: {e}"
                )

        self.logger.info(
            f"Found {len(available_dates)} available dates for {ticker} out of {len(date_range)} total days"
        )

        return available_dates

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

    def _parse_csv_data(
        self, csv_content: str, ticker: str, date: date
    ) -> pd.DataFrame:
        """Parse CSV content and filter for specific ticker with quality validation"""

        # Create StringIO from CSV content for pandas
        csv_io = io.StringIO(csv_content)

        # Read CSV with pandas
        df = pd.read_csv(csv_io)

        # Validate required columns exist
        required_cols = ["ticker", "open", "high", "low", "close", "volume"]
        if not set(required_cols).issubset(set(df.columns)):
            missing = set(required_cols) - set(df.columns)
            raise ValueError(f"Missing required columns: {missing}")
        
        # Check if vwap column exists (optional)
        has_vwap = "vwap" in df.columns

        # Filter for specific ticker only
        df_filtered = df[df["ticker"] == ticker].copy()

        # Check if we found any data for this ticker
        if df_filtered.empty:
            self.logger.warning(f"No data found for ticker {ticker} on {date}")
            return pd.DataFrame()

        # Data type conversions
        numeric_cols = ["open", "high", "low", "close", "volume"]
        if has_vwap:
            numeric_cols.append("vwap")
        
        for col in numeric_cols:
            if col in df_filtered.columns:
                df_filtered[col] = pd.to_numeric(df_filtered[col], errors="coerce")
        
        # Add vwap column if it doesn't exist
        if not has_vwap:
            df_filtered["vwap"] = None

        # Apply row-level quality validation
        validated_rows = []
        quality_metrics = {
            "total_rows": len(df_filtered),
            "accepted": 0,
            "rejected_ohlc_nan": 0,
            "volume_set_zero": 0,
            "vwap_kept_nan": 0,
            "invalid_ohlc_relationships": 0,
            "price_sanity_failures": 0,
        }

        for idx, row in df_filtered.iterrows():
            # Apply NaN handling strategy
            row_valid = True
            row_modified = row.copy()

            # OHLC NaN -> reject row
            ohlc_cols = ["open", "high", "low", "close"]
            if row[ohlc_cols].isna().any():
                quality_metrics["rejected_ohlc_nan"] += 1
                row_valid = False
                continue

            # Volume NaN -> set to 0
            if pd.isna(row["volume"]):
                row_modified["volume"] = 0
                quality_metrics["volume_set_zero"] += 1

            # VWAP NaN -> keep as NaN (will be NULL in DB)
            if pd.isna(row["vwap"]):
                quality_metrics["vwap_kept_nan"] += 1

            # OHLC relationship validation
            if (
                row["high"] < row["low"]
                or row["high"] < row["open"]
                or row["high"] < row["close"]
                or row["low"] > row["open"]
                or row["low"] > row["close"]
            ):
                quality_metrics["invalid_ohlc_relationships"] += 1
                row_valid = False
                continue

            # Price sanity checks
            if (
                any(row[col] <= 0 for col in ohlc_cols)
                or any(row[col] > 10000 for col in ohlc_cols)
                or row_modified["volume"] < 0
            ):
                quality_metrics["price_sanity_failures"] += 1
                row_valid = False
                continue

            if row_valid:
                # Add metadata columns
                row_modified["ingestion_date"] = date
                row_modified["data_source"] = "polygon_s3"
                # Add timestamp column using the date
                row_modified["timestamp"] = pd.Timestamp(date)
                validated_rows.append(row_modified)
                quality_metrics["accepted"] += 1

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
        rejection_rate = (
            (metrics["total_rows"] - metrics["accepted"]) / metrics["total_rows"] * 100
        )

        self.logger.info(
            f"Quality metrics for {ticker} on {date}: "
            f"Total: {metrics['total_rows']}, "
            f"Accepted: {metrics['accepted']}, "
            f"Rejection rate: {rejection_rate:.1f}%"
        )

        if metrics["rejected_ohlc_nan"] > 0:
            self.logger.warning(
                f"Rejected {metrics['rejected_ohlc_nan']} rows due to OHLC NaN"
            )

        if metrics["invalid_ohlc_relationships"] > 0:
            self.logger.warning(
                f"Rejected {metrics['invalid_ohlc_relationships']} rows due to invalid OHLC relationships"
            )

        if rejection_rate > 5:  # Alert threshold
            self.logger.error(
                f"HIGH REJECTION RATE: {rejection_rate:.1f}% for {ticker} on {date}"
            )
