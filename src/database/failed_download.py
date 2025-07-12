"""Failed downloads tracking client."""
import logging
from datetime import date
from typing import List

import pandas as pd

from .base import BaseDBClient
from s3_client import FailedDownload


class FailedDownloadsClient(BaseDBClient):
    """Client for failed_downloads tracking table."""
    
    def record_failure(self, failed: FailedDownload) -> None:
        """Record a failed download attempt."""
        pass
    
    def record_failures_batch(self, failures: List[FailedDownload]) -> None:
        """Record multiple failures in one transaction."""
        pass
    
    def get_pending_retries(self, max_attempts: int) -> List[FailedDownload]:
        """Get failed downloads that should be retried."""
        pass
    
    def increment_attempts(self, ticker: str, date: date, data_type: str) -> None:
        """Increment retry attempts for a failed download."""
        pass
    
    def mark_resolved(self, ticker: str, date: date, data_type: str) -> None:
        """Mark a failed download as resolved."""
        pass
    
    def get_failure_summary(self) -> pd.DataFrame:
        """Get summary of all failures by ticker and error type."""
        pass
    
    def cleanup_old_resolved(self, days: int) -> int:
        """Clean up old resolved failures."""
        pass