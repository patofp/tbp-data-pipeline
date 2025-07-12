"""Data quality metrics client."""
import logging
from datetime import date
from typing import Optional, List, Dict, Any

import pandas as pd

from .base import BaseDBClient


class DataQualityClient(BaseDBClient):
    """Client for data_quality_metrics table."""
    
    def insert_metrics(self, ticker: str, date: date, metrics: Dict[str, Any]) -> None:
        """Insert quality metrics for a specific ticker and date."""
        pass
    
    def insert_metrics_batch(self, metrics_list: List[Dict[str, Any]]) -> None:
        """Insert multiple quality metrics in one transaction."""
        pass
    
    def get_quality_score(self, ticker: str, date: date, 
                         data_type: str = 'day_aggs') -> Optional[float]:
        """Get quality score for specific ticker and date."""
        pass
    
    def get_daily_report(self, date: date) -> pd.DataFrame:
        """Get quality report for all tickers on a specific date."""
        pass
    
    def get_ticker_quality_trend(self, ticker: str, days: int = 30) -> pd.DataFrame:
        """Get quality score trend for a ticker."""
        pass
    
    def flag_quality_issues(self, threshold: float = 0.95) -> pd.DataFrame:
        """Find all ticker-dates with quality below threshold."""
        pass