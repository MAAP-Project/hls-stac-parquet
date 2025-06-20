"""Test the Python API functions."""

import tempfile
from pathlib import Path
from datetime import datetime

import pytest

from hls_stac_parquet.api import (
    parse_date_input,
    parse_month_input,
    generate_date_range,
    generate_month_dates,
    get_summary_stats,
)


class TestAPIFunctions:
    """Test the API functions directly."""
    
    def test_parse_date_input_formats(self):
        """Test that different date formats work."""
        # YYYYMMDD format
        date1 = parse_date_input("20240515")
        assert date1 == datetime(2024, 5, 15)
        
        # YYYY-MM-DD format
        date2 = parse_date_input("2024-05-15")
        assert date2 == datetime(2024, 5, 15)
        
        # Both should be equal
        assert date1 == date2
    
    def test_parse_month_input(self):
        """Test month parsing."""
        year, month = parse_month_input("202405")
        assert year == 2024
        assert month == 5
    
    def test_generate_date_range(self):
        """Test date range generation."""
        start_date = datetime(2024, 5, 15)
        end_date = datetime(2024, 5, 17)
        
        dates = generate_date_range(start_date, end_date)
        assert len(dates) == 3
        assert dates[0] == datetime(2024, 5, 15)
        assert dates[1] == datetime(2024, 5, 16)
        assert dates[2] == datetime(2024, 5, 17)
    
    def test_generate_month_dates(self):
        """Test month date generation."""
        # Test May 2024 (31 days)
        dates = generate_month_dates(2024, 5)
        assert len(dates) == 31
        assert dates[0] == datetime(2024, 5, 1)
        assert dates[-1] == datetime(2024, 5, 31)
        
        # Test February 2024 (leap year, 29 days)
        dates = generate_month_dates(2024, 2)
        assert len(dates) == 29
        assert dates[-1] == datetime(2024, 2, 29)
    
    def test_get_summary_stats(self):
        """Test summary statistics generation."""
        # Mock results
        results = [
            {"date": "2024-05-15", "success": True, "skipped": False, "stac_items_fetched": 10},
            {"date": "2024-05-16", "success": True, "skipped": True, "stac_items_fetched": 0},
            {"date": "2024-05-17", "success": False, "skipped": False, "error": "Network error"},
        ]
        
        stats = get_summary_stats(results)
        
        assert stats["total_days"] == 3
        assert stats["successful"] == 2  # One success + one skipped (counts as success)
        assert stats["skipped"] == 1
        assert stats["failed"] == 1
        assert stats["total_stac_items"] == 10
        assert stats["success_rate"] == 2/3
        assert len(stats["failed_days"]) == 1
        assert stats["failed_days"][0]["date"] == "2024-05-17"
        assert stats["failed_days"][0]["error"] == "Network error"


class TestHighLevelAPI:
    """Test the high-level API functions."""
    
    def test_api_import(self):
        """Test that we can import the high-level API functions."""
        import hls_stac_parquet
        
        # Check that main functions are available
        assert hasattr(hls_stac_parquet, 'process_date_range')
        assert hasattr(hls_stac_parquet, 'process_month')
        assert hasattr(hls_stac_parquet, 'repartition_by_year_month')
        assert hasattr(hls_stac_parquet, 'get_summary_stats')
    
    def test_parameter_validation(self):
        """Test that invalid parameters raise appropriate errors."""
        import hls_stac_parquet
        
        # Invalid date format should raise ValueError
        with pytest.raises(ValueError, match="Invalid date format"):
            hls_stac_parquet.parse_date_input("invalid")
        
        # Invalid month format should raise ValueError
        with pytest.raises(ValueError, match="Invalid month format"):
            hls_stac_parquet.parse_month_input("invalid")
    
    @pytest.mark.asyncio
    async def test_dry_run_functionality(self):
        """Test that we can create the basic API objects without errors."""
        import hls_stac_parquet
        
        # Test date parsing
        date = hls_stac_parquet.parse_date_input("20240515")
        assert isinstance(date, datetime)
        
        # Test date range generation
        dates = hls_stac_parquet.generate_date_range(date, date)
        assert len(dates) == 1
        
        # Test month dates
        month_dates = hls_stac_parquet.generate_month_dates(2024, 5)
        assert len(month_dates) == 31
        
        # Test summary stats with empty results
        stats = hls_stac_parquet.get_summary_stats([])
        assert stats["total_days"] == 0
        assert stats["success_rate"] == 0.0