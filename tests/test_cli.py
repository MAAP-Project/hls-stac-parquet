"""Tests for the CLI module."""

import pytest
from datetime import datetime

from hls_stac_parquet.cli import (
    parse_date_input,
    parse_month_input,
    generate_date_range,
    generate_month_dates,
)


class TestDateParsing:
    """Test date parsing functions."""
    
    def test_parse_date_input_yyyymmdd(self):
        """Test parsing YYYYMMDD format."""
        result = parse_date_input("20240515")
        expected = datetime(2024, 5, 15)
        assert result == expected
    
    def test_parse_date_input_yyyy_mm_dd(self):
        """Test parsing YYYY-MM-DD format."""
        result = parse_date_input("2024-05-15")
        expected = datetime(2024, 5, 15)
        assert result == expected
    
    def test_parse_date_input_invalid_format(self):
        """Test parsing invalid date format."""
        with pytest.raises(ValueError, match="Invalid date format"):
            parse_date_input("2024/05/15")
    
    def test_parse_date_input_invalid_date(self):
        """Test parsing invalid date."""
        with pytest.raises(ValueError, match="Invalid date format"):
            parse_date_input("20240532")  # Invalid day
    
    def test_parse_month_input_valid(self):
        """Test parsing valid month input."""
        result = parse_month_input("202405")
        expected = (2024, 5)
        assert result == expected
    
    def test_parse_month_input_invalid_format(self):
        """Test parsing invalid month format."""
        with pytest.raises(ValueError, match="Invalid month format"):
            parse_month_input("2024-05")
    
    def test_parse_month_input_invalid_month(self):
        """Test parsing invalid month."""
        with pytest.raises(ValueError, match="Invalid month format"):
            parse_month_input("202413")  # Invalid month


class TestDateGeneration:
    """Test date generation functions."""
    
    def test_generate_date_range_single_day(self):
        """Test generating range for single day."""
        start = datetime(2024, 5, 15)
        end = datetime(2024, 5, 15)
        result = generate_date_range(start, end)
        assert len(result) == 1
        assert result[0] == start
    
    def test_generate_date_range_multiple_days(self):
        """Test generating range for multiple days."""
        start = datetime(2024, 5, 15)
        end = datetime(2024, 5, 17)
        result = generate_date_range(start, end)
        expected = [
            datetime(2024, 5, 15),
            datetime(2024, 5, 16),
            datetime(2024, 5, 17),
        ]
        assert result == expected
    
    def test_generate_month_dates_regular_month(self):
        """Test generating dates for regular month."""
        result = generate_month_dates(2024, 5)  # May 2024
        assert len(result) == 31
        assert result[0] == datetime(2024, 5, 1)
        assert result[-1] == datetime(2024, 5, 31)
    
    def test_generate_month_dates_february_leap_year(self):
        """Test generating dates for February in leap year."""
        result = generate_month_dates(2024, 2)  # February 2024 (leap year)
        assert len(result) == 29
        assert result[0] == datetime(2024, 2, 1)
        assert result[-1] == datetime(2024, 2, 29)
    
    def test_generate_month_dates_february_non_leap_year(self):
        """Test generating dates for February in non-leap year."""
        result = generate_month_dates(2023, 2)  # February 2023 (non-leap year)
        assert len(result) == 28
        assert result[0] == datetime(2023, 2, 1)
        assert result[-1] == datetime(2023, 2, 28)