"""Python API for HLS STAC to Parquet conversion.

This module provides the core Python API functions that can be used
programmatically without the CLI interface.
"""

import asyncio
import calendar
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb
from obstore.store import LocalStore

from .main import hls_to_stac_geoparquet


def parse_date_input(date_input: str) -> datetime:
    """Parse date input in various formats.

    Args:
        date_input: Date string in format YYYYMMDD or YYYY-MM-DD

    Returns:
        Parsed datetime object

    Raises:
        ValueError: If date format is invalid
    """
    # Try YYYYMMDD format first
    if len(date_input) == 8 and date_input.isdigit():
        try:
            return datetime.strptime(date_input, "%Y%m%d")
        except ValueError:
            pass

    # Try YYYY-MM-DD format
    try:
        return datetime.strptime(date_input, "%Y-%m-%d")
    except ValueError:
        pass

    raise ValueError(
        f"Invalid date format: {date_input}. Expected YYYYMMDD or YYYY-MM-DD"
    )


def parse_month_input(month_input: str) -> Tuple[int, int]:
    """Parse month input in YYYYMM format.

    Args:
        month_input: Month string in format YYYYMM

    Returns:
        Tuple of (year, month)

    Raises:
        ValueError: If month format is invalid
    """
    if len(month_input) == 6 and month_input.isdigit():
        try:
            year = int(month_input[:4])
            month = int(month_input[4:6])
            if 1 <= month <= 12:
                return year, month
        except ValueError:
            pass

    raise ValueError(f"Invalid month format: {month_input}. Expected YYYYMM")


def generate_date_range(start_date: datetime, end_date: datetime) -> List[datetime]:
    """Generate list of dates between start and end dates (inclusive).

    Args:
        start_date: Start date
        end_date: End date

    Returns:
        List of datetime objects for each day in the range
    """
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date)
        current_date += timedelta(days=1)
    return dates


def generate_month_dates(year: int, month: int) -> List[datetime]:
    """Generate list of dates for all days in a month.

    Args:
        year: Year
        month: Month (1-12)

    Returns:
        List of datetime objects for each day in the month
    """
    num_days = calendar.monthrange(year, month)[1]
    return [datetime(year, month, day) for day in range(1, num_days + 1)]


async def process_single_day(
    date: datetime,
    output_dir: Path,
    max_concurrent: int = 50,
    show_progress: bool = True,
    skip_existing: bool = True,
    bounding_box: Optional[Tuple[float, float, float, float]] = None,
    output_filename_pattern: str = "hls_{date}.parquet",
    **rustac_kwargs,
) -> Dict[str, Any]:
    """Process a single day's data and write to parquet file.

    Args:
        date: Date to process
        output_dir: Output directory for parquet files
        max_concurrent: Maximum concurrent requests
        show_progress: Whether to show progress bars
        skip_existing: Whether to skip if output file already exists
        bounding_box: Optional spatial bounding box (west, south, east, north)
        output_filename_pattern: Pattern for output filename. Use {date} as placeholder
        **rustac_kwargs: Additional arguments passed to rustac.write()

    Returns:
        Dictionary with processing statistics
    """
    date_string = date.strftime("%Y-%m-%d")

    # Create output filename based on the date
    output_filename = output_filename_pattern.format(date=date.strftime("%Y%m%d"))
    output_path = output_dir / output_filename

    # Skip if file exists and skip_existing is True
    if skip_existing and output_path.exists():
        return {
            "date": date_string,
            "skipped": True,
            "output_path": str(output_path),
            "success": True,
        }

    # Create temporal range for the specified date
    start_time = f"{date_string}T00:00:00Z"
    end_time = f"{date_string}T23:59:59Z"

    try:
        stats = await hls_to_stac_geoparquet(
            output_path=output_filename,
            temporal=(start_time, end_time),
            bounding_box=bounding_box,
            max_concurrent=max_concurrent,
            show_progress=show_progress,
            store=LocalStore(prefix=str(output_dir)),
            **rustac_kwargs,
        )

        stats["date"] = date_string
        stats["skipped"] = False
        return stats

    except Exception as e:
        return {
            "date": date_string,
            "error": str(e),
            "success": False,
            "skipped": False,
        }


async def process_dates_sequential(
    dates: List[datetime],
    output_dir: Path,
    max_concurrent: int = 50,
    show_progress: bool = True,
    skip_existing: bool = True,
    bounding_box: Optional[Tuple[float, float, float, float]] = None,
    output_filename_pattern: str = "hls_{date}.parquet",
    **rustac_kwargs,
) -> List[Dict[str, Any]]:
    """Process multiple dates sequentially.

    Args:
        dates: List of dates to process
        output_dir: Output directory for parquet files
        max_concurrent: Maximum concurrent requests per day
        show_progress: Whether to show progress bars
        skip_existing: Whether to skip existing files
        bounding_box: Optional spatial bounding box (west, south, east, north)
        output_filename_pattern: Pattern for output filename. Use {date} as placeholder
        **rustac_kwargs: Additional arguments passed to rustac.write()

    Returns:
        List of statistics dictionaries for each day
    """
    results = []

    for date in dates:
        result = await process_single_day(
            date=date,
            output_dir=output_dir,
            max_concurrent=max_concurrent,
            show_progress=show_progress,
            skip_existing=skip_existing,
            bounding_box=bounding_box,
            output_filename_pattern=output_filename_pattern,
            **rustac_kwargs,
        )
        results.append(result)

    return results


async def process_dates_parallel(
    dates: List[datetime],
    output_dir: Path,
    max_concurrent_days: int = 3,
    max_concurrent_per_day: int = 20,
    show_progress: bool = True,
    skip_existing: bool = True,
    bounding_box: Optional[Tuple[float, float, float, float]] = None,
    output_filename_pattern: str = "hls_{date}.parquet",
    **rustac_kwargs,
) -> List[Dict[str, Any]]:
    """Process multiple dates in parallel.

    Args:
        dates: List of dates to process
        output_dir: Output directory for parquet files
        max_concurrent_days: Maximum number of days to process concurrently
        max_concurrent_per_day: Maximum concurrent requests per day
        show_progress: Whether to show progress bars
        skip_existing: Whether to skip existing files
        bounding_box: Optional spatial bounding box (west, south, east, north)
        output_filename_pattern: Pattern for output filename. Use {date} as placeholder
        **rustac_kwargs: Additional arguments passed to rustac.write()

    Returns:
        List of statistics dictionaries for each day
    """
    # Create semaphore to limit concurrent day processing
    semaphore = asyncio.Semaphore(max_concurrent_days)

    async def process_day_with_semaphore(date):
        async with semaphore:
            return await process_single_day(
                date=date,
                output_dir=output_dir,
                max_concurrent=max_concurrent_per_day,
                show_progress=show_progress,
                skip_existing=skip_existing,
                bounding_box=bounding_box,
                output_filename_pattern=output_filename_pattern,
                **rustac_kwargs,
            )

    # Process all days concurrently
    tasks = [process_day_with_semaphore(date) for date in dates]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Handle any exceptions
    processed_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            date_string = dates[i].strftime("%Y-%m-%d")
            processed_results.append(
                {
                    "date": date_string,
                    "error": str(result),
                    "success": False,
                    "skipped": False,
                }
            )
        else:
            processed_results.append(result)

    return processed_results


def process_date_range(
    start_date: str | datetime,
    output_dir: str | Path,
    end_date: str | datetime | None = None,
    parallel: bool = False,
    max_concurrent_days: int = 3,
    max_concurrent_per_day: int = 50,
    show_progress: bool = True,
    skip_existing: bool = True,
    bounding_box: Optional[Tuple[float, float, float, float]] = None,
    output_filename_pattern: str = "hls_{date}.parquet",
    **rustac_kwargs,
) -> List[Dict[str, Any]]:
    """High-level function to process a date range.

    Args:
        start_date: Start date (datetime object or string in YYYYMMDD/YYYY-MM-DD format)
        end_date: End date (datetime object or string). If None, only process start_date
        output_dir: Output directory for parquet files
        parallel: Whether to process dates in parallel
        max_concurrent_days: Maximum concurrent days (parallel mode only)
        max_concurrent_per_day: Maximum concurrent requests per day
        show_progress: Whether to show progress bars
        skip_existing: Whether to skip existing files
        bounding_box: Optional spatial bounding box (west, south, east, north)
        output_filename_pattern: Pattern for output filename. Use {date} as placeholder
        **rustac_kwargs: Additional arguments passed to rustac.write()

    Returns:
        List of statistics dictionaries for each day processed
    """
    # Parse dates
    if isinstance(start_date, str):
        start_date = parse_date_input(start_date)

    if end_date is None:
        dates = [start_date]
    else:
        if isinstance(end_date, str):
            end_date = parse_date_input(end_date)
        dates = generate_date_range(start_date, end_date)

    # Ensure output directory exists
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Process dates
    if parallel:
        return asyncio.run(
            process_dates_parallel(
                dates=dates,
                output_dir=output_dir,
                max_concurrent_days=max_concurrent_days,
                max_concurrent_per_day=max_concurrent_per_day,
                show_progress=show_progress,
                skip_existing=skip_existing,
                bounding_box=bounding_box,
                output_filename_pattern=output_filename_pattern,
                **rustac_kwargs,
            )
        )
    else:
        return asyncio.run(
            process_dates_sequential(
                dates=dates,
                output_dir=output_dir,
                max_concurrent=max_concurrent_per_day,
                show_progress=show_progress,
                skip_existing=skip_existing,
                bounding_box=bounding_box,
                output_filename_pattern=output_filename_pattern,
                **rustac_kwargs,
            )
        )


def process_month(
    year_month: str | Tuple[int, int],
    output_dir: str | Path,
    parallel: bool = False,
    max_concurrent_days: int = 3,
    max_concurrent_per_day: int = 50,
    show_progress: bool = True,
    skip_existing: bool = True,
    bounding_box: Optional[Tuple[float, float, float, float]] = None,
    output_filename_pattern: str = "hls_{date}.parquet",
    **rustac_kwargs,
) -> List[Dict[str, Any]]:
    """High-level function to process all days in a month.

    Args:
        year_month: Either a string in YYYYMM format or tuple of (year, month)
        output_dir: Output directory for parquet files
        parallel: Whether to process dates in parallel
        max_concurrent_days: Maximum concurrent days (parallel mode only)
        max_concurrent_per_day: Maximum concurrent requests per day
        show_progress: Whether to show progress bars
        skip_existing: Whether to skip existing files
        bounding_box: Optional spatial bounding box (west, south, east, north)
        output_filename_pattern: Pattern for output filename. Use {date} as placeholder
        **rustac_kwargs: Additional arguments passed to rustac.write()

    Returns:
        List of statistics dictionaries for each day processed
    """
    # Parse year/month
    if isinstance(year_month, str):
        year, month = parse_month_input(year_month)
    else:
        year, month = year_month

    # Generate dates for the month
    dates = generate_month_dates(year, month)

    # Ensure output directory exists
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Process dates
    if parallel:
        return asyncio.run(
            process_dates_parallel(
                dates=dates,
                output_dir=output_dir,
                max_concurrent_days=max_concurrent_days,
                max_concurrent_per_day=max_concurrent_per_day,
                show_progress=show_progress,
                skip_existing=skip_existing,
                bounding_box=bounding_box,
                output_filename_pattern=output_filename_pattern,
                **rustac_kwargs,
            )
        )
    else:
        return asyncio.run(
            process_dates_sequential(
                dates=dates,
                output_dir=output_dir,
                max_concurrent=max_concurrent_per_day,
                show_progress=show_progress,
                skip_existing=skip_existing,
                bounding_box=bounding_box,
                output_filename_pattern=output_filename_pattern,
                **rustac_kwargs,
            )
        )


def repartition_by_year_month(
    source_dir: str | Path, destination_dir: str | Path, overwrite: bool = False
) -> None:
    """Repartition daily parquet files by year-month.

    Args:
        source_dir: Directory containing daily parquet files
        destination_dir: Directory to write repartitioned files to
        overwrite: Whether to overwrite existing partitioned data

    Raises:
        ValueError: If source directory doesn't exist or has no parquet files
        RuntimeError: If DuckDB operation fails
    """
    source_dir = Path(source_dir)
    destination_dir = Path(destination_dir)

    if not source_dir.exists():
        raise ValueError(f"Source directory does not exist: {source_dir}")

    # Find parquet files in source directory
    parquet_files = list(source_dir.glob("*.parquet"))
    if not parquet_files:
        raise ValueError(f"No parquet files found in source directory: {source_dir}")

    # Create destination directory if it doesn't exist
    destination_dir.mkdir(parents=True, exist_ok=True)

    # Connect to DuckDB
    conn = duckdb.connect()

    try:
        # Build the SQL query
        source_pattern = str(source_dir / "*.parquet")
        dest_path = str(destination_dir)

        # Determine overwrite behavior
        overwrite_clause = "overwrite_or_ignore" if overwrite else "error_on_conflict"

        sql_query = f"""
        COPY (
            SELECT year(datetime at time zone 'UTC') as year, month(datetime at time zone 'UTC') as month, *, 
            FROM read_parquet('{source_pattern}', union_by_name=true)
        ) 
        TO '{dest_path}' (
            FORMAT parquet,
            COMPRESSION zstd,
            PARTITION_BY (year, month), 
            {overwrite_clause}
        )
        """

        # Execute the query
        conn.sql(sql_query)

    except Exception as e:
        raise RuntimeError(f"DuckDB operation failed: {e}")

    finally:
        conn.close()


def get_summary_stats(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Generate summary statistics from processing results.

    Args:
        results: List of result dictionaries from processing functions

    Returns:
        Dictionary with summary statistics
    """
    total_days = len(results)
    successful = sum(1 for r in results if r.get("success", False))
    skipped = sum(1 for r in results if r.get("skipped", False))
    failed = total_days - successful

    total_items = sum(
        r.get("stac_items_fetched", 0) for r in results if r.get("success", False)
    )

    failed_days = [
        {"date": r["date"], "error": r.get("error", "Unknown error")}
        for r in results
        if not r.get("success", False) and not r.get("skipped", False)
    ]

    return {
        "total_days": total_days,
        "successful": successful,
        "skipped": skipped,
        "failed": failed,
        "total_stac_items": total_items,
        "failed_days": failed_days,
        "success_rate": successful / total_days if total_days > 0 else 0.0,
    }

