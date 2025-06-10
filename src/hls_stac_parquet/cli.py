"""Command-line interface for HLS STAC to Parquet conversion."""

import argparse
import asyncio
import calendar
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple

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
) -> dict:
    """Process a single day's data and write to parquet file.

    Args:
        date: Date to process
        output_dir: Output directory for parquet files
        max_concurrent: Maximum concurrent requests
        show_progress: Whether to show progress bars
        skip_existing: Whether to skip if output file already exists

    Returns:
        Dictionary with processing statistics
    """
    date_string = date.strftime("%Y-%m-%d")

    # Create output filename based on the date
    output_filename = f"hls_{date.strftime('%Y%m%d')}.parquet"
    output_path = output_dir / output_filename

    # Skip if file exists and skip_existing is True
    if skip_existing and output_path.exists():
        print(f"Skipping {date_string} - file already exists: {output_path}")
        return {
            "date": date_string,
            "skipped": True,
            "output_path": str(output_path),
            "success": True,
        }

    # Create temporal range for the specified date
    start_time = f"{date_string}T00:00:00Z"
    end_time = f"{date_string}T23:59:59Z"

    print(f"Processing {date_string} ({start_time} to {end_time})")
    print(f"Output: {output_path}")

    try:
        stats = await hls_to_stac_geoparquet(
            output_path=output_filename,
            temporal=(start_time, end_time),
            max_concurrent=max_concurrent,
            show_progress=show_progress,
            store=LocalStore(prefix=str(output_dir)),
        )

        stats["date"] = date_string
        stats["skipped"] = False
        print(f"Completed {date_string}: {stats.get('stac_items_fetched', 0)} items")

        # Show file size if it exists
        if output_path.exists():
            file_size = output_path.stat().st_size
            print(
                f"File size: {file_size:,} bytes ({file_size / (1024 * 1024):.1f} MB)"
            )

        return stats

    except Exception as e:
        print(f"Error processing {date_string}: {e}")
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
) -> List[dict]:
    """Process multiple dates sequentially.

    Args:
        dates: List of dates to process
        output_dir: Output directory for parquet files
        max_concurrent: Maximum concurrent requests per day
        show_progress: Whether to show progress bars
        skip_existing: Whether to skip existing files

    Returns:
        List of statistics dictionaries for each day
    """
    results = []

    print(f"Processing {len(dates)} days sequentially")

    for i, date in enumerate(dates, 1):
        print(f"\nProgress: {i}/{len(dates)} days")

        result = await process_single_day(
            date=date,
            output_dir=output_dir,
            max_concurrent=max_concurrent,
            show_progress=show_progress,
            skip_existing=skip_existing,
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
) -> List[dict]:
    """Process multiple dates in parallel.

    Args:
        dates: List of dates to process
        output_dir: Output directory for parquet files
        max_concurrent_days: Maximum number of days to process concurrently
        max_concurrent_per_day: Maximum concurrent requests per day
        show_progress: Whether to show progress bars
        skip_existing: Whether to skip existing files

    Returns:
        List of statistics dictionaries for each day
    """
    print(f"Processing {len(dates)} days in parallel")
    print(f"Max concurrent days: {max_concurrent_days}")

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


def print_summary(results: List[dict]):
    """Print a summary of processing results."""
    total_days = len(results)
    successful = sum(1 for r in results if r.get("success", False))
    skipped = sum(1 for r in results if r.get("skipped", False))
    failed = total_days - successful

    total_items = sum(
        r.get("stac_items_fetched", 0) for r in results if r.get("success", False)
    )

    print("\nSummary:")
    print(f"   Total days: {total_days}")
    print(f"   Successful: {successful}")
    print(f"   Skipped: {skipped}")
    print(f"   Failed: {failed}")
    print(f"   Total STAC items: {total_items}")

    if failed > 0:
        print("\nFailed days:")
        for result in results:
            if not result.get("success", False) and not result.get("skipped", False):
                print(f"   - {result['date']}: {result.get('error', 'Unknown error')}")


def repartition_by_year_month(
    source_dir: Path, destination_dir: Path, overwrite: bool = False
):
    """Repartition daily parquet files by year-month.

    Args:
        source_dir: Directory containing daily parquet files
        destination_dir: Directory to write repartitioned files to
        overwrite: Whether to overwrite existing partitioned data

    Raises:
        ValueError: If source directory doesn't exist or has no parquet files
        RuntimeError: If DuckDB operation fails
    """
    if not source_dir.exists():
        raise ValueError(f"Source directory does not exist: {source_dir}")

    # Find parquet files in source directory
    parquet_files = list(source_dir.glob("*.parquet"))
    if not parquet_files:
        raise ValueError(f"No parquet files found in source directory: {source_dir}")

    print(f"Found {len(parquet_files)} parquet files in {source_dir}")
    print(f"Repartitioning data to {destination_dir}")

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
            PARTITION_BY (year, month), 
            {overwrite_clause}
        )
        """

        print("Executing DuckDB query:")
        print(sql_query)
        print()

        # Execute the query
        result = conn.sql(sql_query)

        print("Repartitioning completed successfully!")

        # Show some statistics about the partitioned data
        partitioned_files = list(destination_dir.rglob("*.parquet"))
        print(f"Created {len(partitioned_files)} partitioned files")

        # Group partitioned files by year/month to show structure
        partition_structure = {}
        for file_path in partitioned_files:
            # Extract year and month from path structure
            parts = file_path.relative_to(destination_dir).parts
            if (
                len(parts) >= 2
                and parts[0].startswith("year=")
                and parts[1].startswith("month=")
            ):
                year = parts[0].split("=")[1]
                month = parts[1].split("=")[1]
                partition_key = f"{year}-{month.zfill(2)}"
                partition_structure[partition_key] = (
                    partition_structure.get(partition_key, 0) + 1
                )

        if partition_structure:
            print("\nPartition structure:")
            for partition, file_count in sorted(partition_structure.items()):
                print(f"  {partition}: {file_count} files")

    except Exception as e:
        raise RuntimeError(f"DuckDB operation failed: {e}")

    finally:
        conn.close()


async def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Generate daily HLS STAC parquet files for specified date ranges",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate files for all days in May 2024
  hls-stac-parquet generate-daily 202405 /path/to/output

  # Generate files for specific date range
  hls-stac-parquet generate-daily 20240510 20240520 /path/to/output

  # Generate file for single day
  hls-stac-parquet generate-daily 20240515 /path/to/output

  # Generate files in parallel mode
  hls-stac-parquet generate-daily 202405 /path/to/output --parallel
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # generate-daily subcommand
    daily_parser = subparsers.add_parser(
        "generate-daily",
        help="Generate daily parquet files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Date format options:
  YYYYMM     - All days in the month (e.g., 202405 for May 2024)
  YYYYMMDD   - Single day (e.g., 20240515 for May 15, 2024)
  Two dates  - Date range (e.g., 20240510 20240520 for May 10-20, 2024)
        """,
    )

    daily_parser.add_argument(
        "dates",
        nargs="+",
        help="Date specification: YYYYMM for full month, YYYYMMDD for single day, or two YYYYMMDD for range",
    )

    daily_parser.add_argument(
        "output_dir", help="Output directory path (will be created if it doesn't exist)"
    )

    daily_parser.add_argument(
        "--parallel",
        action="store_true",
        help="Process days in parallel (faster but more resource intensive)",
    )

    daily_parser.add_argument(
        "--max-concurrent-days",
        type=int,
        default=3,
        help="Maximum number of days to process concurrently (parallel mode only, default: 3)",
    )

    daily_parser.add_argument(
        "--max-concurrent-per-day",
        type=int,
        default=50,
        help="Maximum concurrent requests per day (default: 50)",
    )

    daily_parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Process all days even if output files already exist",
    )

    daily_parser.add_argument(
        "--no-progress", action="store_true", help="Hide progress bars"
    )

    # repartition subcommand
    repartition_parser = subparsers.add_parser(
        "repartition",
        help="Repartition daily parquet files by year-month",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Repartition daily files into year-month partitions
  hls-stac-parquet repartition /path/to/daily/files /path/to/partitioned/output

  # Overwrite existing partitioned data
  hls-stac-parquet repartition /path/to/daily/files /path/to/partitioned/output --overwrite
        """,
    )

    repartition_parser.add_argument(
        "source_dir", help="Source directory containing daily parquet files"
    )

    repartition_parser.add_argument(
        "destination_dir", help="Destination directory for partitioned files"
    )

    repartition_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing partitioned data (default: error if partitions exist)",
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    if args.command == "generate-daily":
        return await handle_generate_daily(args)
    elif args.command == "repartition":
        return handle_repartition(args)

    return 0


async def handle_generate_daily(args):
    """Handle the generate-daily command."""
    # Parse date inputs
    dates_to_process = []

    try:
        if len(args.dates) == 1:
            date_input = args.dates[0]

            # Check if it's a month (YYYYMM format)
            if len(date_input) == 6 and date_input.isdigit():
                year, month = parse_month_input(date_input)
                dates_to_process = generate_month_dates(year, month)
                print(f"Processing all days in {year}-{month:02d}")
            else:
                # Single day
                single_date = parse_date_input(date_input)
                dates_to_process = [single_date]
                print(f"Processing single day: {single_date.strftime('%Y-%m-%d')}")

        elif len(args.dates) == 2:
            # Date range
            start_date = parse_date_input(args.dates[0])
            end_date = parse_date_input(args.dates[1])

            if start_date > end_date:
                print("Error: Start date must be before or equal to end date")
                return 1

            dates_to_process = generate_date_range(start_date, end_date)
            print(
                f"Processing date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            )

        else:
            print("Error: Expected 1 or 2 date arguments")
            return 1

    except ValueError as e:
        print(f"Error parsing dates: {e}")
        return 1

    # Ensure output directory exists
    try:
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        print(f"Output directory: {output_dir.absolute()}")
    except Exception as e:
        print(f"Error creating output directory: {e}")
        return 1

    skip_existing = not args.no_skip_existing
    show_progress = not args.no_progress

    print(f"Mode: {'Parallel' if args.parallel else 'Sequential'}")
    print(f"Skip existing: {skip_existing}")
    print(f"Total days to process: {len(dates_to_process)}")

    try:
        if args.parallel:
            results = await process_dates_parallel(
                dates=dates_to_process,
                output_dir=output_dir,
                max_concurrent_days=args.max_concurrent_days,
                max_concurrent_per_day=args.max_concurrent_per_day,
                show_progress=show_progress,
                skip_existing=skip_existing,
            )
        else:
            results = await process_dates_sequential(
                dates=dates_to_process,
                output_dir=output_dir,
                max_concurrent=args.max_concurrent_per_day,
                show_progress=show_progress,
                skip_existing=skip_existing,
            )

        print_summary(results)

        # Determine exit code
        failed_count = sum(1 for r in results if not r.get("success", False))
        return 1 if failed_count > 0 else 0

    except Exception as e:
        print(f"Fatal error during processing: {e}")
        return 1


def handle_repartition(args):
    """Handle the repartition command."""
    try:
        source_dir = Path(args.source_dir)
        destination_dir = Path(args.destination_dir)

        print(f"Source directory: {source_dir.absolute()}")
        print(f"Destination directory: {destination_dir.absolute()}")
        print(f"Overwrite mode: {args.overwrite}")
        print()

        repartition_by_year_month(
            source_dir=source_dir,
            destination_dir=destination_dir,
            overwrite=args.overwrite,
        )

        return 0

    except (ValueError, RuntimeError) as e:
        print(f"Error: {e}")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 1


def cli_main():
    """Entry point for the CLI."""
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)
