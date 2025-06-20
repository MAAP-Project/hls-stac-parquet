"""Command-line interface for HLS STAC to Parquet conversion."""

import argparse
import asyncio
import sys
from pathlib import Path

from .api import (
    parse_date_input,
    parse_month_input,
    generate_date_range,
    generate_month_dates,
    process_dates_sequential,
    process_dates_parallel,
    repartition_by_year_month,
    get_summary_stats,
)


def print_verbose_processing_info(
    dates, output_dir, parallel, max_concurrent_days, skip_existing
):
    """Print detailed processing information for verbose mode."""
    print(f"Processing {len(dates)} days")
    print(f"Output directory: {output_dir.absolute()}")
    print(f"Mode: {'Parallel' if parallel else 'Sequential'}")
    if parallel:
        print(f"Max concurrent days: {max_concurrent_days}")
    print(f"Skip existing: {skip_existing}")
    print(f"Total days to process: {len(dates)}")


def print_processing_progress(i, total, date_string):
    """Print processing progress for sequential mode."""
    print(f"\nProgress: {i}/{total} days - {date_string}")


def print_day_result(result):
    """Print result information for a single day."""
    if result.get("skipped"):
        print(f"Skipped {result['date']} - file already exists")
    elif result.get("success"):
        print(f"Completed {result['date']}: {result.get('stac_items_fetched', 0)} items")
        # Show file size if available
        if "output_path" in result:
            output_path = Path(result["output_path"])
            if output_path.exists():
                file_size = output_path.stat().st_size
                print(f"File size: {file_size:,} bytes ({file_size / (1024 * 1024):.1f} MB)")
    else:
        print(f"Error processing {result['date']}: {result.get('error', 'Unknown error')}")


def print_summary(results):
    """Print a summary of processing results."""
    stats = get_summary_stats(results)
    
    print("\nSummary:")
    print(f"   Total days: {stats['total_days']}")
    print(f"   Successful: {stats['successful']}")
    print(f"   Skipped: {stats['skipped']}")
    print(f"   Failed: {stats['failed']}")
    print(f"   Total STAC items: {stats['total_stac_items']}")
    print(f"   Success rate: {stats['success_rate']:.1%}")

    if stats['failed'] > 0:
        print("\nFailed days:")
        for failed_day in stats['failed_days']:
            print(f"   - {failed_day['date']}: {failed_day['error']}")


def print_repartition_info(source_dir, destination_dir, overwrite):
    """Print repartitioning information."""
    parquet_files = list(source_dir.glob("*.parquet"))
    print(f"Found {len(parquet_files)} parquet files in {source_dir}")
    print(f"Repartitioning data to {destination_dir}")
    print(f"Overwrite mode: {overwrite}")
    print()


def print_repartition_results(destination_dir):
    """Print repartitioning results."""
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
    except Exception as e:
        print(f"Error creating output directory: {e}")
        return 1

    skip_existing = not args.no_skip_existing
    show_progress = not args.no_progress

    print_verbose_processing_info(
        dates_to_process, output_dir, args.parallel, args.max_concurrent_days, skip_existing
    )

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
        stats = get_summary_stats(results)
        return 1 if stats['failed'] > 0 else 0

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
        print_repartition_info(source_dir, destination_dir, args.overwrite)

        repartition_by_year_month(
            source_dir=source_dir,
            destination_dir=destination_dir,
            overwrite=args.overwrite,
        )

        print_repartition_results(destination_dir)
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
