#!/usr/bin/env python3
"""
AWS Batch job script for generating daily HLS parquet files for a month,
combining them into a single monthly file, and uploading to S3 with hive partitioning.

Usage:
    python aws_batch_monthly.py --year-month 202401 --s3-bucket my-bucket --s3-prefix hls-data [--parallel]

Environment variables:
    AWS_DEFAULT_REGION: AWS region (optional, defaults to us-east-1)
    MAX_CONCURRENT_DAYS: Maximum concurrent days to process (optional, defaults to 3)
    MAX_CONCURRENT_PER_DAY: Maximum concurrent requests per day (optional, defaults to 50)
    TEMP_DIR: Base directory for temporary processing files (optional, defaults to /tmp/hls-processing)
"""

import argparse
import logging
import os
import sys
import tempfile
from pathlib import Path

import duckdb

# Import the HLS processing functions
from hls_stac_parquet.api import get_summary_stats, process_month


def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)

    return logging.getLogger(__name__)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate monthly HLS parquet files and upload to S3"
    )
    parser.add_argument(
        "--year-month",
        required=True,
        help="Year and month in YYYYMM format (e.g., 202401)",
    )
    parser.add_argument("--s3-bucket", required=True, help="S3 bucket name for upload")
    parser.add_argument(
        "--s3-prefix",
        default="hls-data",
        help="S3 prefix/key prefix (default: hls-data)",
    )
    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Process days in parallel (default: sequential)",
    )
    parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Don't clean up temporary files after processing",
    )
    return parser.parse_args()


def write_monthly_to_s3(
    daily_dir: Path,
    bucket: str,
    s3_prefix: str,
    year: int,
    month: int,
    logger: logging.Logger,
) -> bool:
    """Write daily parquet files to S3 as a single monthly file using DuckDB."""
    try:
        # Find parquet files in daily directory
        parquet_files = list(daily_dir.glob("*.parquet"))
        if not parquet_files:
            logger.error(f"No parquet files found in {daily_dir}")
            return False

        logger.info(f"Found {len(parquet_files)} daily parquet files")

        # Connect to DuckDB
        conn = duckdb.connect()

        # Install and load httpfs extension for S3 support
        conn.sql("INSTALL httpfs")
        conn.sql("LOAD httpfs")
        conn.sql("INSTALL spatial")
        conn.sql("LOAD spatial")
        conn.sql("CREATE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN)")

        # Build the source pattern and S3 destination
        source_pattern = str(daily_dir / "*.parquet")
        s3_path = (
            f"s3://{bucket}/{s3_prefix}/year={year}/month={month:02d}/data.parquet"
        )

        logger.info(f"Writing monthly file to: {s3_path}")

        # Create the DuckDB query to copy all daily files to S3
        sql_query = f"""
        COPY (
            SELECT * FROM read_parquet('{source_pattern}', union_by_name=true)
            ORDER BY datetime
        ) 
        TO '{s3_path}' (
            FORMAT parquet,
            COMPRESSION zstd
        )
        """

        logger.info("Executing DuckDB query to write to S3...")
        conn.sql(sql_query)

        conn.close()
        logger.info(f"Successfully wrote monthly file to {s3_path}")
        return True

    except Exception as e:
        logger.error(f"Failed to write monthly file to S3: {e}")
        return False


def main():
    """Main processing function."""
    logger = setup_logging()
    args = parse_arguments()

    # Parse year/month
    try:
        year_month = args.year_month
        if len(year_month) != 6 or not year_month.isdigit():
            raise ValueError("Invalid format")
        year = int(year_month[:4])
        month = int(year_month[4:6])
        if not (1 <= month <= 12):
            raise ValueError("Invalid month")
    except ValueError:
        logger.error(f"Invalid year-month format: {args.year_month}. Expected YYYYMM")
        return 1

    logger.info(f"Processing data for {year}-{month:02d}")

    # Use TemporaryDirectory context manager for automatic cleanup
    temp_dir_base = os.getenv("TEMP_DIR", "/tmp/hls-processing")

    with tempfile.TemporaryDirectory(
        dir=temp_dir_base, prefix="hls-monthly-"
    ) as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        daily_dir = temp_dir / "daily"

        # Create directories
        daily_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Using temporary directory: {temp_dir}")

        try:
            # Get processing parameters from environment
            max_concurrent_days = int(os.getenv("MAX_CONCURRENT_DAYS", "4"))
            max_concurrent_per_day = int(os.getenv("MAX_CONCURRENT_PER_DAY", "10"))

            logger.info(f"Processing daily files for {year}-{month:02d}")
            logger.info(
                f"Concurrent days: {max_concurrent_days}, Concurrent per day: {max_concurrent_per_day}"
            )
            logger.info(f"Parallel processing: {args.parallel}")

            # Process all days in the month
            results = process_month(
                year_month=(year, month),
                output_dir=daily_dir,
                parallel=args.parallel,
                max_concurrent_days=max_concurrent_days,
                max_concurrent_per_day=max_concurrent_per_day,
                show_progress=False,
                skip_existing=False,
                output_filename_pattern="hls_{date}.parquet",
            )

            # Log summary statistics
            stats = get_summary_stats(results)
            logger.info(
                f"Processing complete: {stats['successful']}/{stats['total_days']} days successful"
            )
            logger.info(f"Total STAC items processed: {stats['total_stac_items']}")

            if stats["failed"] > 0:
                logger.warning(f"{stats['failed']} days failed:")
                for failed_day in stats["failed_days"]:
                    logger.warning(f"  {failed_day['date']}: {failed_day['error']}")

            # Check if we have any successful daily files
            parquet_files = list(daily_dir.glob("*.parquet"))
            if not parquet_files:
                logger.error("No parquet files were generated successfully")
                return 1

            logger.info(f"Generated {len(parquet_files)} daily parquet files")

            # Write monthly file directly to S3 using DuckDB
            if not write_monthly_to_s3(
                daily_dir, args.s3_bucket, args.s3_prefix, year, month, logger
            ):
                return 1

            logger.info(f"Successfully completed processing for {year}-{month:02d}")

            return 0

        except Exception as e:
            logger.error(f"Processing failed: {e}")
            return 1

        finally:
            # Log cleanup info - TemporaryDirectory will handle actual cleanup
            if not args.no_cleanup:
                logger.info(
                    f"Temporary directory will be automatically cleaned up: {temp_dir}"
                )
            else:
                logger.info(
                    f"--no-cleanup specified, but TemporaryDirectory will still clean up: {temp_dir}"
                )


if __name__ == "__main__":
    main()
