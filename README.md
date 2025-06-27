# HLS STAC Cache

A Python package for querying NASA's CMR API and caching HLS (Harmonized Landsat Sentinel-2) STAC items as GeoParquet files using `rustac`.

## Installation

```bash
# Using pip
pip install git+https://github.com/MAAP-project/hls-stac-parquet
```

## Command Line Interface

The package provides a command-line interface for generating daily HLS STAC parquet files:

### Generate Daily Files

```bash
# Generate files for all days in May 2024
hls-stac-parquet generate-daily 202405 /path/to/output

# Generate files for specific date range (May 10-20, 2024)
hls-stac-parquet generate-daily 20240510 20240520 /path/to/output

# Generate file for single day
hls-stac-parquet generate-daily 20240515 /path/to/output

# Generate files in parallel mode (faster)
hls-stac-parquet generate-daily 202405 /path/to/output --parallel

# Customize concurrency settings
hls-stac-parquet generate-daily 202405 /path/to/output --parallel \
  --max-concurrent-days 5 --max-concurrent-per-day 100
```

### Repartition Daily Files

Repartition daily parquet files by year-month for better query performance:

```bash
# Repartition daily files into year-month partitions
hls-stac-parquet repartition /path/to/daily/files /path/to/partitioned/output

# Overwrite existing partitioned data
hls-stac-parquet repartition /path/to/daily/files /path/to/partitioned/output --overwrite
```

### Date Format Options

- `YYYYMM` - All days in the month (e.g., `202405` for May 2024)
- `YYYYMMDD` - Single day (e.g., `20240515` for May 15, 2024)
- Two `YYYYMMDD` - Date range (e.g., `20240510 20240520` for May 10-20, 2024)

### CLI Options

- `--parallel` - Process days in parallel (faster but more resource intensive)
- `--max-concurrent-days N` - Maximum number of days to process concurrently (default: 3)
- `--max-concurrent-per-day N` - Maximum concurrent requests per day (default: 50)
- `--no-skip-existing` - Process all days even if output files already exist
- `--no-progress` - Hide progress bars

## Python API

The package provides both a command-line interface and a Python API for programmatic usage.

### High-Level API

```python
import hls_stac_parquet

# Process a single day
results = hls_stac_parquet.process_date_range(
    start_date="2024-05-15",
    output_dir="./output"
)

# Process a date range in parallel
results = hls_stac_parquet.process_date_range(
    start_date="20240510",
    end_date="20240515",
    output_dir="./output",
    parallel=True
)

# Process an entire month
results = hls_stac_parquet.process_month(
    year_month="202405",
    output_dir="./output",
    parallel=True
)

# Get summary statistics
stats = hls_stac_parquet.get_summary_stats(results)
print(f"Success rate: {stats['success_rate']:.1%}")

# Repartition files by year-month
hls_stac_parquet.repartition_by_year_month(
    source_dir="./daily_files",
    destination_dir="./partitioned_files"
)
```

### Low-Level API

```python
"""Example usage of the low-level HLS STAC to Parquet conversion functions."""
import asyncio

from obstore.store import LocalStore

from hls_stac_parquet import hls_to_stac_geoparquet


async def main():
    stats = await hls_to_stac_geoparquet(
        output_path="hls_example.parquet",
        bounding_box=(-100, 40, -90, 50),
        temporal=("2024-06-01T00:00:00Z", "2024-06-02T00:00:00Z"),
        max_concurrent=50,
        show_progress=True,
        store=LocalStore(prefix="/tmp"),
    )

    print("Conversion completed!")
    print(f"Stats: {stats}")


if __name__ == "__main__":
    asyncio.run(main())
```

For detailed API documentation and examples, see [API_USAGE.md](API_USAGE.md).

## Development

```bash
git clone https://github.com/MAAP-project/hls-stac-parquet.git
cd hls-stac-parquet

uv sync
```

## Acknowledgments

- NASA's CMR API for providing access to HLS data
- The `rustac` library for efficient STAC GeoParquet writing
- The `obstore` library for high-performance object storage access
