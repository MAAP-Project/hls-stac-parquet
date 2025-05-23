# HLS STAC Cache

A Python package for querying NASA's CMR API and caching HLS (Harmonized Landsat Sentinel-2) STAC items as GeoParquet files using `rustac`.

## Installation

```bash
# Using pip
pip install git+https://github.com/MAAP-project/hls-stac-parquet
```

## Quick Start

### Python API

```python

"""Example usage of the HLS STAC to Parquet conversion functions."""
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
