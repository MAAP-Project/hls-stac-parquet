#!/usr/bin/env python3
"""Example usage of the HLS STAC to Parquet conversion functions."""

import asyncio

from obstore.store import LocalStore

from hls_stac_parquet import hls_to_stac_geoparquet


async def main():
    """Example usage of the HLS STAC to Parquet conversion."""

    # Example 1: Simple conversion for a small dataset
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
