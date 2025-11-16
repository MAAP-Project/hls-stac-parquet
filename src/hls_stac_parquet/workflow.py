"""Main integration functions for HLS STAC to parquet workflow."""

import json
import logging
import re
import urllib.parse
from datetime import datetime, timedelta
from typing import Annotated, List, Literal
from urllib.parse import ParseResult

import obstore
import rustac
import typer
from hilbertcurve.hilbertcurve import HilbertCurve
from mgrs import MGRS
from obstore.store import ObjectStore, from_url

from hls_stac_parquet._version import __version__
from hls_stac_parquet.cmr_api import (
    HlsCollection,
    collect_cmr_results,
    create_hls_query,
    extract_stac_json_links,
)
from hls_stac_parquet.constants import (
    LINK_PATH_FORMAT,
    LINK_PATH_PREFIX,
    PARQUET_PATH_FORMAT,
)
from hls_stac_parquet.fetch import fetch_stac_items

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

# logging.getLogger("httpx").setLevel("WARN")
logging.getLogger("stac_io").setLevel("WARN")

logger = logging.getLogger("hls-stac-geoparquet-archive")

# Initialize MGRS converter and Hilbert curve (14 bits = 16384x16384 grid)
_mgrs_converter = MGRS()
_hilbert_curve = HilbertCurve(14, 2)

# Regex pattern to extract MGRS tile ID from HLS STAC URLs
# Pattern: HLS.{SENSOR}.T{MGRS_TILE}.{DATE}.v{VERSION}
_MGRS_PATTERN = re.compile(r"\.T([0-9]{2}[A-Z]{3})\.")


def extract_mgrs_from_url(url: str) -> str | None:
    """
    Extract MGRS tile ID from HLS STAC JSON URL.

    Example URL:
    https://data.lpdaac.earthdatacloud.nasa.gov/lp-prod-public/HLSS30.020/
    HLS.S30.T60WWV.2025275T234641.v2.0/HLS.S30.T60WWV.2025275T234641.v2.0_stac.json

    Returns: "60WWV" or None if pattern not found
    """
    match = _MGRS_PATTERN.search(url)
    return match.group(1) if match else None


def mgrs_to_hilbert_index(mgrs_tile: str) -> int:
    """
    Convert MGRS tile ID to Hilbert curve index for spatial sorting.

    Args:
        mgrs_tile: MGRS tile identifier (e.g., "60WWV")

    Returns:
        Hilbert curve index (integer) for spatial ordering
    """
    try:
        # Convert MGRS tile to lat/lon coordinates
        # Use center of tile (60000m, 60000m offset in a ~110km tile)
        lat, lon = _mgrs_converter.toLatLon(mgrs_tile)

        # Normalize coordinates to grid space [0, 16384)
        # Longitude: -180 to +180 -> 0 to 16384
        # Latitude: -90 to +90 -> 0 to 16384
        x = int((lon + 180) / 360 * 16384)
        y = int((lat + 90) / 180 * 16384)

        # Clamp to valid range
        x = max(0, min(16383, x))
        y = max(0, min(16383, y))

        # Calculate Hilbert curve distance
        return _hilbert_curve.distance_from_point([x, y])
    except Exception as e:
        logger.warning(f"Failed to convert MGRS tile {mgrs_tile} to Hilbert index: {e}")
        # Return a large number to sort errors to the end
        return 2**28


async def _check_exists(store, path) -> bool:
    try:
        _ = await obstore.head_async(store, path)
        return True
    except FileNotFoundError:
        return False


async def collect_stac_json_links(
    collection: HlsCollection,
    bounding_box: tuple[float, float, float, float] | None = None,
    temporal: tuple[str, str] | None = None,
    protocol: Literal["s3", "https"] = "https",
) -> List[ParseResult]:
    query = create_hls_query(
        collection=collection,
        bounding_box=bounding_box,
        temporal=temporal,
    )

    cmr_results = await collect_cmr_results(query)

    return extract_stac_json_links(cmr_results, protocol=protocol)


async def write_stac_links(
    stac_links: List[ParseResult],
    store: ObjectStore,
    path: str,
) -> None:
    links = json.dumps([link.geturl() for link in stac_links]).encode()
    await obstore.put_async(store, path, links)


async def cache_daily_stac_json_links(
    collection: Annotated[
        HlsCollection, typer.Argument(help="HLS collection to query (HLSL30 or HLSS30)")
    ],
    date: Annotated[datetime, typer.Argument(help="Date to query for STAC items")],
    dest: Annotated[
        str,
        typer.Argument(
            help="Destination URL for storing cached links (e.g., s3://bucket/path)"
        ),
    ],
    bounding_box: Annotated[
        tuple[float, float, float, float] | None,
        typer.Option(
            help="Spatial bounding box as (min_lon, min_lat, max_lon, max_lat)"
        ),
    ] = None,
    protocol: Annotated[
        Literal["s3", "https"], typer.Option(help="Protocol to use for STAC JSON links")
    ] = "https",
    skip_existing: Annotated[
        bool, typer.Option(help="Skip processing if output file already exists")
    ] = False,
) -> None:
    """
    Cache daily STAC JSON links from CMR to object storage.

    Queries CMR for HLS STAC items on a specific date and writes the
    STAC JSON links to object storage for later retrieval.
    """
    store = from_url(dest)
    out_path = LINK_PATH_FORMAT.format(
        collection_id=collection.collection_id,
        year=date.year,
        month=date.month,
        day=date.day,
    )

    if skip_existing:
        if await _check_exists(store, out_path):
            logger.info(f"{dest}/{out_path} already exists ... skipping")
            return

    start_datetime = datetime(year=date.year, month=date.month, day=date.day)
    end_datetime = start_datetime + timedelta(days=1) - timedelta(seconds=1)

    stac_links = await collect_stac_json_links(
        collection=collection,
        bounding_box=bounding_box,
        temporal=(start_datetime.isoformat(), end_datetime.isoformat()),
        protocol=protocol,
    )

    await write_stac_links(
        stac_links=stac_links,
        store=store,
        path=out_path,
    )


async def write_monthly_stac_geoparquet(
    collection: Annotated[
        HlsCollection,
        typer.Argument(help="HLS collection to process (HLSL30 or HLSS30)"),
    ],
    yearmonth: Annotated[
        datetime,
        typer.Argument(help="Year and month to process (YYYY-MM-DD, day is ignored)"),
    ],
    dest: Annotated[
        str,
        typer.Argument(
            help="Destination URL for writing GeoParquet file (e.g., s3://bucket/path)"
        ),
    ],
    version: Annotated[
        str, typer.Option(help="Version string for output file path")
    ] = __version__,
    require_complete_links: Annotated[
        bool,
        typer.Option(
            help="Require all daily link files for the month to exist before processing"
        ),
    ] = False,
    skip_existing: Annotated[
        bool,
        typer.Option(help="Skip processing if output GeoParquet file already exists"),
    ] = False,
) -> None:
    """
    Write monthly STAC items to GeoParquet format.

    Collects cached STAC JSON links for a given month, fetches all STAC items,
    and writes them to a GeoParquet file in object storage.
    """
    store = from_url(dest)

    year = yearmonth.year
    month = yearmonth.month

    out_path = PARQUET_PATH_FORMAT.format(
        version=version,
        collection_id=collection.collection_id,
        year=str(year),
        month=str(month),
    )

    if skip_existing:
        if await _check_exists(store, out_path):
            logger.info(f"{out_path} found in {dest}... skipping")
            return

    stac_json_links = []
    stream = obstore.list(
        store,
        prefix=LINK_PATH_PREFIX.format(
            collection_id=collection.collection_id,
            year=year,
            month=month,
        ),
    )

    actual_links = []
    async for list_result in stream:
        for result in list_result:
            actual_links.append(result["path"])
            resp = await obstore.get_async(store, result["path"])
            buffer = await resp.bytes_async()
            links = json.loads(bytes(buffer).decode())
            stac_json_links.extend(links)

    logger.info(f"{collection.collection_id}: found {len(stac_json_links)} links")

    if require_complete_links:
        next_month = month + 1 if month < 12 else 1
        next_year = year if month < 12 else year + 1
        last_date_in_month = datetime(
            year=next_year, month=next_month, day=1
        ) - timedelta(days=1)

        # Handle partial first month if this is the origin month
        origin_date = collection.origin_date
        first_day = 1
        if year == origin_date.year and month == origin_date.month:
            first_day = origin_date.day
            logger.info(
                f"Origin month detected: expecting links starting from day {first_day}"
            )

        expected_links = [
            LINK_PATH_FORMAT.format(
                collection_id=collection.collection_id,
                year=year,
                month=month,
                day=day,
            )
            for day in range(first_day, last_date_in_month.day + 1)
        ]

        if not set(expected_links) == set(actual_links):
            raise ValueError(
                f"expected these links: \n{'\n'.join(expected_links)}\n",
                f"found these links:\n{'\n'.join(stac_json_links)}",
            )

    # Sort links by Hilbert curve for optimal spatial ordering
    logger.info(
        f"{collection.collection_id}: sorting {len(stac_json_links)} links by spatial order (Hilbert curve)"
    )

    def hilbert_sort_key(url: str) -> int:
        """Extract MGRS tile and convert to Hilbert index for sorting."""
        mgrs_tile = extract_mgrs_from_url(url)
        if mgrs_tile:
            return mgrs_to_hilbert_index(mgrs_tile)
        else:
            # If we can't extract MGRS, sort to end
            logger.warning(f"Could not extract MGRS tile from URL: {url}")
            return 2**28

    stac_json_links.sort(key=hilbert_sort_key)

    logger.info(f"{collection.collection_id}: loading stac items")
    stac_items, failed_links = await fetch_stac_items(
        [urllib.parse.urlparse(link) for link in stac_json_links],
        max_concurrent=50,
        show_progress=True,
    )
    if failed_links:
        logger.warning(f"failed to retrieve {len(failed_links)} items")

    for item in stac_items:
        item["collection"] = collection.collection_id

    _ = await rustac.write(
        out_path,
        stac_items,
        parquet_compression="zstd(6)",
        store=store,
    )

    logger.info(f"successfully wrote {len(stac_items)} items to {out_path}")
