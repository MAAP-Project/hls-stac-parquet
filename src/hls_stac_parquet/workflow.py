"""Main integration functions for HLS STAC to parquet workflow."""

import json
import logging
import urllib.parse
from datetime import datetime, timedelta
from typing import Annotated, List, Literal
from urllib.parse import ParseResult

import obstore
import rustac
import typer
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

    if not stac_links:
        raise ValueError("CMR query returned no STAC links:", collection, date)

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
        expected_links = [
            LINK_PATH_FORMAT.format(
                collection_id=collection.collection_id,
                year=year,
                month=month,
                day=day,
            )
            for day in range(1, last_date_in_month.day + 1)
        ]

        if not set(expected_links) == set(actual_links):
            raise ValueError(
                f"expected these links: \n{'\n'.join(expected_links)}\n",
                f"found these links:\n{'\n'.join(stac_json_links)}",
            )

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

    # Sort by datetime for better query performance and compression
    stac_items.sort(key=lambda item: item.get("properties", {}).get("datetime"))

    _ = await rustac.write(
        out_path,
        stac_items,
        parquet_compression="zstd(6)",
        store=store,
    )

    logger.info(f"successfully wrote {len(stac_items)} items to {out_path}")
