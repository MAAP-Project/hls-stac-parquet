"""Main integration functions for HLS STAC to parquet workflow."""

import json
import logging
import urllib.parse
from datetime import datetime, timedelta
from typing import List, Literal
from urllib.parse import ParseResult

import obstore
import rustac
from obstore.store import ObjectStore, from_url

from hls_stac_parquet._version import __version__
from hls_stac_parquet.cmr_api import (
    HlsCollectionConceptId,
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

logger = logging.getLogger()


async def _check_exists(store, path) -> bool:
    try:
        _ = await obstore.head_async(store, path)
        return True
    except FileNotFoundError:
        return False


async def collect_stac_json_links(
    collection_concept_id: HlsCollectionConceptId,
    bounding_box: tuple[float, float, float, float] | None = None,
    temporal: tuple[str, str] | None = None,
    protocol: Literal["s3", "https"] = "https",
) -> List[ParseResult]:
    query = create_hls_query(
        collection_concept_id=collection_concept_id,
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
    collection_concept_id: HlsCollectionConceptId,
    date: datetime,
    dest: str,
    bounding_box: tuple[float, float, float, float] | None = None,
    protocol: Literal["s3", "https"] = "https",
    skip_existing: bool = False,
) -> None:
    store = from_url(dest)
    out_path = LINK_PATH_FORMAT.format(
        collection_id=collection_concept_id.collection_id,
        year=str(date.year),
        month=str(date.month),
        day=str(date.day),
    )

    if skip_existing:
        if await _check_exists(store, out_path):
            logger.info(f"{out_path} found in {dest}... skipping")
            return

    start_datetime = datetime(year=date.year, month=date.month, day=date.day)
    end_datetime = start_datetime + timedelta(days=1) - timedelta(seconds=1)

    stac_links = await collect_stac_json_links(
        collection_concept_id=collection_concept_id,
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
    collection_concept_id: HlsCollectionConceptId,
    year: int,
    month: int,
    dest: str,
    version: str = __version__,
    require_complete_links: bool = False,
    skip_existing: bool = False,
) -> None:
    store = from_url(dest)

    out_path = PARQUET_PATH_FORMAT.format(
        version=version,
        collection_id=collection_concept_id.collection_id,
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
            collection_id=collection_concept_id.collection_id,
            year=str(year),
            month=str(month),
        ),
    )

    async for list_result in stream:
        for result in list_result:
            resp = await obstore.get_async(store, result["path"])
            buffer = await resp.bytes_async()
            links = json.loads(bytes(buffer).decode())
            stac_json_links.extend(links)

    logger.info(
        f"{collection_concept_id.collection_id}: found {len(stac_json_links)} links"
    )

    if require_complete_links:
        last_date_in_month = datetime(year=year, month=(month + 1), day=1) - timedelta(
            days=1
        )
        expected_links = [
            LINK_PATH_FORMAT.format(
                collection_id=collection_concept_id.collection_id,
                year=str(year),
                month=str(month),
                day=str(day),
            )
            for day in range(1, last_date_in_month.day + 1)
        ]

        if not set(expected_links) == set(stac_json_links):
            raise ValueError(
                f"expected these links: \n{'\n'.join(expected_links)}\n",
                f"found these links:\n{'\n'.join(stac_json_links)}",
            )

    logger.info(f"{collection_concept_id.collection_id}: loading stac items")
    stac_items, failed_links = await fetch_stac_items(
        [urllib.parse.urlparse(link) for link in stac_json_links],
        max_concurrent=50,
        show_progress=True,
    )
    if failed_links:
        logger.warning(f"failed to retrieve {len(failed_links)} items")

    for item in stac_items:
        item["collection"] = collection_concept_id.collection_id

    _ = await rustac.write(
        out_path,
        stac_items,
        parquet_compression="zstd(6)",
        store=store,
    )
