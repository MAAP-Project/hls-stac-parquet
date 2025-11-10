"""Main integration functions for HLS STAC to parquet workflow."""

import json
import logging
from pathlib import Path
from typing import List
from urllib.parse import ParseResult

import obstore
import rustac
from obstore.store import ObjectStore

from .cmr_api import (
    HlsCollectionConceptId,
    collect_cmr_results,
    create_hls_query,
    extract_stac_json_links,
)
from .fetch import fetch_stac_items

logger = logging.getLogger()


async def collect_stac_json_links(
    collection_concept_id: HlsCollectionConceptId,
    bounding_box: tuple[float, float, float, float] | None = None,
    temporal: tuple[str, str] | None = None,
) -> List[ParseResult]:
    query = create_hls_query(
        collection_concept_id=collection_concept_id,
        bounding_box=bounding_box,
        temporal=temporal,
    )

    cmr_results = await collect_cmr_results(query)

    return extract_stac_json_links(cmr_results)


def write_stac_links(
    stac_links: List[ParseResult],
    store: ObjectStore,
    path: str,
) -> None:
    links = json.dumps([link.geturl() for link in stac_links]).encode()
    obstore.put(store, path, links)


async def write_stac_geoparquet(
    stac_links: List[ParseResult],
    output_path: str | Path,
    max_concurrent: int = 50,
    show_progress: bool = True,
    **rustac_kwargs,
) -> None:
    """Complete workflow: Query CMR, fetch STAC items, write to parquet.

    Args:
        output_path: Path to output parquet file
        bounding_box: Tuple of (west, south, east, north) coordinates
        temporal: Tuple of (start_date, end_date) in ISO format
        max_concurrent: Maximum concurrent STAC item fetches
        show_progress: Whether to show progress bars
        **rustac_kwargs: Additional arguments for rustac.write()
    """

    stac_items, failed_links = await fetch_stac_items(
        stac_links, max_concurrent=max_concurrent, show_progress=show_progress
    )

    if not stac_items:
        raise ValueError("No STAC items could be fetched")

    await rustac.write(str(output_path), stac_items, **rustac_kwargs)

    logger.info(f"Successfully wrote {len(stac_items)} STAC items to {output_path}")
