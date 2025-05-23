"""Main integration functions for HLS STAC to parquet workflow."""

from pathlib import Path
from typing import Any, Dict

import rustac

from .cmr import collect_cmr_results, create_hls_query, extract_stac_json_links
from .fetch import fetch_stac_items


async def hls_to_stac_geoparquet(
    output_path: str | Path,
    bounding_box: tuple[float, float, float, float] | None = None,
    temporal: tuple[str, str] | None = None,
    max_concurrent: int = 50,
    show_progress: bool = True,
    **rustac_kwargs,
) -> Dict[str, Any]:
    """Complete workflow: Query CMR, fetch STAC items, write to parquet.

    Args:
        output_path: Path to output parquet file
        bounding_box: Tuple of (west, south, east, north) coordinates
        temporal: Tuple of (start_date, end_date) in ISO format
        max_concurrent: Maximum concurrent STAC item fetches
        show_progress: Whether to show progress bars
        **rustac_kwargs: Additional arguments for rustac.write()

    Returns:
        Dictionary with processing statistics
    """
    stats = {}

    print("Creating CMR query...")
    query = create_hls_query(
        bounding_box=bounding_box,
        temporal=temporal,
    )

    print(f"Query will return {query.hits()} results")
    stats["total_cmr_results"] = query.hits()

    print("Collecting CMR results...")
    cmr_results = await collect_cmr_results(query)
    stats["collected_cmr_results"] = len(cmr_results)

    print("Extracting STAC JSON links...")
    stac_links = extract_stac_json_links(cmr_results)
    stats["stac_links_found"] = len(stac_links)

    if not stac_links:
        raise ValueError("No STAC JSON links found in CMR results")

    print(f"Fetching {len(stac_links)} STAC items...")
    stac_items, failed_links = await fetch_stac_items(
        stac_links, max_concurrent=max_concurrent, show_progress=show_progress
    )

    stats["stac_items_fetched"] = len(stac_items)
    stats["stac_items_failed"] = len(failed_links)

    if not stac_items:
        raise ValueError("No STAC items could be fetched")

    print(f"Writing {len(stac_items)} STAC items to {output_path}...")
    await rustac.write(str(output_path), stac_items, **rustac_kwargs)

    stats["output_path"] = str(output_path)
    stats["success"] = True

    print(f"Successfully wrote {len(stac_items)} STAC items to {output_path}")
    return stats
