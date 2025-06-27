"""CMR query functions for HLS data."""

from typing import Any, AsyncGenerator, Dict, List
from urllib.parse import ParseResult, urlparse

import httpx
from cmr import GranuleQuery


def create_hls_query(
    bounding_box: tuple[float, float, float, float] | None = None,
    temporal: tuple[str, str] | None = None,
) -> GranuleQuery:
    """Create a CMR granule query for HLS data.

    Args:
        collection_concept_ids: List of collection concept IDs. Defaults to HLS collections.
        bounding_box: Tuple of (west, south, east, north) coordinates
        temporal: Tuple of (start_date, end_date) in ISO format

    Returns:
        Configured GranuleQuery object
    """

    query = GranuleQuery().collection_concept_id(
        [
            "C2021957657-LPCLOUD",  # HLSL30
            "C2021957295-LPCLOUD",  # HLSS30
        ]
    )

    if bounding_box:
        query = query.bounding_box(*bounding_box)

    if temporal:
        query = query.temporal(*temporal)

    return query.format("json")


async def get_cmr_results_async(
    query: GranuleQuery, page_size: int = 2000
) -> AsyncGenerator[Dict[str, Any], None]:
    """Async generator that yields CMR granule results.

    Args:
        query: CMR GranuleQuery object
        page_size: Number of results per page (max 2000)

    Yields:
        Individual granule result dictionaries
    """
    page_size = min(max(1, page_size), 2000)
    url = query._build_url() + f"&page_size={page_size}"
    headers = dict(query.headers or {})

    async with httpx.AsyncClient(timeout=None) as client:
        while True:
            response = await client.get(url, headers=headers)
            response.raise_for_status()

            data = response.json()
            for entry in data["feed"]["entry"]:
                yield entry

            if not (cmr_search_after := response.headers.get("cmr-search-after")):
                break

            headers["cmr-search-after"] = cmr_search_after


def extract_stac_json_links(
    results: List[Dict[str, Any]], prefix: str = "https"
) -> List[ParseResult]:
    """Extract STAC JSON links from CMR results.

    Args:
        results: List of CMR granule result dictionaries
        prefix: URL scheme prefix to filter by

    Returns:
        List of parsed STAC JSON URLs
    """
    stac_links = []
    for result in results:
        try:
            # Skip results without links or with malformed links
            if "links" not in result or not isinstance(result["links"], list):
                continue

            url = next(
                link["href"]
                for link in result["links"]
                if isinstance(link, dict)
                and "href" in link
                and link["href"].endswith("stac.json")
                and link["href"].startswith(prefix)
            )
            stac_links.append(urlparse(url))
        except (StopIteration, KeyError, TypeError):
            # Skip results without STAC JSON links or malformed data
            continue
    return stac_links


async def collect_cmr_results(
    query: GranuleQuery, page_size: int = 2000
) -> List[Dict[str, Any]]:
    """Collect all CMR results into a list.

    Args:
        query: CMR GranuleQuery object
        page_size: Number of results per page

    Returns:
        List of all granule result dictionaries
    """
    results = []
    async for result in get_cmr_results_async(query, page_size):
        results.append(result)

    return results
