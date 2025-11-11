"""Async STAC item fetching functions."""

import asyncio
import json
from typing import Any, Dict, List
from urllib.parse import ParseResult

import obstore as obs
import tqdm.asyncio
from obstore.store import from_url


async def fetch_stac_items(
    stac_links: List[ParseResult], max_concurrent: int = 50, show_progress: bool = True
) -> tuple[List[Dict[str, Any]], List[ParseResult]]:
    """Fetch STAC items and return both successful items and failed links.

    Args:
        stac_links: List of parsed STAC JSON URLs
        max_concurrent: Maximum number of concurrent requests
        show_progress: Whether to show progress bar

    Returns:
        Tuple of (successful_items, failed_links)
    """
    if not stac_links:
        return [], []

    # Group by netloc to create stores efficiently
    stores_by_netloc = {}
    for link in stac_links:
        netloc = link.netloc
        if netloc not in stores_by_netloc:
            stores_by_netloc[netloc] = from_url(f"{link.scheme}://{netloc}")

    semaphore = asyncio.Semaphore(max_concurrent)

    async def fetch_with_error_handling(
        link: ParseResult,
    ) -> tuple[Dict[str, Any] | None, ParseResult | None]:
        async with semaphore:
            try:
                store = stores_by_netloc[link.netloc]
                item_data = await obs.get_async(store, link.path)
                item_bytes = await item_data.bytes_async()
                item = json.loads(item_bytes.to_bytes().decode("utf-8"))

                return item, None
            except Exception as e:
                print(f"Failed to fetch {link.geturl()}: {e}")
                return None, link

    # Execute fetches concurrently
    tasks = [fetch_with_error_handling(link) for link in stac_links]

    if show_progress:
        results = await tqdm.asyncio.tqdm.gather(
            *tasks, desc="Fetching STAC items", total=len(tasks)
        )
    else:
        results = await asyncio.gather(*tasks)

    # Separate successful items from failed links
    successful_items = []
    failed_links = []

    for item, failed_link in results:
        if item is not None:
            successful_items.append(item)
        if failed_link is not None:
            failed_links.append(failed_link)

    return successful_items, failed_links
