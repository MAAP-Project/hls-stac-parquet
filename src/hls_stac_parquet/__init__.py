"""HLS STAC to Parquet conversion package."""

from .api import (
    process_date_range,
    process_month,
    repartition_by_year_month,
    get_summary_stats,
    parse_date_input,
    parse_month_input,
    generate_date_range,
    generate_month_dates,
    process_single_day,
    process_dates_sequential,
    process_dates_parallel,
)
from .cmr import collect_cmr_results, create_hls_query, extract_stac_json_links
from .fetch import fetch_stac_items, fetch_stac_items_batch
from .main import hls_to_stac_geoparquet

__all__ = [
    # High-level API functions
    "process_date_range",
    "process_month", 
    "repartition_by_year_month",
    "get_summary_stats",
    
    # Core processing functions
    "hls_to_stac_geoparquet",
    "process_single_day",
    "process_dates_sequential",
    "process_dates_parallel",
    
    # Utility functions
    "parse_date_input",
    "parse_month_input",
    "generate_date_range",
    "generate_month_dates",
    
    # Lower-level functions
    "create_hls_query",
    "collect_cmr_results",
    "extract_stac_json_links",
    "fetch_stac_items_batch",
    "fetch_stac_items",
]
