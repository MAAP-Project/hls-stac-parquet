"""CLI commands for HLS STAC to parquet workflow."""

import asyncio
from functools import wraps

import typer

from hls_stac_parquet.links import cache_daily_stac_json_links
from hls_stac_parquet.write import write_monthly_stac_geoparquet

app = typer.Typer()


def async_command(func):
    """Decorator to convert async function to sync typer command."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        return asyncio.run(func(*args, **kwargs))

    return app.command()(wrapper)


# Register commands - signatures are automatically preserved
cache_daily_stac_json_links_cmd = async_command(cache_daily_stac_json_links)
write_monthly_stac_geoparquet_cmd = async_command(write_monthly_stac_geoparquet)


if __name__ == "__main__":
    app()
