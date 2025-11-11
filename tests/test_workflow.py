import json
from datetime import datetime
from tempfile import TemporaryDirectory

import obstore
import pytest
from obstore.store import MemoryStore

from hls_stac_parquet._version import __version__
from hls_stac_parquet.cmr_api import HlsCollectionConceptId
from hls_stac_parquet.constants import PARQUET_PATH_FORMAT
from hls_stac_parquet.workflow import (
    _check_exists,
    cache_daily_stac_json_links,
    collect_stac_json_links,
    write_monthly_stac_geoparquet,
    write_stac_links,
)

TEST_BOUNDING_BOX = (-93, 46, -92, 47)


@pytest.mark.vcr
@pytest.mark.parametrize("collection_concept_id", list(HlsCollectionConceptId))
async def test_collect_stac_json_links(collection_concept_id: HlsCollectionConceptId):
    links = await collect_stac_json_links(
        collection_concept_id=collection_concept_id,
        bounding_box=TEST_BOUNDING_BOX,
        temporal=("2025-10-01T00:00:00Z", "2025-10-10T00:00:00Z"),
    )

    for link in links:
        assert link.path.endswith("stac.json")


@pytest.mark.vcr
async def test_write_stac_json_links():
    links = await collect_stac_json_links(
        collection_concept_id=HlsCollectionConceptId.HLSL30,
        bounding_box=TEST_BOUNDING_BOX,
        temporal=("2025-10-01T00:00:00Z", "2025-10-10T00:00:00Z"),
    )

    store = MemoryStore()

    await write_stac_links(links, store, "test.json")

    res = obstore.get(store, "test.json").bytes()

    assert set(json.loads(bytes(res).decode())) == set(
        [link.geturl() for link in links]
    )


@pytest.mark.vcr
async def test_cache_daily_stac_json_links():
    await cache_daily_stac_json_links(
        HlsCollectionConceptId.HLSL30,
        date=datetime(2025, 10, 2),
        dest="memory:///",
        bounding_box=TEST_BOUNDING_BOX,
    )

    assert _check_exists(
        MemoryStore,
        PARQUET_PATH_FORMAT.format(
            version=__version__,
            collection_id=HlsCollectionConceptId.HLSL30.collection_id,
            year="2025",
            month="10",
        ),
    )


@pytest.mark.vcr
async def test_write_monthly_stac_geoparquet():
    # tried using MemoryStore but it didn't work :/
    with TemporaryDirectory() as tempdir:
        await cache_daily_stac_json_links(
            HlsCollectionConceptId.HLSL30,
            date=datetime(2025, 10, 2),
            dest=f"file://{tempdir}",
            bounding_box=TEST_BOUNDING_BOX,
        )

        await write_monthly_stac_geoparquet(
            HlsCollectionConceptId.HLSL30,
            year=2025,
            month=10,
            dest=f"file://{tempdir}",
            require_complete_links=False,
        )

        with pytest.raises(ValueError, match="expected these links:"):
            await write_monthly_stac_geoparquet(
                HlsCollectionConceptId.HLSL30,
                year=2025,
                month=10,
                dest=f"file://{tempdir}",
                require_complete_links=True,
            )
