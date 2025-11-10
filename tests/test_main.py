import json

import obstore
import pytest
from obstore.store import MemoryStore

from hls_stac_parquet.cmr_api import HlsCollectionConceptId
from hls_stac_parquet.main import collect_stac_json_links, write_stac_links


@pytest.mark.parametrize("collection_concept_id", list(HlsCollectionConceptId))
async def test_collect_stac_json_links(collection_concept_id: HlsCollectionConceptId):
    links = await collect_stac_json_links(
        collection_concept_id=collection_concept_id,
        bounding_box=(-93, 46, -92, 47),
        temporal=("2025-10-01T00:00:00Z", "2025-10-10T00:00:00Z"),
    )

    for link in links:
        assert link.path.endswith("stac.json")


async def test_write_stac_json_links():
    links = await collect_stac_json_links(
        collection_concept_id=HlsCollectionConceptId.HLSL30,
        bounding_box=(-93, 46, -92, 47),
        temporal=("2025-10-01T00:00:00Z", "2025-10-10T00:00:00Z"),
    )

    store = MemoryStore()

    write_stac_links(links, store, "test.json")

    res = obstore.get(store, "test.json").bytes()
    assert set(json.loads(bytes(res).decode())) == set(
        [link.geturl() for link in links]
    )
