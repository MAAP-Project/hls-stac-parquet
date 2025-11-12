from enum import StrEnum

CLIENT_ID = "nasa-maap-hls-stac-geoparquet; contact henry@developmentseed.org"

LINK_PATH_PREFIX = "links/{collection_id}/{year}/{month}"
LINK_PATH_FORMAT = LINK_PATH_PREFIX + "/{year}-{month}-{day}.json"
PARQUET_PATH_FORMAT = (
    "{version}/{collection_id}/{year}/{month}/{collection_id}-{year}-{month}.parquet"
)


class HlsCollection(StrEnum):
    """HLS collection identifiers with associated concept IDs."""

    HLSL30 = "HLSL30"
    HLSS30 = "HLSS30"

    @property
    def concept_id(self) -> str:
        """Return the CMR concept ID for this collection."""
        concept_ids = {
            "HLSL30": "C2021957657-LPCLOUD",
            "HLSS30": "C2021957295-LPCLOUD",
        }
        return concept_ids[self.value]

    @property
    def collection_id(self) -> str:
        """Return the collection ID with _2.0 appended."""
        return f"{self.value}_2.0"
