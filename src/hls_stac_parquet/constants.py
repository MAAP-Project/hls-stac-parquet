from enum import StrEnum

CLIENT_ID = "nasa-maap-hls-stac-geoparquet; contact henry@developmentseed.org"

LINK_PATH_PREFIX = "links/{collection_id}/{year}/{month}"
LINK_PATH_FORMAT = LINK_PATH_PREFIX + "/{year}-{month}-{day}.json"
PARQUET_PATH_FORMAT = (
    "{version}/{collection_id}/{year}/{month}/{collection_id}-{year}-{month}.parquet"
)


class HlsCollectionConceptId(StrEnum):
    HLSL30 = "C2021957657-LPCLOUD"
    HLSS30 = "C2021957295-LPCLOUD"

    @property
    def collection_id(self) -> str:
        """Return the collection ID with _2.0 appended."""
        return f"{self.name}_2.0"
