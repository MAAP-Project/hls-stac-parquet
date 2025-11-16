"""Parameter validation functions for CLI and API."""

from typing import Annotated


def validate_bbox(
    bbox: tuple[float, float, float, float],
) -> tuple[float, float, float, float]:
    """Validate bounding box coordinates.

    Args:
        bbox: Tuple of (min_lon, min_lat, max_lon, max_lat)

    Returns:
        The validated bbox tuple

    Raises:
        ValueError: If coordinates are invalid
    """
    min_lon, min_lat, max_lon, max_lat = bbox

    if not -180 <= min_lon <= 180:
        raise ValueError(f"min_lon must be between -180 and 180, got {min_lon}")

    if not -180 <= max_lon <= 180:
        raise ValueError(f"max_lon must be between -180 and 180, got {max_lon}")

    if not -90 <= min_lat <= 90:
        raise ValueError(f"min_lat must be between -90 and 90, got {min_lat}")

    if not -90 <= max_lat <= 90:
        raise ValueError(f"max_lat must be between -90 and 90, got {max_lat}")

    if min_lon >= max_lon:
        raise ValueError(f"min_lon ({min_lon}) must be less than max_lon ({max_lon})")

    if min_lat >= max_lat:
        raise ValueError(f"min_lat ({min_lat}) must be less than max_lat ({max_lat})")

    return bbox


BBox = Annotated[tuple[float, float, float, float], validate_bbox]
