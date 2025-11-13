#!/bin/bash
set -e

# Get job type as first parameter
JOB_TYPE=$1

# Validate job type
if [ -z "$JOB_TYPE" ]; then
    echo "ERROR: JOB_TYPE (first parameter) is required"
    echo "Valid values: cache-daily, write-monthly"
    exit 1
fi

echo "=== HLS STAC Parquet Batch Job ==="
echo "Job Type: $JOB_TYPE"
echo "=================================="

case "$JOB_TYPE" in
    cache-daily)
        # Parameters for cache-daily-stac-json-links
        COLLECTION=$2
        DATE=$3
        DEST=$4
        BOUNDING_BOX=$5
        PROTOCOL=${6:-s3}
        SKIP_EXISTING=${7:-true}

        # Validate required parameters
        if [ -z "$COLLECTION" ]; then
            echo "ERROR: COLLECTION parameter is required"
            exit 1
        fi

        if [ -z "$DATE" ]; then
            echo "ERROR: DATE parameter is required (format: YYYY-MM-DD)"
            exit 1
        fi

        if [ -z "$DEST" ]; then
            echo "ERROR: DEST parameter is required (S3 URL)"
            exit 1
        fi

        # Build command
        CMD="/app/.venv/bin/hls-stac-parquet cache-daily-stac-json-links $COLLECTION $DATE $DEST"

        # Add optional parameters
        if [ -n "$BOUNDING_BOX" ] && [ "$BOUNDING_BOX" != "none" ]; then
            CMD="$CMD --bounding-box $BOUNDING_BOX"
        fi

        CMD="$CMD --protocol $PROTOCOL"

        if [ "$SKIP_EXISTING" = "true" ]; then
            CMD="$CMD --skip-existing"
        fi

        echo "Collection: $COLLECTION"
        echo "Date: $DATE"
        echo "Destination: $DEST"
        echo "Bounding Box: ${BOUNDING_BOX:-none}"
        echo "Protocol: $PROTOCOL"
        echo "Skip Existing: $SKIP_EXISTING"
        echo "Command: $CMD"
        echo "================================="

        exec $CMD
        ;;

    write-monthly)
        # Parameters for write-monthly-stac-geoparquet
        COLLECTION=$2
        YEAR_MONTH=$3
        DEST=$4
        VERSION=$5
        REQUIRE_COMPLETE_LINKS=${6:-false}
        SKIP_EXISTING=${7:-true}

        # Validate required parameters
        if [ -z "$COLLECTION" ]; then
            echo "ERROR: COLLECTION parameter is required"
            exit 1
        fi

        if [ -z "$YEAR_MONTH" ]; then
            echo "ERROR: YEAR_MONTH parameter is required (format: YYYY-MM-DD)"
            exit 1
        fi

        if [ -z "$DEST" ]; then
            echo "ERROR: DEST parameter is required (S3 URL)"
            exit 1
        fi

        # Build command
        CMD="/app/.venv/bin/hls-stac-parquet write-monthly-stac-geoparquet $COLLECTION $YEAR_MONTH $DEST"

        # Add optional parameters
        if [ -n "$VERSION" ] && [ "$VERSION" != "none" ]; then
            CMD="$CMD --version $VERSION"
        fi

        if [ "$REQUIRE_COMPLETE_LINKS" = "true" ]; then
            CMD="$CMD --require-complete-links"
        fi

        if [ "$SKIP_EXISTING" = "true" ]; then
            CMD="$CMD --skip-existing"
        fi

        echo "Collection: $COLLECTION"
        echo "Year-Month: $YEAR_MONTH"
        echo "Destination: $DEST"
        echo "Version: ${VERSION:-default}"
        echo "Require Complete Links: $REQUIRE_COMPLETE_LINKS"
        echo "Skip Existing: $SKIP_EXISTING"
        echo "Command: $CMD"
        echo "================================="

        exec $CMD
        ;;

    *)
        echo "ERROR: Invalid JOB_TYPE '$JOB_TYPE'"
        echo "Valid values: cache-daily, write-monthly"
        exit 1
        ;;
esac
