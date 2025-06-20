#!/bin/bash
set -e

YEAR=$1

# Loop through all months of the provided year
for month in {01..12}; do
    yearMonth="${YEAR}${month}"
    
    echo "Submitting job for ${yearMonth}..."
    
    aws batch submit-job \
        --job-name "hls-processing-${yearMonth}-$(date +%Y%m%d-%H%M%S)" \
        --job-queue HlsBatchJobQueue594F3C3C-gLa2KZ6p02yOHT4L \
        --job-definition hls-stac-parquet-monthly \
        --parameters "yearMonth=${yearMonth},s3Bucket=hls-stac-geoparquet,s3Prefix=v1"
    
    sleep 1
done

