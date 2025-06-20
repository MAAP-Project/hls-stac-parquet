#!/bin/bash
set -e

# Get parameters directly from environment variables (now that they're mapped)
YEAR_MONTH=$1
S3_BUCKET=$2
S3_PREFIX=$3
PARALLEL=$4

# Validate required parameters
if [ -z "$YEAR_MONTH" ]; then
    echo "ERROR: yearMonth parameter is required"
    echo "Available environment: $(env | grep -E '^(yearMonth|AWS_BATCH)' || echo 'none')"
    exit 1
fi

if [ -z "$S3_BUCKET" ]; then
    echo "ERROR: s3Bucket parameter is required"
    echo "Available environment: $(env | grep -E '^(s3Bucket|AWS_BATCH)' || echo 'none')"
    exit 1
fi

# Build command line arguments
ARGS="--year-month $YEAR_MONTH --s3-bucket $S3_BUCKET --s3-prefix $S3_PREFIX"

if [ "$PARALLEL" = "true" ]; then
    ARGS="$ARGS --parallel"
fi

echo "=== Executing with parameters ==="
echo "Year-Month: $YEAR_MONTH"
echo "S3 Bucket: $S3_BUCKET"
echo "S3 Prefix: $S3_PREFIX"
echo "Parallel: $PARALLEL"
echo "Command: uv run aws_batch_monthly.py $ARGS"
echo "================================="

uv run aws_batch_monthly.py $ARGS
