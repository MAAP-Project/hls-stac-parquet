#!/bin/bash

# HLS Batch Job Submission Helper Script
# This script helps submit jobs to the AWS Batch environment

set -e

# Default values
YEAR_MONTH=""
S3_BUCKET=""
S3_PREFIX="hls-data"
BOUNDING_BOX=""
PARALLEL="false"
JOB_NAME=""
STACK_NAME="HlsBatchStack"

# Function to display usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Submit HLS processing jobs to AWS Batch

Required Options:
    -y, --year-month YYYYMM     Year and month to process (e.g., 202401)
    -b, --bucket BUCKET         S3 bucket name for output

Optional Options:
    -p, --prefix PREFIX         S3 prefix for output (default: hls-data)
    -r, --region REGION         AWS region (default: from AWS CLI config)
    -n, --name NAME             Job name (default: auto-generated)
    -s, --stack STACK           CDK stack name (default: HlsBatchStack)
    --bounding-box "W,S,E,N"    Spatial bounding box (west,south,east,north)
    --parallel                  Enable parallel processing
    --dry-run                   Show command without executing
    -h, --help                  Show this help message

Examples:
    # Process January 2024 data
    $0 -y 202401 -b my-hls-bucket

    # Process with bounding box for California
    $0 -y 202401 -b my-hls-bucket --bounding-box "-124.7,32.5,-114.1,42.0"

    # Process with parallel processing enabled
    $0 -y 202401 -b my-hls-bucket --parallel

    # Dry run to see the command
    $0 -y 202401 -b my-hls-bucket --dry-run

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -y|--year-month)
            YEAR_MONTH="$2"
            shift 2
            ;;
        -b|--bucket)
            S3_BUCKET="$2"
            shift 2
            ;;
        -p|--prefix)
            S3_PREFIX="$2"
            shift 2
            ;;
        -r|--region)
            export AWS_DEFAULT_REGION="$2"
            shift 2
            ;;
        -n|--name)
            JOB_NAME="$2"
            shift 2
            ;;
        -s|--stack)
            STACK_NAME="$2"
            shift 2
            ;;
        --bounding-box)
            BOUNDING_BOX="$2"
            shift 2
            ;;
        --parallel)
            PARALLEL="true"
            shift
            ;;
        --dry-run)
            DRY_RUN="true"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Validate required arguments
if [[ -z "$YEAR_MONTH" ]]; then
    echo "❌ Error: Year-month is required (-y/--year-month)"
    usage
    exit 1
fi

if [[ -z "$S3_BUCKET" ]]; then
    echo "❌ Error: S3 bucket is required (-b/--bucket)"
    usage
    exit 1
fi

# Validate year-month format
if [[ ! "$YEAR_MONTH" =~ ^[0-9]{6}$ ]]; then
    echo "❌ Error: Year-month must be in YYYYMM format (e.g., 202401)"
    exit 1
fi

# Extract year and month for validation
YEAR=${YEAR_MONTH:0:4}
MONTH=${YEAR_MONTH:4:2}

if [[ $YEAR -lt 2000 || $YEAR -gt 2099 ]]; then
    echo "❌ Error: Year must be between 2000 and 2099"
    exit 1
fi

if [[ $MONTH -lt 1 || $MONTH -gt 12 ]]; then
    echo "❌ Error: Month must be between 01 and 12"
    exit 1
fi

# Generate job name if not provided
if [[ -z "$JOB_NAME" ]]; then
    JOB_NAME="hls-processing-${YEAR_MONTH}-$(date +%Y%m%d-%H%M%S)"
fi

# Get AWS region
AWS_REGION=${AWS_DEFAULT_REGION:-$(aws configure get region)}
if [[ -z "$AWS_REGION" ]]; then
    AWS_REGION="us-east-1"
fi

echo "🔍 Retrieving job queue and definition from CloudFormation stack..."

# Get job queue name
JOB_QUEUE=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION" \
    --query 'Stacks[0].Outputs[?OutputKey==`JobQueueArn`].OutputValue' \
    --output text | cut -d'/' -f2)

if [[ -z "$JOB_QUEUE" ]]; then
    echo "❌ Error: Could not retrieve job queue from stack $STACK_NAME"
    echo "   Make sure the stack is deployed and you have access to it"
    exit 1
fi

# Get job definition name
JOB_DEFINITION=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION" \
    --query 'Stacks[0].Outputs[?OutputKey==`JobDefinitionArn`].OutputValue' \
    --output text | cut -d'/' -f2 | cut -d':' -f1)

if [[ -z "$JOB_DEFINITION" ]]; then
    echo "❌ Error: Could not retrieve job definition from stack $STACK_NAME"
    exit 1
fi

# Build parameters
PARAMETERS="yearMonth=$YEAR_MONTH,s3Bucket=$S3_BUCKET,s3Prefix=$S3_PREFIX,parallel=$PARALLEL"

if [[ -n "$BOUNDING_BOX" ]]; then
    PARAMETERS="$PARAMETERS,boundingBox=$BOUNDING_BOX"
fi

# Build the AWS CLI command
CMD="aws batch submit-job \
    --job-name \"$JOB_NAME\" \
    --job-queue \"$JOB_QUEUE\" \
    --job-definition \"$JOB_DEFINITION\" \
    --parameters \"$PARAMETERS\" \
    --region \"$AWS_REGION\""

echo ""
echo "📋 Job Configuration:"
echo "   Job Name: $JOB_NAME"
echo "   Year-Month: $YEAR_MONTH"
echo "   S3 Bucket: $S3_BUCKET"
echo "   S3 Prefix: $S3_PREFIX"
echo "   Job Queue: $JOB_QUEUE"
echo "   Job Definition: $JOB_DEFINITION"
echo "   Parallel Processing: $PARALLEL"
if [[ -n "$BOUNDING_BOX" ]]; then
    echo "   Bounding Box: $BOUNDING_BOX"
fi
echo ""

if [[ "$DRY_RUN" == "true" ]]; then
    echo "🔍 Dry run - Command that would be executed:"
    echo "$CMD"
    exit 0
fi

echo "🚀 Submitting job to AWS Batch..."
JOB_ID=$(eval $CMD --query 'jobId' --output text)

if [[ $? -eq 0 && -n "$JOB_ID" ]]; then
    echo "✅ Job submitted successfully!"
    echo "   Job ID: $JOB_ID"
    echo ""
    echo "📊 Monitor your job:"
    echo "   AWS Console: https://$AWS_REGION.console.aws.amazon.com/batch/home?region=$AWS_REGION#jobs/detail/$JOB_ID"
    echo "   AWS CLI: aws batch describe-jobs --jobs $JOB_ID --region $AWS_REGION"
    echo ""
    echo "📋 View logs (once job starts):"
    echo "   Log Group: /aws/batch/hls-stac-parquet"
    echo "   AWS CLI: aws logs describe-log-streams --log-group-name /aws/batch/hls-stac-parquet --region $AWS_REGION"
else
    echo "❌ Failed to submit job"
    exit 1
fi