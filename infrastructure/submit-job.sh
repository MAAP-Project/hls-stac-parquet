#!/bin/bash

# HLS Batch Job Submission Helper Script
# This script helps submit jobs to the AWS Batch environment

set -e

# Default values
JOB_TYPE=""
COLLECTION=""
DATE=""
YEAR_MONTH=""
DEST=""
BOUNDING_BOX=""
PROTOCOL="s3"
VERSION=""
REQUIRE_COMPLETE_LINKS="true"
SKIP_EXISTING="true"
JOB_NAME=""
STACK_NAME="HlsBatchStack"
AWS_REGION=""
DRY_RUN="false"

# Function to display usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Submit HLS processing jobs to AWS Batch

Required Options:
    -t, --type TYPE             Job type: cache-daily or write-monthly
    -c, --collection COLLECTION HLS collection: HLSL30 or HLSS30

Required for cache-daily (one of):
    --date YYYY-MM-DD           Single date to process
    --year-month YYYY-MM        Process entire month (submits one job per day)

Required for write-monthly:
    --year-month YYYY-MM        Year and month to process

Optional Options:
    -d, --dest DEST             Destination S3 URL (default: from stack's bucket)
    -r, --region REGION         AWS region (default: from AWS CLI config)
    -n, --name NAME             Job name (default: auto-generated)
    -s, --stack STACK           CDK stack name (default: HlsBatchStack)
    --bounding-box "W,S,E,N"    Spatial bounding box (for cache-daily only)
    --protocol PROTOCOL         Protocol for STAC links: s3 or https (default: s3)
    --version VERSION           Version string for output path (write-monthly only)
    --require-complete-links    Require all daily files before processing (write-monthly)
    --no-skip-existing          Process even if output file exists
    --dry-run                   Show command without executing
    -h, --help                  Show this help message

Examples:
    # Cache daily STAC links for a single date (using stack's default bucket)
    $0 -t cache-daily -c HLSL30 --date 2024-01-15

    # Cache entire month of daily STAC links (one job per day)
    $0 -t cache-daily -c HLSL30 --year-month 2024-01

    # Cache with custom destination
    $0 -t cache-daily -c HLSL30 --date 2024-01-15 -d s3://my-bucket/data

    # Cache with bounding box for California
    $0 -t cache-daily -c HLSL30 --date 2024-01-15 \\
       --bounding-box "-124.7,32.5,-114.1,42.0"

    # Write monthly parquet for January 2024 (using stack's default bucket)
    $0 -t write-monthly -c HLSL30 --year-month 2024-01

    # Write monthly with specific version
    $0 -t write-monthly -c HLSS30 --year-month 2024-01 --version v1.2.3

    # Dry run to see the command
    $0 -t cache-daily -c HLSL30 --date 2024-01-15 --dry-run

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--type)
            JOB_TYPE="$2"
            shift 2
            ;;
        -c|--collection)
            COLLECTION="$2"
            shift 2
            ;;
        --date)
            DATE="$2"
            shift 2
            ;;
        --year-month)
            YEAR_MONTH="$2"
            shift 2
            ;;
        -d|--dest)
            DEST="$2"
            shift 2
            ;;
        --bounding-box)
            BOUNDING_BOX="$2"
            shift 2
            ;;
        --protocol)
            PROTOCOL="$2"
            shift 2
            ;;
        --version)
            VERSION="$2"
            shift 2
            ;;
        --require-complete-links)
            REQUIRE_COMPLETE_LINKS="true"
            shift
            ;;
        --no-skip-existing)
            SKIP_EXISTING="false"
            shift
            ;;
        -r|--region)
            AWS_REGION="$2"
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
if [[ -z "$JOB_TYPE" ]]; then
    echo "Error: Job type is required (-t/--type)"
    usage
    exit 1
fi

if [[ "$JOB_TYPE" != "cache-daily" && "$JOB_TYPE" != "write-monthly" ]]; then
    echo "Error: Job type must be 'cache-daily' or 'write-monthly'"
    usage
    exit 1
fi

if [[ -z "$COLLECTION" ]]; then
    echo "Error: Collection is required (-c/--collection)"
    usage
    exit 1
fi

if [[ "$COLLECTION" != "HLSL30" && "$COLLECTION" != "HLSS30" ]]; then
    echo "Error: Collection must be 'HLSL30' or 'HLSS30'"
    usage
    exit 1
fi

# DEST will be set later if not provided (from CloudFormation stack outputs)

# Validate job-type specific arguments
if [[ "$JOB_TYPE" == "cache-daily" ]]; then
    if [[ -z "$DATE" && -z "$YEAR_MONTH" ]]; then
        echo "Error: Either --date or --year-month is required for cache-daily jobs"
        usage
        exit 1
    fi

    if [[ -n "$DATE" && -n "$YEAR_MONTH" ]]; then
        echo "Error: Cannot specify both --date and --year-month for cache-daily jobs"
        usage
        exit 1
    fi

    # Validate date format (YYYY-MM-DD)
    if [[ -n "$DATE" ]]; then
        if ! date -d "$DATE" >/dev/null 2>&1; then
            echo "Error: Invalid date format. Use YYYY-MM-DD"
            exit 1
        fi
    fi

    # Validate year-month format (YYYY-MM)
    if [[ -n "$YEAR_MONTH" ]]; then
        if [[ ! "$YEAR_MONTH" =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
            echo "Error: Invalid year-month format. Use YYYY-MM"
            exit 1
        fi
        # Validate it's a valid date by appending -01
        if ! date -d "${YEAR_MONTH}-01" >/dev/null 2>&1; then
            echo "Error: Invalid year-month value: $YEAR_MONTH"
            exit 1
        fi
    fi
fi

if [[ "$JOB_TYPE" == "write-monthly" ]]; then
    if [[ -z "$YEAR_MONTH" ]]; then
        echo "Error: Year-month is required for write-monthly jobs (--year-month)"
        usage
        exit 1
    fi

    # Validate year-month format (YYYY-MM)
    if [[ ! "$YEAR_MONTH" =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
        echo "Error: Invalid year-month format. Use YYYY-MM"
        exit 1
    fi
    # Validate it's a valid date by appending -01
    if ! date -d "${YEAR_MONTH}-01" >/dev/null 2>&1; then
        echo "Error: Invalid year-month value: $YEAR_MONTH"
        exit 1
    fi
fi

# Generate job name prefix if not provided (actual names will be generated per job)
if [[ -z "$JOB_NAME" ]]; then
    if [[ "$JOB_TYPE" == "cache-daily" ]]; then
        if [[ -n "$DATE" ]]; then
            JOB_NAME="hls-cache-daily-${COLLECTION}-${DATE}-$(date +%Y%m%d-%H%M%S)"
        else
            # For month batches, just use base name (individual job names will append date)
            JOB_NAME_PREFIX="hls-cache-daily-${COLLECTION}-${YEAR_MONTH}"
        fi
    else
        JOB_NAME="hls-write-monthly-${COLLECTION}-${YEAR_MONTH}-$(date +%Y%m%d-%H%M%S)"
    fi
fi

# Get AWS region
if [[ -z "$AWS_REGION" ]]; then
    AWS_REGION=$(aws configure get region 2>/dev/null || echo "us-east-1")
fi

echo "Retrieving job queue and definition from CloudFormation stack..."

# Get job queue name based on job type
if [[ "$JOB_TYPE" == "cache-daily" ]]; then
    QUEUE_OUTPUT_KEY="CacheDailyJobQueueArn"
else
    QUEUE_OUTPUT_KEY="WriteMonthlyJobQueueArn"
fi

JOB_QUEUE=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION" \
    --query "Stacks[0].Outputs[?OutputKey==\`$QUEUE_OUTPUT_KEY\`].OutputValue" \
    --output text | cut -d'/' -f2)

if [[ -z "$JOB_QUEUE" ]]; then
    echo "Error: Could not retrieve job queue from stack $STACK_NAME"
    echo "Make sure the stack is deployed and you have access to it"
    exit 1
fi

# Get bucket name if DEST is not provided
if [[ -z "$DEST" ]]; then
    BUCKET_NAME=$(aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$AWS_REGION" \
        --query 'Stacks[0].Outputs[?OutputKey==`BucketName`].OutputValue' \
        --output text)

    if [[ -z "$BUCKET_NAME" ]]; then
        echo "Error: Could not retrieve bucket name from stack $STACK_NAME and no destination provided"
        echo "Please provide a destination using -d/--dest"
        exit 1
    fi

    DEST="s3://${BUCKET_NAME}"
    echo "Using default destination from stack: $DEST"
fi

# Get job definition name based on job type
if [[ "$JOB_TYPE" == "cache-daily" ]]; then
    OUTPUT_KEY="CacheDailyJobDefinitionArn"
else
    OUTPUT_KEY="WriteMonthlyJobDefinitionArn"
fi

JOB_DEFINITION=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION" \
    --query "Stacks[0].Outputs[?OutputKey==\`$OUTPUT_KEY\`].OutputValue" \
    --output text | cut -d'/' -f2 | cut -d':' -f1)

if [[ -z "$JOB_DEFINITION" ]]; then
    echo "Error: Could not retrieve job definition from stack $STACK_NAME"
    exit 1
fi

# Function to submit a single job
submit_job() {
    local job_name="$1"
    local job_date="$2"  # For cache-daily, or year-month-01 for write-monthly
    local parameters=""

    if [[ "$JOB_TYPE" == "cache-daily" ]]; then
        parameters="jobType=cache-daily,collection=$COLLECTION,date=$job_date,dest=$DEST,protocol=$PROTOCOL,skipExisting=$SKIP_EXISTING"

        if [[ -n "$BOUNDING_BOX" ]]; then
            parameters="$parameters,boundingBox=$BOUNDING_BOX"
        else
            parameters="$parameters,boundingBox=none"
        fi
    else
        # write-monthly
        parameters="jobType=write-monthly,collection=$COLLECTION,yearMonth=$job_date,dest=$DEST,requireCompleteLinks=$REQUIRE_COMPLETE_LINKS,skipExisting=$SKIP_EXISTING"

        if [[ -n "$VERSION" ]]; then
            parameters="$parameters,version=$VERSION"
        else
            parameters="$parameters,version=none"
        fi
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  Job: $job_name"
        echo "  Parameters: $parameters"
        return 0
    fi

    local job_id=$(aws batch submit-job \
        --job-name "$job_name" \
        --job-queue "$JOB_QUEUE" \
        --job-definition "$JOB_DEFINITION" \
        --parameters "$parameters" \
        --region "$AWS_REGION" \
        --query 'jobId' \
        --output text)

    if [[ $? -eq 0 && -n "$job_id" ]]; then
        echo "  $job_date: Job ID $job_id"
        return 0
    else
        echo "  $job_date: Failed to submit"
        return 1
    fi
}

# Display job configuration
echo ""
echo "Job Configuration:"
echo "  Job Type: $JOB_TYPE"
echo "  Collection: $COLLECTION"
if [[ "$JOB_TYPE" == "cache-daily" ]]; then
    if [[ -n "$DATE" ]]; then
        echo "  Date: $DATE"
    else
        echo "  Year-Month: $YEAR_MONTH (will submit one job per day)"
    fi
    echo "  Protocol: $PROTOCOL"
    if [[ -n "$BOUNDING_BOX" ]]; then
        echo "  Bounding Box: $BOUNDING_BOX"
    fi
else
    echo "  Year-Month: $YEAR_MONTH"
    if [[ -n "$VERSION" ]]; then
        echo "  Version: $VERSION"
    fi
    echo "  Require Complete Links: $REQUIRE_COMPLETE_LINKS"
fi
echo "  Destination: $DEST"
echo "  Skip Existing: $SKIP_EXISTING"
echo "  Job Queue: $JOB_QUEUE"
echo "  Job Definition: $JOB_DEFINITION"
echo "  Region: $AWS_REGION"
echo ""

if [[ "$DRY_RUN" == "true" ]]; then
    echo "Dry run - Jobs that would be submitted:"
    echo ""
fi

# Submit jobs
if [[ "$JOB_TYPE" == "cache-daily" && -n "$YEAR_MONTH" ]]; then
    # Submit one job per day in the month
    echo "Submitting cache-daily jobs for month $YEAR_MONTH..."

    # Get number of days in the month
    YEAR="${YEAR_MONTH%-*}"
    MONTH="${YEAR_MONTH#*-}"
    LAST_DAY=$(date -d "${YEAR_MONTH}-01 +1 month -1 day" +%d)

    TOTAL_JOBS=$LAST_DAY
    SUBMITTED_JOBS=0
    FAILED_JOBS=0


    for day in $(seq -w 1 $LAST_DAY); do
        current_date="${YEAR_MONTH}-${day}"
        if [[ -n "$JOB_NAME" ]]; then
            current_job_name="${JOB_NAME}-${day}"
        else
            current_job_name="${JOB_NAME_PREFIX}-${day}-$(date +%Y%m%d-%H%M%S)"
        fi

        if submit_job "$current_job_name" "$current_date"; then
            SUBMITTED_JOBS=$((SUBMITTED_JOBS + 1))
        else
            FAILED_JOBS=$((FAILED_JOBS + 1))
        fi
    done

    echo ""
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "Dry run complete. Would submit $TOTAL_JOBS jobs."
    else
        echo "Batch submission complete!"
        echo "  Total: $TOTAL_JOBS jobs"
        echo "  Submitted: $SUBMITTED_JOBS"
        echo "  Failed: $FAILED_JOBS"
        echo ""
        echo "Monitor your jobs:"
        echo "  AWS Console: https://$AWS_REGION.console.aws.amazon.com/batch/home?region=$AWS_REGION#jobs"
        echo "  AWS CLI: aws batch list-jobs --job-queue $JOB_QUEUE --region $AWS_REGION"
    fi

    if [[ $FAILED_JOBS -gt 0 ]]; then
        exit 1
    fi
else
    # Single job submission
    if [[ "$JOB_TYPE" == "cache-daily" ]]; then
        JOB_DATE="$DATE"
    else
        # For write-monthly, append -01 to the year-month
        JOB_DATE="${YEAR_MONTH}-01"
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        submit_job "$JOB_NAME" "$JOB_DATE"
        exit 0
    fi

    echo "Submitting job to AWS Batch..."
    if submit_job "$JOB_NAME" "$JOB_DATE"; then
        echo ""
        echo "Job submitted successfully!"
        echo ""
        echo "Monitor your job:"
        echo "  AWS Console: https://$AWS_REGION.console.aws.amazon.com/batch/home?region=$AWS_REGION#jobs"
        echo "  AWS CLI: aws batch describe-jobs --jobs \$(aws batch list-jobs --job-queue $JOB_QUEUE --region $AWS_REGION --query 'jobSummaryList[0].jobId' --output text) --region $AWS_REGION"
        echo ""
        echo "View logs (once job starts):"
        echo "  Log Group: /aws/batch/hls-stac-parquet"
        echo "  AWS CLI: aws logs tail /aws/batch/hls-stac-parquet --follow --region $AWS_REGION"
    else
        echo "Failed to submit job"
        exit 1
    fi
fi
