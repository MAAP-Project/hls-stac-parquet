"""Lightweight Lambda handler for batch publishing SNS messages for date ranges."""

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Collection-specific origin dates (when data starts being available)
COLLECTION_ORIGIN_DATES = {
    "HLSL30": "2013-04-11",  # Landsat 8 launch + HLS processing start
    "HLSS30": "2015-11-28",  # Sentinel-2A launch + HLS processing start
}


def handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    """
    Lambda handler for batch publishing SNS messages for date ranges.

    This function publishes SNS messages for every day in a specified date range,
    triggering the cache-daily Lambda function for each date.

    Expected event format (JSON):
    {
        "collection": "HLSL30" or "HLSS30",
        "start_date": "YYYY-MM-DD",  # optional, defaults to collection origin date
        "end_date": "YYYY-MM-DD",    # optional, defaults to yesterday
        "dest": "s3://bucket/path",  # optional, defaults to BUCKET_NAME env var
        "bounding_box": [min_lon, min_lat, max_lon, max_lat],  # optional
        "protocol": "s3" or "https",  # optional, default "s3"
        "skip_existing": true or false  # optional, default true
    }

    Environment Variables:
    - TOPIC_ARN: SNS topic ARN to publish to (required)
    - BUCKET_NAME: S3 bucket name (used if "dest" not provided in message)

    Returns:
        dict: Response with count of published messages and summary
    """
    logger.info(f"Batch publisher invoked with event: {json.dumps(event)}")

    # Get and validate required parameters
    topic_arn = os.environ.get("TOPIC_ARN")
    if not topic_arn:
        raise ValueError("TOPIC_ARN environment variable not set")

    collection = event.get("collection")
    if not collection:
        raise ValueError("Missing required parameter: 'collection'")

    if collection not in COLLECTION_ORIGIN_DATES:
        raise ValueError(
            f"Invalid collection: {collection}. Must be 'HLSL30' or 'HLSS30'"
        )

    # Parse start_date (default to collection origin)
    start_date_str = event.get("start_date")
    if start_date_str:
        try:
            start_date = datetime.fromisoformat(start_date_str)
        except ValueError:
            raise ValueError(
                f"Invalid start_date format: {start_date_str}. Expected ISO format (YYYY-MM-DD)"
            )
    else:
        start_date = datetime.fromisoformat(COLLECTION_ORIGIN_DATES[collection])
        logger.info(
            f"No start_date provided, using collection origin: {start_date.date()}"
        )

    # Parse end_date (default to yesterday)
    end_date_str = event.get("end_date")
    if end_date_str:
        try:
            end_date = datetime.fromisoformat(end_date_str)
        except ValueError:
            raise ValueError(
                f"Invalid end_date format: {end_date_str}. Expected ISO format (YYYY-MM-DD)"
            )
    else:
        end_date = datetime.now() - timedelta(days=1)
        logger.info(f"No end_date provided, using yesterday: {end_date.date()}")

    # Validate date range
    if start_date > end_date:
        raise ValueError(
            f"start_date ({start_date.date()}) must be before or equal to end_date ({end_date.date()})"
        )

    # Get dest from event or environment variable
    dest = event.get("dest")
    if not dest:
        bucket_name = os.environ.get("BUCKET_NAME")
        if bucket_name:
            dest = f"s3://{bucket_name}"
            logger.info(f"Using default destination from BUCKET_NAME: {dest}")

    # Parse optional parameters
    bounding_box_list = event.get("bounding_box")
    bounding_box = None
    if bounding_box_list:
        if len(bounding_box_list) != 4:
            raise ValueError(
                f"Invalid bounding_box: expected 4 values, got {len(bounding_box_list)}"
            )
        bounding_box = list(bounding_box_list)

    protocol = event.get("protocol", "s3")
    if protocol not in ["s3", "https"]:
        raise ValueError(f"Invalid protocol: {protocol}. Must be 's3' or 'https'")

    skip_existing = event.get("skip_existing", True)

    # Calculate number of days
    num_days = (end_date - start_date).days + 1
    logger.info(
        f"Publishing {num_days} messages for {collection} from {start_date.date()} to {end_date.date()}"
    )

    # Publish messages
    sns_client = boto3.client("sns")
    published_messages = []
    current_date = start_date

    while current_date <= end_date:
        message = {
            "collection": collection,
            "date": current_date.date().isoformat(),
            "protocol": protocol,
            "skip_existing": skip_existing,
        }

        if dest:
            message["dest"] = dest

        if bounding_box:
            message["bounding_box"] = bounding_box

        try:
            response = sns_client.publish(
                TopicArn=topic_arn,
                Message=json.dumps(message),
                Subject=f"Cache STAC links for {collection} on {current_date.date()}",
            )

            published_messages.append(
                {
                    "date": current_date.date().isoformat(),
                    "message_id": response["MessageId"],
                }
            )

            logger.info(
                f"Published message for {collection} {current_date.date()}: {response['MessageId']}"
            )

        except Exception as e:
            logger.error(
                f"Failed to publish message for {current_date.date()}: {str(e)}"
            )
            raise

        current_date += timedelta(days=1)

    logger.info(f"Successfully published {len(published_messages)} messages")

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "collection": collection,
                "start_date": start_date.date().isoformat(),
                "end_date": end_date.date().isoformat(),
                "messages_published": len(published_messages),
                "first_message_id": (
                    published_messages[0]["message_id"] if published_messages else None
                ),
                "last_message_id": (
                    published_messages[-1]["message_id"] if published_messages else None
                ),
            }
        ),
    }
