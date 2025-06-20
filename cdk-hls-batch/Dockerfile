FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

COPY README.md pyproject.toml uv.lock entrypoint.sh aws_batch_monthly.py src ./

RUN uv sync && mkdir -p /tmp/hls-processing && chmod +x /app/entrypoint.sh

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV AWS_DEFAULT_REGION=us-west-2
ENV MAX_CONCURRENT_DAYS=10
ENV MAX_CONCURRENT_PER_DAY=50
ENV TEMP_DIR=/tmp/hls-processing

# Set the entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

