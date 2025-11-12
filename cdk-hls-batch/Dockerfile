FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app

COPY README.md pyproject.toml uv.lock entrypoint.sh ./
COPY src ./src

RUN uv sync --no-dev && mkdir -p /tmp/hls-processing && chmod +x /app/entrypoint.sh

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV AWS_DEFAULT_REGION=us-west-2
ENV TEMP_DIR=/tmp/hls-processing

# Set the entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

