# Ingest image: the GTFS-R poller and the static watcher. Small: no Java, no Spark.
# Build context is the repo root (see local/docker-compose.yml).
FROM python:3.11-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.11.25 /uv /usr/local/bin/uv
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH=/opt/venv/bin:$PATH

WORKDIR /app

# 1. Third-party dependencies only. Every member's pyproject is needed to read the lock file.
COPY pyproject.toml uv.lock ./
COPY packages/core/pyproject.toml packages/core/pyproject.toml
COPY packages/ingest/pyproject.toml packages/ingest/pyproject.toml
COPY packages/jobs/pyproject.toml packages/jobs/pyproject.toml
COPY packages/api/pyproject.toml packages/api/pyproject.toml
RUN uv sync --frozen --no-dev --package ghostbus-ingest --no-install-workspace

# 2. Our code.
COPY packages/core packages/core
COPY packages/ingest packages/ingest
RUN uv sync --frozen --no-dev --package ghostbus-ingest

RUN useradd --uid 10001 --no-create-home ghostbus
USER ghostbus

CMD ["python", "-m", "ghostbus_ingest.poller"]
