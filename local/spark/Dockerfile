# Local Spark image: Python 3.11, Java 17, PySpark 3.5.6, Iceberg 1.10 and Kafka connector baked in.
# Build context is the repo root (see local/docker-compose.yml).
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java
ENV JAVA_HOME=/opt/java

COPY --from=ghcr.io/astral-sh/uv:0.11.25 /uv /usr/local/bin/uv
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    PATH=/opt/venv/bin:$PATH

WORKDIR /app

# 1. Third-party dependencies only, so code edits do not invalidate this layer.
COPY pyproject.toml uv.lock ./
COPY packages/core/pyproject.toml packages/core/pyproject.toml
COPY packages/ingest/pyproject.toml packages/ingest/pyproject.toml
COPY packages/jobs/pyproject.toml packages/jobs/pyproject.toml
COPY packages/api/pyproject.toml packages/api/pyproject.toml
RUN uv sync --frozen --all-packages --no-install-workspace

# 2. Connector jars.
COPY local/spark/fetch_jars.py /tmp/fetch_jars.py
RUN python /tmp/fetch_jars.py && rm /tmp/fetch_jars.py

# 3. Our code.
COPY packages packages
COPY tests tests
RUN uv sync --frozen --all-packages

EXPOSE 4040
CMD ["python", "-m", "ghostbus_jobs.smoke"]
