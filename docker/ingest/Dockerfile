ARG PYTHON_IMAGE=python:3.12.13-slim-bookworm
FROM ${PYTHON_IMAGE}

ARG KAFKA_PYTHON_VERSION=3.0.2
ARG PSYCOPG2_BINARY_VERSION=2.9.12

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    INGEST_SCHEMA_SQL_PATH=/workspace/sql/analytics-postgres-init/010_event_ingest.sql

WORKDIR /workspace

RUN pip install --no-cache-dir \
    "kafka-python==${KAFKA_PYTHON_VERSION}" \
    "psycopg2-binary==${PSYCOPG2_BINARY_VERSION}"

COPY ingest_worker /workspace/ingest_worker
COPY sql/analytics-postgres-init/010_event_ingest.sql /workspace/sql/analytics-postgres-init/010_event_ingest.sql

CMD ["python", "-m", "ingest_worker.worker"]
