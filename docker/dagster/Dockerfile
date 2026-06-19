ARG PYTHON_IMAGE=python:3.12.13-slim-bookworm
FROM ${PYTHON_IMAGE}

ARG DAGSTER_VERSION=1.13.9
ARG DAGSTER_WEBSERVER_VERSION=1.13.9
ARG DAGSTER_POSTGRES_VERSION=0.29.9
ARG DAGSTER_DBT_VERSION=0.29.9
ARG DBT_CORE_VERSION=1.11.11
ARG DBT_POSTGRES_VERSION=1.10.1
ARG SODA_CORE_VERSION=4.14.0
ARG SODA_POSTGRES_VERSION=4.14.0
ARG PSYCOPG2_BINARY_VERSION=2.9.12

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/workspace/.dagster

WORKDIR /workspace

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "dagster==${DAGSTER_VERSION}" \
    "dagster-webserver==${DAGSTER_WEBSERVER_VERSION}" \
    "dagster-postgres==${DAGSTER_POSTGRES_VERSION}" \
    "dagster-dbt==${DAGSTER_DBT_VERSION}" \
    "dbt-core==${DBT_CORE_VERSION}" \
    "dbt-postgres==${DBT_POSTGRES_VERSION}" \
    "soda-core==${SODA_CORE_VERSION}" \
    "soda-postgres==${SODA_POSTGRES_VERSION}" \
    "psycopg2-binary==${PSYCOPG2_BINARY_VERSION}"

COPY dagster_workspace.yaml /workspace/dagster_workspace.yaml
COPY dagster_home /workspace/.dagster
COPY dagster_project /workspace/dagster_project
COPY dbt /workspace/dbt
COPY soda /workspace/soda
