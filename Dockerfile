FROM python:3.11-slim AS builder

ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.8.3 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl build-essential && \
    curl -sSL https://install.python-poetry.org | python - && \
    ln -s /opt/poetry/bin/poetry /usr/local/bin/poetry && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml poetry.lock* ./
RUN poetry config virtualenvs.create false && \
    poetry install --no-root

COPY . .

FROM python:3.11-slim

WORKDIR /app

COPY --from=builder /app /app
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

ENV PYTHONPATH=/app:$PYTHONPATH
ENV DAGSTER_HOME=/app/dagster_home

LABEL maintainer="Untify <untify@icloud.com>"
LABEL version="1.0"
LABEL description="CineMetrics Data Engineering Project"
