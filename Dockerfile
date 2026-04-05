FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml .python-version ./
COPY pipeline/ pipeline/
RUN uv sync --no-dev --all-extras

EXPOSE 5002

ENTRYPOINT ["uv", "run", "crdb-wxd-pipeline"]
CMD ["webhook"]
