FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    POETRY_VIRTUALENVS_CREATE=false

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir poetry

WORKDIR /app
COPY pyproject.toml poetry.lock README.md ./
RUN poetry install --only main --no-root --no-interaction --no-ansi
COPY src ./src
COPY docker/entrypoint.sh ./docker/entrypoint.sh
RUN chmod +x ./docker/entrypoint.sh \
    && poetry install --only-root --no-interaction --no-ansi

# Match the bundled apache/spark image so driver-created shared-volume directories
# remain writable by executor processes in Compose and Kubernetes.
RUN groupadd --gid 185 appuser \
    && useradd --uid 185 --gid 185 --create-home appuser
USER appuser

ENTRYPOINT ["/app/docker/entrypoint.sh"]
CMD ["uvicorn", "dalmatian.api:app", "--host", "0.0.0.0", "--port", "8000"]
