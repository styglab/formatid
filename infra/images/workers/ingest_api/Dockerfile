FROM python:3.11-slim

WORKDIR /app

COPY infra/images/workers/ingest_api /tmp/build/ingest_api
RUN pip install --no-cache-dir -r /tmp/build/ingest_api/requirements.txt

COPY services ./services
COPY apps ./apps
COPY shared ./shared
COPY scripts ./scripts
COPY infra/postgres ./infra/postgres

CMD ["python", "-m", "services.worker.runtime.worker"]
