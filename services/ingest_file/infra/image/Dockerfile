FROM python:3.11-slim

WORKDIR /app

COPY services/ingest_file/infra/image /tmp/build/ingest_file
RUN pip install --no-cache-dir -r /tmp/build/ingest_file/requirements.txt

COPY services ./services
COPY core ./core
COPY apps ./apps
COPY scripts ./scripts

CMD ["python", "-m", "core.runtime.worker.runtime.worker"]
