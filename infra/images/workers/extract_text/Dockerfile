FROM python:3.11-slim

WORKDIR /app

COPY infra/images/workers/extract_text /tmp/build/extract_text
RUN pip install --no-cache-dir -r /tmp/build/extract_text/requirements.txt

COPY services ./services
COPY apps ./apps
COPY shared ./shared
COPY scripts ./scripts
COPY infra/postgres ./infra/postgres

CMD ["python", "-m", "services.worker.runtime.worker"]
