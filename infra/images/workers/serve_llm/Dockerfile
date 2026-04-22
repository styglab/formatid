FROM python:3.11-slim

WORKDIR /app

COPY infra/images/workers/serve_llm /tmp/build/serve_llm
RUN pip install --no-cache-dir -r /tmp/build/serve_llm/requirements.txt

COPY services ./services
COPY apps ./apps
COPY shared ./shared
COPY scripts ./scripts
COPY infra/postgres ./infra/postgres

CMD ["python", "-m", "services.worker.runtime.worker"]
