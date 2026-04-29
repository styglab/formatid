FROM python:3.11-slim

WORKDIR /app

COPY services/index_sparse/infra/image/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY services ./services
COPY core ./core
COPY apps ./apps
COPY scripts ./scripts

CMD ["python", "-m", "core.runtime.worker.runtime.worker"]
