FROM python:3.11-slim

WORKDIR /app

COPY services/extract/infra/image /tmp/build/extract_text
RUN pip install --no-cache-dir -r /tmp/build/extract_text/requirements.txt

COPY services ./services
COPY core ./core
COPY apps ./apps
COPY scripts ./scripts

CMD ["python", "-m", "core.runtime.worker.runtime.worker"]
