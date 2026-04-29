FROM python:3.11-slim

WORKDIR /app

COPY services/llm/infra/image /tmp/build/llm
RUN pip install --no-cache-dir -r /tmp/build/llm/requirements.txt

COPY services ./services
COPY core ./core
COPY apps ./apps
COPY scripts ./scripts

CMD ["python", "-m", "core.runtime.worker.runtime.worker"]
