FROM prefecthq/prefect:3-latest

WORKDIR /app

COPY services/semantic_platform/adapters/worker/infra/image/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY apps ./apps
COPY services ./services
COPY core ./core
COPY sources ./sources

ENV PYTHONPATH=/app

CMD ["prefect", "worker", "start", "--pool", "semantic-platform-worker-pool"]
