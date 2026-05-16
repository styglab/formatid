FROM prefecthq/prefect:3-latest

WORKDIR /app

COPY apps/g2b/pipeline/infra/image/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY apps ./apps
COPY core ./core

ENV PYTHONPATH=/app

CMD ["prefect", "worker", "start", "--pool", "g2b-pipeline-pool"]
