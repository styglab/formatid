FROM prefecthq/prefect:3-latest

WORKDIR /app

COPY apps/shared/data_pipeline/infra/image/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY apps ./apps

ENV PYTHONPATH=/app

CMD ["prefect", "worker", "start", "--pool", "shared-data-pipeline-pool"]
