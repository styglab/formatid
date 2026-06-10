FROM python:3.11-slim

WORKDIR /app

COPY services/semantic_layer/adapters/worker/infra/image/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY services /app/services
COPY core /app/core

CMD ["python", "-m", "services.semantic_layer.adapters.worker.deployments"]
