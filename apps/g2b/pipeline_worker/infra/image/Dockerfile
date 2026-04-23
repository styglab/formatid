FROM python:3.11-slim

WORKDIR /app

COPY apps/g2b/pipeline_worker/infra/image/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY services /app/services
COPY core /app/core
COPY apps /app/apps

CMD ["python", "-m", "apps.g2b.pipeline_worker.app.service.run_worker"]
