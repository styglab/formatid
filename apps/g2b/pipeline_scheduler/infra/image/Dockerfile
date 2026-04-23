FROM python:3.11-slim

WORKDIR /app

COPY apps/g2b/pipeline_scheduler/infra/image/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY services /app/services
COPY core /app/core
COPY apps /app/apps

CMD ["python", "-m", "apps.g2b.pipeline_scheduler.app.service.run_scheduler"]
