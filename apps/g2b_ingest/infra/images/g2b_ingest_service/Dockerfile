FROM python:3.11-slim

WORKDIR /app

COPY apps/g2b_ingest/infra/images/g2b_ingest_service/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY services /app/services
COPY shared /app/shared
COPY apps /app/apps
COPY infra /app/infra

CMD ["python", "-m", "apps.g2b_ingest.service.main"]
