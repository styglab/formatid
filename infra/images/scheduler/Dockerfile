FROM python:3.11-slim

WORKDIR /app

COPY infra/images/scheduler/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY services /app/services
COPY shared /app/shared
COPY domains /app/domains
COPY infra /app/infra

CMD ["python", "-m", "services.scheduler.runtime.scheduler"]
