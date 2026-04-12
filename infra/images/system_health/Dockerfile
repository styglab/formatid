FROM python:3.11-slim

WORKDIR /app

COPY infra/images/system_health /tmp/build/system_health
RUN pip install --no-cache-dir -r /tmp/build/system_health/requirements.txt

COPY services ./services
COPY tasks ./tasks
COPY shared ./shared
COPY scripts ./scripts

CMD ["python", "-m", "services.worker.app.worker"]
