FROM python:3.11-slim

WORKDIR /app

COPY infra/images/pps_bid /tmp/build/pps_bid
RUN pip install --no-cache-dir -r /tmp/build/pps_bid/requirements.txt

COPY services ./services
COPY tasks ./tasks
COPY shared ./shared
COPY scripts ./scripts

CMD ["python", "-m", "services.worker.app.worker"]
