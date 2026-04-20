FROM python:3.11-slim

WORKDIR /app

COPY domains/pps/images/pps_bid /tmp/build/pps_bid
RUN pip install --no-cache-dir -r /tmp/build/pps_bid/requirements.txt

COPY services ./services
COPY domains ./domains
COPY shared ./shared
COPY scripts ./scripts
COPY infra/postgres ./infra/postgres

CMD ["python", "-m", "services.worker.runtime.worker"]
