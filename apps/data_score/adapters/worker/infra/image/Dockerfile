FROM python:3.11-slim

WORKDIR /app

COPY apps/data_score/adapters/worker/infra/image/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY apps /app/apps

CMD ["python", "-m", "apps.data_score.adapters.worker.main"]
