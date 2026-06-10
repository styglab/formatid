FROM python:3.11-slim

WORKDIR /app

COPY apps /app/apps

CMD ["python", "-m", "apps.data_score.app.service.worker_stub"]
