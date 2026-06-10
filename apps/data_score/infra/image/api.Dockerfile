FROM python:3.11-slim

WORKDIR /app

COPY apps/data_score/infra/image/api-requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY apps /app/apps

CMD ["uvicorn", "apps.data_score.app.service.api_stub:app", "--host", "0.0.0.0", "--port", "8000"]
