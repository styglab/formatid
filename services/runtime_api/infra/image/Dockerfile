FROM python:3.11-slim

WORKDIR /app

COPY services/runtime_api/infra/image/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY services /app/services
COPY core /app/core
COPY apps /app/apps

CMD ["uvicorn", "services.runtime_api.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
