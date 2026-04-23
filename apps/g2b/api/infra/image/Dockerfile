FROM python:3.11-slim

WORKDIR /app

COPY apps/g2b/api/infra/image/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY apps /app/apps
COPY services /app/services
COPY core /app/core

CMD ["uvicorn", "apps.g2b.api.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
