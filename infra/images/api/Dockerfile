FROM python:3.11-slim

WORKDIR /app

COPY infra/images/api/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY services /app/services
COPY shared /app/shared
COPY infra /app/infra

CMD ["uvicorn", "services.api.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
