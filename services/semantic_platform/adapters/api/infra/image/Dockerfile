FROM python:3.11-slim

WORKDIR /app

COPY services/semantic_platform/adapters/api/infra/image/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY services/semantic_platform /app/services/semantic_platform
COPY sources /app/sources

ENV PYTHONPATH=/app

CMD ["uvicorn", "services.semantic_platform.adapters.api.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
