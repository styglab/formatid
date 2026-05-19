FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY services/embedding/infra/image/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu torch==2.3.1+cpu
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY services/embedding /app/services/embedding

ENV PYTHONPATH=/app

CMD ["uvicorn", "services.embedding.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
