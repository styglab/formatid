FROM python:3.11-slim

WORKDIR /app

COPY services/embedding/infra/image/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY services/embedding /app/services/embedding

CMD ["uvicorn", "services.embedding.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
