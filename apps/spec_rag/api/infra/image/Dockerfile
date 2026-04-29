FROM python:3.11-slim

WORKDIR /app

COPY apps/spec_rag/api/infra/image/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY apps ./apps
COPY core ./core
COPY services ./services
COPY scripts ./scripts

CMD ["uvicorn", "apps.spec_rag.api.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
