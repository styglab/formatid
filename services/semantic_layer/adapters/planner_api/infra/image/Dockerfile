FROM python:3.11-slim

WORKDIR /app

COPY services/semantic_layer/adapters/planner_api/infra/image/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY services /app/services
COPY core /app/core

CMD ["uvicorn", "services.semantic_layer.adapters.planner_api.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
