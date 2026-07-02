FROM python:3.11-slim

WORKDIR /app

COPY services/context_platform/adapters/admin_api/infra/image/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

ENV HOME=/tmp
ENV PYSTOW_HOME=/tmp/pystow

COPY services /app/services
COPY core /app/core

CMD ["uvicorn", "services.context_platform.adapters.admin_api.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
