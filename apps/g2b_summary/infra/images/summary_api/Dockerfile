FROM python:3.11-slim

WORKDIR /app

COPY apps/g2b_summary/infra/images/summary_api/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY apps /app/apps
COPY services /app/services
COPY shared /app/shared

CMD ["uvicorn", "apps.g2b_summary.service.main:app", "--host", "0.0.0.0", "--port", "8000"]
