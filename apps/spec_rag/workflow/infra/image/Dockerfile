FROM python:3.11-slim

WORKDIR /app

COPY apps/spec_rag/workflow/infra/image/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY apps ./apps
COPY core ./core
COPY services ./services
COPY scripts ./scripts

CMD ["python", "-m", "apps.spec_rag.workflow.app.service.run_worker"]
