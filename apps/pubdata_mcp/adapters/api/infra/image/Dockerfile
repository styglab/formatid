FROM python:3.11-slim

WORKDIR /app

COPY apps/pubdata_mcp/adapters/api/infra/image/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY apps/pubdata_mcp /app/apps/pubdata_mcp
COPY core/runtime/mcp /app/core/runtime/mcp

ENV PYTHONPATH=/app

CMD ["fastmcp", "run", "apps/pubdata_mcp/adapters/api/main.py:mcp", "--transport", "http", "--host", "0.0.0.0", "--port", "8000"]
