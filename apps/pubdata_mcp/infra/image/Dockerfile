FROM python:3.11-slim

WORKDIR /app

COPY apps/pubdata_mcp/infra/image/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY apps/pubdata_mcp /app/apps/pubdata_mcp
COPY core/mcp_runtime /app/core/mcp_runtime

ENV PYTHONPATH=/app

CMD ["fastmcp", "run", "apps/pubdata_mcp/app/main.py:mcp", "--transport", "http", "--host", "0.0.0.0", "--port", "8000"]
