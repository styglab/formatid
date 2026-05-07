FROM python:3.11-slim

WORKDIR /app

COPY apps/g2b_mcp/infra/image/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY apps/g2b_mcp /app/apps/g2b_mcp

ENV PYTHONPATH=/app

CMD ["fastmcp", "run", "apps/g2b_mcp/app/main.py:mcp", "--transport", "http", "--host", "0.0.0.0", "--port", "8000"]
