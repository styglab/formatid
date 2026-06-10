FROM python:3.11-slim

WORKDIR /app

COPY apps /app/apps

CMD ["python", "-m", "http.server", "3000", "--directory", "/app/apps/data_score/frontend"]
