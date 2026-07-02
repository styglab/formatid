FROM python:3.11-slim

WORKDIR /app
ENV USER=root
ENV HOME=/tmp
ENV CUDA_VISIBLE_DEVICES=""
ENV TORCHINDUCTOR_CACHE_DIR=/tmp/torchinductor

RUN apt-get update \
    && apt-get install -y --no-install-recommends libglib2.0-0 libgl1 libxcb1 \
    && rm -rf /var/lib/apt/lists/*

COPY services/context_platform/adapters/worker/infra/image/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --extra-index-url https://download.pytorch.org/whl/cpu -r /tmp/requirements.txt

ENV PYSTOW_HOME=/tmp/pystow

COPY services /app/services
COPY core /app/core

CMD ["python", "-m", "services.context_platform.adapters.worker.deployments"]
