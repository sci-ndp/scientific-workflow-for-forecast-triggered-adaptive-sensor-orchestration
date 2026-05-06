FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Runtime libraries used by scientific Python wheels and cartographic packages.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first so code-only changes reuse cached layers.
COPY requirements.txt /app/requirements.txt
COPY streaming-py /opt/streaming-py
RUN pip install --no-cache-dir -r /app/requirements.txt \
    && pip uninstall -y scidx_streaming \
    && pip install --no-cache-dir -e /opt/streaming-py

# Copy the application and static assets into the image.
COPY . /app
