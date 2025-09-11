ARG PY_IMAGE
FROM ${PY_IMAGE:-python:3.11-slim}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates wget && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Expose both ports (HTTP + /metrics)
EXPOSE ${PORT}
EXPOSE ${METRICS_PORT}

# Run your script's server mode
CMD ["python", "main.py", "--serve"]
