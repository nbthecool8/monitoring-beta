FROM python:3.11-slim

WORKDIR /app

COPY app/main.py .

RUN pip install prometheus_client

EXPOSE 8080 9000

CMD ["python", "main.py"]
