FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1     PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements-serve.txt ./
RUN pip install --no-cache-dir -r requirements-serve.txt

COPY 311_backend.py ./
COPY sync_socrata_to_postgres.py ./
COPY consumer_to_postgres.py ./
COPY stream_from_csv.py ./
COPY get_data.py ./
COPY schema.sql ./

CMD ["sh", "-c", "uvicorn 311_backend:app --host 0.0.0.0 --port ${PORT:-8000}"]
