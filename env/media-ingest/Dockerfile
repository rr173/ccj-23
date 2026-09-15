FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    DATA_DIR=/data

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

RUN useradd --create-home ingest && mkdir -p /data && chown -R ingest:ingest /data
USER ingest
VOLUME /data

EXPOSE 8000
# Default: API ingest layer. The merge worker overrides the command (see docker-compose.yml).
CMD ["uvicorn", "app.main:build_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
