FROM python:3.11-slim

LABEL maintainer="UTS Sistem Terdistribusi"
LABEL description="Pub-Sub Log Aggregator - idempotent Consumer with Deduplication"

WORKDIR /app
RUN adduser --disabled-password --gecos '' appuser \
    && mkdir -p /app/data \
    && chown -R appuser:appuser /app

USER appuser

COPY --chown=appuser:appuser requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=appuser:appuser src/ ./src/
COPY --chown=appuser:appuser tests/ ./tests/

ENV DEDUP_DB_PATH=/app/data/dedup.db
ENV LOG_LEVEL=INFO
ENV PYTHONPATH=/app
ENV PATH=/home/appuser/.local/bin:${PATH}

VOLUME ["/app/data"]

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

CMD ["python", "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]
