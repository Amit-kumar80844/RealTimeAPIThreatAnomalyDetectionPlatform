# ─────────────────────────────────────────────────────────────────────────────
# Dockerfile — Real-Time API Threat & Anomaly Detection Platform Dashboard
# Builds the FastAPI dashboard service that runs on port 8000.
# ─────────────────────────────────────────────────────────────────────────────

FROM python:3.10-slim

LABEL maintainer="CSE412 Applied Big Data Team"
LABEL description="Real-Time API Threat Detection Dashboard (FastAPI + Uvicorn)"

# Set working directory
WORKDIR /app

# Copy dependency list first for Docker layer caching efficiency
COPY requirements.txt .

# Install Python dependencies (no cache to keep image small)
RUN pip install --no-cache-dir -r requirements.txt

# Copy entire project source into image
COPY . .

# Expose FastAPI / Uvicorn default port
EXPOSE 8000

# Health-check: verify the dashboard is responsive
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/state')"

# Start the FastAPI server
CMD ["uvicorn", "dashboard.app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
