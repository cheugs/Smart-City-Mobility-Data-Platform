# Online demo of the dashboard (Render, Koyeb, Hugging Face Spaces...).
# It runs in DEMO_MODE: the answers come from dashboard/data/snapshot.json,
# a copy of a real pipeline run, so no database is needed.
# The full local stack does NOT use this file (see docker-compose.yml).
FROM python:3.12-slim
WORKDIR /app
COPY dashboard/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dashboard/ ./
ENV PYTHONUNBUFFERED=1 DEMO_MODE=1 PORT=8000
EXPOSE 8000
# Most free hosts give the port in $PORT
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT}"]
