# Built from the project root (see docker-compose.yml and render.yaml).
# The same image runs locally (live, reads PostGIS) and on Render (DEMO_MODE=1, reads the snapshot).
FROM python:3.12-slim
WORKDIR /app
COPY dashboard/requirements.txt .
RUN pip install --no-cache-dir --default-timeout=100 --retries 10 -r requirements.txt
COPY dashboard/ ./
ENV PYTHONUNBUFFERED=1 PORT=8000
EXPOSE 8000
# Render gives the port in $PORT; locally it is 8000
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT}"]
