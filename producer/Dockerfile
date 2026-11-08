# Built from the project root (see docker-compose.yml) so the sample city
# (data/city/city.json) can be copied next to the code.
FROM python:3.12-slim
WORKDIR /app
COPY producer/requirements.txt .
RUN pip install --no-cache-dir --default-timeout=100 --retries 10 -r requirements.txt
COPY producer/*.py ./
COPY data/city/city.json /app/data/city.json
ENV CITY_FILE=/app/data/city.json PYTHONUNBUFFERED=1
CMD ["python", "-u", "producer.py"]
