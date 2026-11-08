FROM apache/spark:3.5.3-python3

USER root
RUN pip install --no-cache-dir --default-timeout=100 --retries 10 psycopg2-binary==2.9.10 pytest==8.3.3 \
 && mkdir -p /opt/checkpoints /opt/ivy \
 && chown -R spark:spark /opt/checkpoints /opt/ivy
USER spark

COPY *.py /app/
WORKDIR /app

# --packages downloads the Kafka connector the first time (cached in /opt/ivy)
CMD ["/bin/sh", "-c", "exec /opt/spark/bin/spark-submit \
     --master \"$SPARK_MASTER\" \
     --driver-memory 2g \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
     --conf spark.jars.ivy=/opt/ivy \
     --py-files /app/transformations.py \
     /app/streaming_job.py"]
