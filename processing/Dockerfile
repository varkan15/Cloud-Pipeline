FROM apache/spark:3.5.1-python3

USER root
WORKDIR /app

COPY spark_processor.py /app/spark_processor.py
COPY jars/ /opt/spark/jars/

RUN mkdir -p /app/metrics && chmod -R 777 /app

USER 185