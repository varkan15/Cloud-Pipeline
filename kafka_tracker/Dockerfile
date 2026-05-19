FROM python:3.9

WORKDIR /app

RUN pip install kafka-python

COPY kafka_metrics.py .

CMD ["python", "kafka_metrics.py"]