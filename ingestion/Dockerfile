FROM python:3.9

WORKDIR /app

RUN pip install kafka-python

COPY producer.py .

CMD ["python", "-u", "producer.py"]