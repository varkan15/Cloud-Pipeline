from kafka import KafkaConsumer, TopicPartition
from datetime import datetime, timezone
import csv
import os
import time

TOPIC = "transactions"
BOOTSTRAP_SERVERS = "kafka:9092"   # Docker network
INTERVAL = 10

METRICS_DIR = "/app/metrics/kafka_metrics"
METRICS_FILE = f"{METRICS_DIR}/kafka_input_metrics.csv"

os.makedirs(METRICS_DIR, exist_ok=True)

if not os.path.exists(METRICS_FILE):
    with open(METRICS_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "window_start_utc",
            "window_end_utc",
            "messages_entered",
            "msg_per_sec"
        ])

print("Connecting to Kafka for metrics...", flush=True)

while True:
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=None,
            enable_auto_commit=False
        )
        partitions = consumer.partitions_for_topic(TOPIC)
        if partitions:
            print(f"Connected. Found topic '{TOPIC}' with partitions: {partitions}", flush=True)
            break
        print("Topic not ready yet, retrying in 2 seconds...", flush=True)
        time.sleep(2)
    except Exception as e:
        print(f"Kafka not ready for metrics, retrying in 2 seconds... ({e})", flush=True)
        time.sleep(2)

topic_partitions = [TopicPartition(TOPIC, p) for p in partitions]

previous_offsets = consumer.end_offsets(topic_partitions)
window_start = datetime.now(timezone.utc)

print("Kafka metrics tracking started.", flush=True)

while True:
    time.sleep(INTERVAL)
    window_end = datetime.now(timezone.utc)

    current_offsets = consumer.end_offsets(topic_partitions)

    messages_entered = 0
    for tp in topic_partitions:
        messages_entered += current_offsets[tp] - previous_offsets[tp]

    duration = max((window_end - window_start).total_seconds(), 1e-9)
    msg_per_sec = messages_entered / duration

    with open(METRICS_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            window_start.isoformat(),
            window_end.isoformat(),
            messages_entered,
            round(msg_per_sec, 3)
        ])

    print(
        f"[KAFKA METRICS] {window_start.isoformat()} to {window_end.isoformat()} | "
        f"messages={messages_entered} | rate={msg_per_sec:.2f} msg/s",
        flush=True
    )

    previous_offsets = current_offsets
    window_start = window_end