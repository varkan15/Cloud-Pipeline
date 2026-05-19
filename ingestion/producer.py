from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timezone
import uuid
import csv
import os

print("Waiting for Kafka...", flush=True)

while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Connected to Kafka!", flush=True)
        break
    except Exception:
        print("Kafka not ready, retrying in 1 second...", flush=True)
        time.sleep(1)

banks = ["HDFC", "ICICI", "SBI", "Canara", "BOB", "Kotak"]

container_id = os.getenv("HOSTNAME", "producer")
METRICS_DIR = "/app/metrics/p_metrics"
METRICS_FILE = f"{METRICS_DIR}/{container_id}_metrics.csv"
METRICS_INTERVAL = 10  # seconds

os.makedirs(METRICS_DIR, exist_ok=True)

def init_metrics_file():
    if not os.path.exists(METRICS_FILE):
        with open(METRICS_FILE, mode="w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "window_start_utc",
                "window_end_utc",
                "messages_sent",
                "msg_per_sec"
            ])

def log_input_rate(window_start, window_end, messages_sent):
    duration = max((window_end - window_start).total_seconds(), 1e-9)
    msg_per_sec = messages_sent / duration

    with open(METRICS_FILE, mode="a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            window_start.isoformat(),
            window_end.isoformat(),
            messages_sent,
            round(msg_per_sec, 3)
        ])

    print(
        f"[{container_id}] [METRICS] {window_start.isoformat()} to {window_end.isoformat()} | "
        f"messages={messages_sent} | rate={msg_per_sec:.2f} msg/s",
        flush=True
    )

def generate_transaction():
    bank_from = random.choice(banks)
    bank_to = random.choice(banks)
    from_account = f"{bank_from}_A{random.randint(1000, 2000)}"
    to_account = f"{bank_to}_A{random.randint(1000, 2000)}"

    while from_account == to_account:
        bank_to = random.choice(banks)
        to_account = f"{bank_to}_A{random.randint(1000, 2000)}"

    amount = round(random.uniform(100, 1000000), 2)
    now = datetime.now(timezone.utc)

    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id_from": from_account,
        "account_id_to": to_account,
        "bank_from": bank_from,
        "bank_to": bank_to,
        "amount": amount,
        "transaction_time": now.isoformat(),
        "producer_time": now.isoformat(),
        "is_suspicious": (
            amount > 800000 or (bank_from != bank_to and amount > 500000)
        )
    }

init_metrics_file()

window_start = datetime.now(timezone.utc)
messages_sent_in_window = 0

while True:
    transaction = generate_transaction()
    producer.send("transactions", value=transaction)
    producer.flush()

    messages_sent_in_window += 1
    print(f"[{container_id}] Produced:", transaction, flush=True)

    now = datetime.now(timezone.utc)
    elapsed = (now - window_start).total_seconds()

    if elapsed >= METRICS_INTERVAL:
        log_input_rate(window_start, now, messages_sent_in_window)
        window_start = now
        messages_sent_in_window = 0
    
    time.sleep(1)