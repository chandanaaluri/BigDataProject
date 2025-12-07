# src/simulator_producer.py
"""
Producer that attempts to send messages to Kafka, but falls back to a CSV when Kafka is unavailable.

Usage examples:
  # Run infinite (attempts Kafka, falls back to CSV if needed):
  python src/simulator_producer.py

  # For a finite test (rounds), modify rounds parameter below or add argparse wrapper as needed.
"""

import json
import csv
import os
import time
import random
from datetime import datetime, timezone

# Try to import kafka, but handle missing/NoBrokersAvailable gracefully
try:
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable
except Exception:
    KafkaProducer = None
    NoBrokersAvailable = Exception

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "agri_topic"
FALLBACK_CSV = os.path.join("data", "kafka_fallback.csv")


# ensure data folder exists
os.makedirs("data", exist_ok=True)


def create_producer():
    """Try to create Kafka producer; return producer or None on failure."""
    if KafkaProducer is None:
        print("kafka-python not installed; falling back to CSV.", flush=True)
        return None

    try:
        p = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retry_backoff_ms=200,
            request_timeout_ms=1000
        )
        # quick connection check
        if not p.bootstrap_connected():
            print("KafkaProducer created but not connected to broker - falling back.", flush=True)
            try:
                p.close()
            except Exception:
                pass
            return None
        print(f"Connected to Kafka broker at {KAFKA_BROKER}", flush=True)
        return p
    except NoBrokersAvailable:
        print("No Kafka brokers available at", KAFKA_BROKER, "- falling back to CSV.", flush=True)
    except Exception as e:
        print("Kafka producer creation failed:", e, "- falling back to CSV.", flush=True)
    return None


def write_fallback_csv(payload):
    header = ["sensor_id", "ts", "soil_moisture", "temperature", "humidity", "rainfall", "irrigation_needed"]
    file_exists = os.path.exists(FALLBACK_CSV)
    with open(FALLBACK_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        if not file_exists:
            writer.writeheader()
        row = {
            "sensor_id": payload.get("sensor_id"),
            "ts": payload.get("ts"),
            "soil_moisture": payload.get("soil_moisture"),
            "temperature": payload.get("temperature"),
            "humidity": payload.get("humidity"),
            "rainfall": payload.get("rainfall"),
            "irrigation_needed": payload.get("irrigation_needed"),
        }
        writer.writerow(row)
        f.flush()


def send_message(producer, topic, payload):
    """Send to Kafka if available, otherwise append JSON row to fallback CSV."""
    if producer:
        try:
            producer.send(topic, payload)
            # optional: flush to ensure delivery (kept non-blocking by default)
            # producer.flush(timeout=1)
            print("[Producer -> Kafka]", payload, flush=True)
            return True
        except Exception as e:
            print("Producer send failed:", e, "— will fallback to CSV append.", flush=True)
            # fallback to CSV below
    # fallback: write to CSV (append)
    write_fallback_csv(payload)
    print("[Producer -> CSV fallback]", payload, flush=True)
    return False


def simulate_and_produce(producer, topic, sensors=3, interval_min=0.5, interval_max=1.5, rounds=None):
    """Simple simulator: produce messages from `sensors` sensors; rounds=None => infinite"""
    count = 0
    try:
        while True:
            for i in range(sensors):
                msg = {
                    "sensor_id": f"sensor_{i+1}",
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "soil_moisture": float(max(0, min(100, random.gauss(40, 15)))),
                    "temperature": float(round(random.gauss(25, 5), 1)),
                    "humidity": float(round(random.uniform(30, 90), 1)),
                    "rainfall": float(round(max(0, random.gauss(0.2, 0.6)), 2)),
                }
                # compute irrigation_needed using the same rule
                msg["irrigation_needed"] = (msg["soil_moisture"] < 30.0) and (msg["rainfall"] < 0.5)
                send_message(producer, topic, msg)
                count += 1
                if rounds and count >= rounds:
                    print("Finished requested rounds:", rounds, flush=True)
                    return
                time.sleep(random.uniform(interval_min, interval_max))
    except KeyboardInterrupt:
        print("Simulator interrupted by user", flush=True)


if __name__ == "__main__":
    p = create_producer()
    # For notebook demos, you can call simulate_and_produce(p, KAFKA_TOPIC, rounds=50)
    # For interactive runs, this will run forever unless interrupted (or you modify the call)
    simulate_and_produce(p, KAFKA_TOPIC, sensors=3, rounds=None)
