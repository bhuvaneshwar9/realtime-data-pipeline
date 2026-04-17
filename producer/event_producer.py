"""Kafka event producer — publishes synthetic transactions."""

import json, random, time, uuid
from datetime import datetime
from kafka import KafkaProducer

BROKER = "localhost:9092"
TOPIC  = "transactions"
CATEGORIES = ["electronics", "clothing", "food", "travel", "sports"]
REGIONS    = ["us-east", "us-west", "eu-central", "ap-south"]


def make_event() -> dict:
    amount = round(random.gauss(150, 60), 2)
    if random.random() < 0.02:
        amount = round(random.uniform(5000, 20000), 2)  # anomaly
    return {
        "id":         str(uuid.uuid4()),
        "user_id":    f"user_{random.randint(1, 5000):04d}",
        "category":   random.choice(CATEGORIES),
        "region":     random.choice(REGIONS),
        "amount":     max(0.01, amount),
        "event_type": random.choice(["purchase", "refund", "click"]),
        "timestamp":  datetime.utcnow().isoformat(),
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode(),
        acks="all",
        retries=3,
    )
    print(f"Publishing to '{TOPIC}' — Ctrl+C to stop")
    count = 0
    try:
        while True:
            producer.send(TOPIC, make_event())
            count += 1
            if count % 500 == 0:
                print(f"  {count} events published")
            time.sleep(0.02)
    except KeyboardInterrupt:
        print(f"\nStopped after {count} events")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
