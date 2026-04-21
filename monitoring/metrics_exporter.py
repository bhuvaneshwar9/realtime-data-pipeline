"""
Prometheus metrics exporter for pipeline health.
Exposes event throughput, anomaly rate, and lag metrics.

Run: python monitoring/metrics_exporter.py
Then scrape: http://localhost:8000/metrics
"""

import time, os, random
from prometheus_client import start_http_server, Counter, Gauge, Histogram

PORT = 8000

# ── Metrics ───────────────────────────────────────────────────────────────────
events_processed = Counter(
    "pipeline_events_processed_total",
    "Total events processed", ["region", "category"]
)
anomalies_detected = Counter(
    "pipeline_anomalies_total",
    "Total anomalies flagged", ["region"]
)
processing_latency = Histogram(
    "pipeline_processing_latency_seconds",
    "Batch processing latency",
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0]
)
pipeline_lag = Gauge(
    "pipeline_consumer_lag_messages",
    "Kafka consumer lag", ["topic"]
)
active_users = Gauge(
    "pipeline_active_users_total",
    "Unique active users in last 5 min"
)

CATEGORIES = ["electronics", "clothing", "food", "travel"]
REGIONS    = ["us-east", "us-west", "eu-central", "ap-south"]


def simulate_metrics():
    """Simulate real pipeline metrics for demo purposes."""
    while True:
        region   = random.choice(REGIONS)
        category = random.choice(CATEGORIES)
        batch_n  = random.randint(50, 200)

        events_processed.labels(region=region, category=category).inc(batch_n)

        if random.random() < 0.05:
            anomalies_detected.labels(region=region).inc(random.randint(1, 3))

        latency = random.uniform(0.2, 2.5)
        processing_latency.observe(latency)
        pipeline_lag.labels(topic="transactions").set(random.randint(0, 500))
        active_users.set(random.randint(200, 800))

        print(f"  Batch: {batch_n} events | latency: {latency:.2f}s | region: {region}")
        time.sleep(5)


if __name__ == "__main__":
    start_http_server(PORT)
    print(f"Prometheus metrics at http://localhost:{PORT}/metrics")
    simulate_metrics()
