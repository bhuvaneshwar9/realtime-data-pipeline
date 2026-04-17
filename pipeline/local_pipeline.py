"""
Full pipeline demo — runs locally without Kafka or AWS.
Simulates Bronze → Silver → Gold with anomaly detection.

Run: python pipeline/local_pipeline.py
"""

import os, time, uuid, random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

BASE  = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(os.path.join(BASE, "bronze"), exist_ok=True)
os.makedirs(os.path.join(BASE, "silver"), exist_ok=True)
os.makedirs(os.path.join(BASE, "gold"),   exist_ok=True)

CATEGORIES = ["electronics", "clothing", "food", "travel", "sports"]
REGIONS    = ["us-east", "us-west", "eu-central", "ap-south"]


# ── Generate synthetic events ─────────────────────────────────────────────────
def generate_events(n: int = 500) -> pd.DataFrame:
    np.random.seed(42)
    amounts = np.random.normal(150, 60, n)
    anomaly_idx = random.sample(range(n), int(n * 0.02))
    for i in anomaly_idx:
        amounts[i] = random.uniform(5000, 20000)

    base_date = datetime(2026, 1, 1)
    return pd.DataFrame({
        "id":         [str(uuid.uuid4())[:8] for _ in range(n)],
        "user_id":    [f"user_{random.randint(1, 200):04d}" for _ in range(n)],
        "category":   [random.choice(CATEGORIES) for _ in range(n)],
        "region":     [random.choice(REGIONS) for _ in range(n)],
        "amount":     [round(max(0.01, a), 2) for a in amounts],
        "event_type": [random.choice(["purchase", "refund"]) for _ in range(n)],
        "timestamp":  [base_date + timedelta(minutes=i * 5) for i in range(n)],
    })


# ── Bronze: raw ingest ────────────────────────────────────────────────────────
def bronze(df: pd.DataFrame) -> pd.DataFrame:
    df["_ingested_at"] = datetime.utcnow().isoformat()
    path = os.path.join(BASE, "bronze", "events.parquet")
    df.to_parquet(path, index=False)
    print(f"  Bronze: {len(df)} events  →  {path}")
    return df


# ── Silver: clean + validate ──────────────────────────────────────────────────
def silver(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates("id").dropna(subset=["id", "user_id"])
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df[df["amount"] > 0].copy()
    df["timestamp"]  = pd.to_datetime(df["timestamp"])
    df["event_date"] = df["timestamp"].dt.date

    mean, std = df["amount"].mean(), df["amount"].std()
    df["is_anomaly"] = (df["amount"] - mean).abs() > 3 * std
    df["_processed_at"] = datetime.utcnow().isoformat()

    path = os.path.join(BASE, "silver", "events.parquet")
    df.to_parquet(path, index=False)
    print(f"  Silver: {len(df)} clean rows | {df['is_anomaly'].sum()} anomalies  →  {path}")
    return df


# ── Gold: aggregate ───────────────────────────────────────────────────────────
def gold(df: pd.DataFrame):
    purchases = df[df["event_type"] == "purchase"]

    daily = (purchases.groupby(["event_date", "region", "category"])
             .agg(total_events=("id", "count"),
                  total_revenue=("amount", "sum"),
                  avg_order=("amount", "mean"),
                  unique_users=("user_id", "nunique"),
                  anomalies=("is_anomaly", "sum"))
             .reset_index())

    users = (df.groupby("user_id")
             .agg(total_orders=("id", "count"),
                  lifetime_value=("amount", "sum"),
                  avg_order=("amount", "mean"),
                  last_seen=("timestamp", "max"))
             .reset_index())
    users["days_since_last"] = (pd.Timestamp("2026-02-01") - users["last_seen"]).dt.days
    users["is_churned"]      = users["days_since_last"] > 20

    daily.to_parquet(os.path.join(BASE, "gold", "daily.parquet"), index=False)
    users.to_parquet(os.path.join(BASE, "gold", "users.parquet"), index=False)
    print(f"  Gold:   {len(daily)} daily rows | {len(users)} user profiles")
    return daily, users


# ── Report ────────────────────────────────────────────────────────────────────
def report(daily, users):
    print(f"\n{'='*55}")
    print("  PIPELINE REPORT")
    print(f"{'='*55}")
    print(f"\n  Revenue by region:")
    rev = daily.groupby("region")["total_revenue"].sum().sort_values(ascending=False)
    for r, v in rev.items():
        print(f"    {r:<15}  ${v:>10,.2f}")

    print(f"\n  Churn risk: {users['is_churned'].sum()} / {len(users)} users")
    print(f"  Top spenders:")
    for _, row in users.nlargest(3, "lifetime_value").iterrows():
        print(f"    {row['user_id']}  ${row['lifetime_value']:>8,.2f}  ({int(row['total_orders'])} orders)")
    print()


if __name__ == "__main__":
    print("="*55)
    print("  REAL-TIME DATA PIPELINE — LOCAL DEMO")
    print("="*55)
    raw = generate_events(500)
    b   = bronze(raw)
    s   = silver(b)
    d, u = gold(s)
    report(d, u)
