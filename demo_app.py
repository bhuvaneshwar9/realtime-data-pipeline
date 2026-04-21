"""
Realtime Data Pipeline — Live Demo API
Simulates the full Bronze → Silver → Gold medallion pipeline
on synthetic e-commerce events. No Kafka or AWS required.
"""
import sys, os
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.path.insert(0, os.path.dirname(__file__))

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
import uuid, random
from datetime import datetime, timedelta, timezone

app = FastAPI(
    title="Realtime Data Pipeline — Demo",
    description="Bronze → Silver → Gold medallion pipeline on synthetic streaming events",
    version="1.0.0",
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

CATEGORIES = ["electronics", "clothing", "food", "travel", "sports"]
REGIONS    = ["us-east", "us-west", "eu-central", "ap-south"]

_last_run: dict = {}


def generate_events(n: int = 500, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    amounts = rng.normal(150, 60, n)
    anomaly_idx = rng.choice(n, int(n * 0.02), replace=False)
    for i in anomaly_idx:
        amounts[i] = rng.uniform(5000, 20000)
    base_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return pd.DataFrame({
        "id":         [str(uuid.uuid4())[:8] for _ in range(n)],
        "user_id":    [f"user_{rng.integers(1, 201):04d}" for _ in range(n)],
        "category":   rng.choice(CATEGORIES, n).tolist(),
        "region":     rng.choice(REGIONS, n).tolist(),
        "amount":     [round(max(0.01, float(a)), 2) for a in amounts],
        "event_type": rng.choice(["purchase", "refund"], n, p=[0.85, 0.15]).tolist(),
        "timestamp":  [base_date + timedelta(minutes=int(i) * 5) for i in range(n)],
    })


def run_bronze(df: pd.DataFrame) -> dict:
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    return {
        "rows": len(df),
        "columns": list(df.columns),
        "sample": df.head(3).to_dict(orient="records"),
        "size_kb": round(df.memory_usage(deep=True).sum() / 1024, 1),
    }


def run_silver(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    before = len(df)
    df = df.drop_duplicates("id").dropna(subset=["id", "user_id"])
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df[df["amount"] > 0].copy()
    df["timestamp"]  = pd.to_datetime(df["timestamp"], utc=True)
    df["event_date"] = df["timestamp"].dt.date

    mean, std = df["amount"].mean(), df["amount"].std()
    z_scores = (df["amount"] - mean) / std
    df["is_anomaly"]  = (z_scores.abs() > 3.0)
    df["z_score"]     = z_scores.round(3)
    df["cleaned_amt"] = df["amount"].where(~df["is_anomaly"], mean)

    anomalies = df["is_anomaly"].sum()
    return df, {
        "rows_in": before,
        "rows_out": len(df),
        "dropped": before - len(df),
        "anomalies_flagged": int(anomalies),
        "anomaly_rate_pct": round(float(anomalies) / len(df) * 100, 2),
        "amount_mean": round(float(mean), 2),
        "amount_std": round(float(std), 2),
    }


def run_gold(df: pd.DataFrame) -> dict:
    by_cat = df.groupby("category").agg(
        total_revenue=("cleaned_amt", "sum"),
        transaction_count=("id", "count"),
        avg_order_value=("cleaned_amt", "mean"),
        anomaly_count=("is_anomaly", "sum"),
    ).reset_index()
    by_cat["anomaly_rate"] = (by_cat["anomaly_count"] / by_cat["transaction_count"] * 100).round(2)
    by_cat["total_revenue"] = by_cat["total_revenue"].round(2)
    by_cat["avg_order_value"] = by_cat["avg_order_value"].round(2)

    by_region = df.groupby("region").agg(
        total_revenue=("cleaned_amt", "sum"),
        transaction_count=("id", "count"),
    ).reset_index()
    by_region["total_revenue"] = by_region["total_revenue"].round(2)

    return {
        "by_category": by_cat.to_dict(orient="records"),
        "by_region": by_region.to_dict(orient="records"),
        "total_revenue": round(float(df["cleaned_amt"].sum()), 2),
        "total_transactions": len(df),
        "purchase_count": int((df["event_type"] == "purchase").sum()),
        "refund_count": int((df["event_type"] == "refund").sum()),
    }


@app.get("/health")
def health():
    return {"status": "ok", "service": "realtime-data-pipeline", "version": "1.0.0"}


@app.get("/run", summary="Run the full Bronze → Silver → Gold pipeline")
def run_pipeline(n_events: int = 500):
    n_events = min(n_events, 5000)
    t0 = datetime.now(timezone.utc)
    raw_df = generate_events(n_events)
    bronze = run_bronze(raw_df.copy())
    silver_df, silver = run_silver(raw_df.copy())
    gold = run_gold(silver_df)
    elapsed = round((datetime.now(timezone.utc) - t0).total_seconds() * 1000, 1)

    result = {
        "run_at": t0.isoformat(),
        "elapsed_ms": elapsed,
        "pipeline": {
            "bronze": bronze,
            "silver": silver,
            "gold": gold,
        },
    }
    global _last_run
    _last_run = result
    return result


@app.get("/status", summary="Return the last pipeline run status")
def pipeline_status():
    if not _last_run:
        return run_pipeline()
    return _last_run


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard():
    data = run_pipeline()
    p = data["pipeline"]
    b, s, g = p["bronze"], p["silver"], p["gold"]

    cat_rows = ""
    for row in sorted(g["by_category"], key=lambda x: -x["total_revenue"]):
        cat_rows += f"""<tr>
          <td>{row['category']}</td>
          <td>${row['total_revenue']:,.2f}</td>
          <td>{row['transaction_count']}</td>
          <td>${row['avg_order_value']:,.2f}</td>
          <td style="color:{'#ef4444' if row['anomaly_rate'] > 2 else '#34d399'}">{row['anomaly_rate']}%</td>
        </tr>"""

    reg_rows = ""
    for row in sorted(g["by_region"], key=lambda x: -x["total_revenue"]):
        reg_rows += f"""<tr>
          <td>{row['region']}</td>
          <td>${row['total_revenue']:,.2f}</td>
          <td>{row['transaction_count']}</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Realtime Data Pipeline</title>
  <style>
    body{{background:#0d0a06;color:#e2e8f0;font-family:'Segoe UI',sans-serif;margin:0;padding:2rem}}
    h1{{color:#f97316;margin-bottom:.3rem}}
    .sub{{color:#94a3b8;margin-bottom:1.5rem;font-size:.9rem}}
    .pipeline{{display:flex;align-items:center;gap:.5rem;margin-bottom:2rem;flex-wrap:wrap}}
    .layer{{background:rgba(249,115,22,.1);border:1px solid rgba(249,115,22,.25);border-radius:10px;padding:1rem 1.4rem;min-width:160px}}
    .layer.silver{{background:rgba(56,189,248,.08);border-color:rgba(56,189,248,.25)}}
    .layer.gold{{background:rgba(52,211,153,.08);border-color:rgba(52,211,153,.25)}}
    .layer-name{{font-size:.7rem;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:#f97316;margin-bottom:.4rem}}
    .layer.silver .layer-name{{color:#38bdf8}}
    .layer.gold .layer-name{{color:#34d399}}
    .layer-num{{font-size:1.8rem;font-weight:800}}
    .layer-desc{{font-size:.75rem;color:#94a3b8;margin-top:.2rem}}
    .arrow{{font-size:1.5rem;color:#f97316}}
    .section-title{{color:#f97316;margin:1.5rem 0 .7rem;font-size:1rem;font-weight:700;letter-spacing:1px}}
    table{{width:100%;border-collapse:collapse;background:rgba(20,12,4,.8);border-radius:10px;overflow:hidden;margin-bottom:1.5rem}}
    th{{background:rgba(249,115,22,.12);color:#f97316;padding:.65rem 1rem;text-align:left;font-size:.78rem;letter-spacing:1px;text-transform:uppercase}}
    td{{padding:.65rem 1rem;border-bottom:1px solid rgba(249,115,22,.07);font-size:.86rem}}
    tr:last-child td{{border-bottom:none}}
    .badge{{display:inline-block;padding:.15rem .55rem;border-radius:20px;font-size:.72rem;font-weight:700}}
    .badge.warn{{background:rgba(239,68,68,.15);color:#ef4444}}
    .badge.ok{{background:rgba(52,211,153,.15);color:#34d399}}
    .btn{{display:inline-block;margin-top:.5rem;background:#f97316;color:#000;padding:.55rem 1.2rem;border-radius:8px;text-decoration:none;font-weight:700;font-size:.88rem}}
  </style>
</head>
<body>
  <h1>⚡ Realtime Data Pipeline</h1>
  <p class="sub">Bronze → Silver → Gold medallion architecture &nbsp;·&nbsp; {data['run_at'][:19]} UTC &nbsp;·&nbsp; {data['elapsed_ms']} ms</p>

  <div class="pipeline">
    <div class="layer">
      <div class="layer-name">Bronze</div>
      <div class="layer-num">{b['rows']:,}</div>
      <div class="layer-desc">Raw events ingested<br/>{b['size_kb']} KB in memory</div>
    </div>
    <div class="arrow">→</div>
    <div class="layer silver">
      <div class="layer-name">Silver</div>
      <div class="layer-num" style="color:#38bdf8">{s['rows_out']:,}</div>
      <div class="layer-desc">{s['dropped']} dropped · {s['anomalies_flagged']} anomalies ({s['anomaly_rate_pct']}%)</div>
    </div>
    <div class="arrow">→</div>
    <div class="layer gold">
      <div class="layer-name">Gold</div>
      <div class="layer-num" style="color:#34d399">${g['total_revenue']:,.0f}</div>
      <div class="layer-desc">{g['total_transactions']:,} txns · {g['purchase_count']} purchases</div>
    </div>
  </div>

  <div class="section-title">BY CATEGORY</div>
  <table>
    <thead><tr><th>Category</th><th>Total Revenue</th><th>Transactions</th><th>Avg Order</th><th>Anomaly Rate</th></tr></thead>
    <tbody>{cat_rows}</tbody>
  </table>

  <div class="section-title">BY REGION</div>
  <table>
    <thead><tr><th>Region</th><th>Total Revenue</th><th>Transactions</th></tr></thead>
    <tbody>{reg_rows}</tbody>
  </table>

  <a class="btn" href="/run">↻ Re-run Pipeline</a>&nbsp;&nbsp;
  <a class="btn" style="background:rgba(249,115,22,.2);color:#f97316;border:1px solid rgba(249,115,22,.4)" href="/docs">📖 API Docs</a>
</body>
</html>"""
