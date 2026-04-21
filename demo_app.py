"""
Realtime Data Pipeline — Live Demo API
Fetches live cryptocurrency market data from CoinGecko (free, no API key)
and processes it through Bronze → Silver → Gold medallion architecture.
Auto-refreshes every 60 seconds.
"""
import sys, os
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.dirname(__file__))

import requests
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
from datetime import datetime, timezone

app = FastAPI(
    title="Realtime Data Pipeline — CoinGecko Live",
    description="Bronze → Silver → Gold medallion pipeline on live crypto market data",
    version="2.0.0",
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

COINGECKO = "https://api.coingecko.com/api/v3/coins/markets"
HDR       = {"User-Agent": "Mozilla/5.0 (DataPipelineBot/2.0; +github.com/bhuvaneshwar9)"}
CACHE_TTL = 300

_cache    = {"df": None, "fetched_at": None, "source": ""}
_last_run = {}


# ── Data ingestion ─────────────────────────────────────────────────────────────
def fetch_coins() -> tuple[pd.DataFrame, str]:
    now = datetime.now(timezone.utc)
    if _cache["df"] is not None:
        if (now - _cache["fetched_at"]).total_seconds() < CACHE_TTL:
            return _cache["df"], _cache["source"]
    try:
        resp = requests.get(COINGECKO, headers=HDR, timeout=12, params={
            "vs_currency": "usd", "order": "market_cap_desc",
            "per_page": 100, "page": 1, "sparkline": "false",
            "price_change_percentage": "24h,7d",
        })
        resp.raise_for_status()
        coins = resp.json()

        rows = []
        for c in coins:
            mc = c.get("market_cap") or 0
            tier = ("Large Cap" if mc >= 10_000_000_000 else
                    "Mid Cap"   if mc >= 1_000_000_000  else
                    "Small Cap" if mc >= 100_000_000     else "Micro Cap")
            pc24 = c.get("price_change_percentage_24h") or 0.0
            rows.append({
                "id":              c["id"],
                "symbol":          c["symbol"].upper(),
                "name":            c["name"],
                "category":        tier,
                "price_usd":       c.get("current_price") or 0.0,
                "market_cap":      mc,
                "volume_24h":      c.get("total_volume") or 0.0,
                "price_change_24h": pc24,
                "price_change_7d": c.get("price_change_percentage_7d_in_currency") or 0.0,
                "high_24h":        c.get("high_24h") or 0.0,
                "low_24h":         c.get("low_24h") or 0.0,
                "event_type":      "bullish" if pc24 >= 0 else "bearish",
                "timestamp":       now,
            })
        df  = pd.DataFrame(rows)
        src = f"CoinGecko API — top {len(df)} coins by market cap"
        _cache.update(df=df, fetched_at=now, source=src)
        return df, src
    except Exception as e:
        df  = _fallback()
        src = f"Synthetic fallback (CoinGecko error: {e})"
        _cache.update(df=df, fetched_at=now, source=src)
        return df, src


def _fallback() -> pd.DataFrame:
    rng = np.random.default_rng(42)
    now = datetime.now(timezone.utc)
    n   = 100
    names = ["Bitcoin","Ethereum","Solana","Cardano","Avalanche","Polkadot",
             "Chainlink","Uniswap","Litecoin","Ripple"]
    tiers = rng.choice(["Large Cap","Mid Cap","Small Cap","Micro Cap"], n,
                       p=[0.1,0.15,0.35,0.4]).tolist()
    return pd.DataFrame({
        "id":              [f"coin_{i}" for i in range(n)],
        "symbol":          [names[i % len(names)][:3].upper() for i in range(n)],
        "name":            [names[i % len(names)] for i in range(n)],
        "category":        tiers,
        "price_usd":       rng.uniform(0.001, 65000, n).round(4).tolist(),
        "market_cap":      rng.uniform(1e6, 1.3e12, n).tolist(),
        "volume_24h":      rng.uniform(1e5, 3e10, n).tolist(),
        "price_change_24h": rng.normal(0, 6, n).round(2).tolist(),
        "price_change_7d": rng.normal(0, 12, n).round(2).tolist(),
        "high_24h":        rng.uniform(0.001, 70000, n).round(4).tolist(),
        "low_24h":         rng.uniform(0.001, 60000, n).round(4).tolist(),
        "event_type":      rng.choice(["bullish","bearish"], n).tolist(),
        "timestamp":       [now]*n,
    })


# ── Medallion pipeline layers ──────────────────────────────────────────────────
def run_bronze(df: pd.DataFrame) -> dict:
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    return {
        "rows": len(df),
        "columns": list(df.columns),
        "sample": df[["symbol","name","price_usd","price_change_24h","category"]].head(5).to_dict(orient="records"),
        "size_kb": round(df.memory_usage(deep=True).sum() / 1024, 1),
        "bullish": int((df["event_type"] == "bullish").sum()),
        "bearish": int((df["event_type"] == "bearish").sum()),
    }


def run_silver(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    before = len(df)
    df = df.dropna(subset=["id", "price_usd"]).copy()
    df = df[df["price_usd"] > 0]

    # Anomaly detection — coins with abnormal 24h price movement
    mean = df["price_change_24h"].mean()
    std  = df["price_change_24h"].std() + 1e-9
    z    = (df["price_change_24h"] - mean) / std
    df["z_score"]    = z.round(3)
    df["is_anomaly"] = z.abs() > 2.0   # 2σ threshold (crypto is volatile)

    # Volume spike detection
    vm, vs = df["volume_24h"].mean(), df["volume_24h"].std() + 1e-9
    df["volume_spike"] = ((df["volume_24h"] - vm) / vs) > 3.0

    anomalies = int(df["is_anomaly"].sum())
    return df, {
        "rows_in": before, "rows_out": len(df), "dropped": before - len(df),
        "price_anomalies":    anomalies,
        "volume_spikes":      int(df["volume_spike"].sum()),
        "anomaly_rate_pct":   round(anomalies / len(df) * 100, 2),
        "avg_change_24h":     round(float(mean), 2),
        "market_sentiment":   "Bullish 🟢" if mean > 0 else "Bearish 🔴",
    }


def run_gold(df: pd.DataFrame) -> dict:
    tier_order = ["Large Cap", "Mid Cap", "Small Cap", "Micro Cap"]
    by_tier = df.groupby("category").agg(
        total_market_cap_B=("market_cap",      lambda x: round(x.sum() / 1e9, 2)),
        total_volume_B    =("volume_24h",      lambda x: round(x.sum() / 1e9, 2)),
        coin_count        =("id",              "count"),
        avg_change_24h    =("price_change_24h","mean"),
        anomaly_count     =("is_anomaly",      "sum"),
    ).reset_index()
    by_tier["avg_change_24h"] = by_tier["avg_change_24h"].round(2)
    # Sort by defined tier order
    by_tier["_ord"] = by_tier["category"].map({t: i for i, t in enumerate(tier_order)})
    by_tier = by_tier.sort_values("_ord").drop(columns="_ord")

    top5_gain = df.nlargest(5,  "price_change_24h")[["symbol","name","price_usd","price_change_24h"]].to_dict(orient="records")
    top5_loss = df.nsmallest(5, "price_change_24h")[["symbol","name","price_usd","price_change_24h"]].to_dict(orient="records")

    return {
        "by_tier":          by_tier.to_dict(orient="records"),
        "top_gainers":      top5_gain,
        "top_losers":       top5_loss,
        "total_market_cap_B": round(float(df["market_cap"].sum()) / 1e9, 2),
        "total_volume_B":   round(float(df["volume_24h"].sum()) / 1e9, 2),
        "bullish_coins":    int((df["price_change_24h"] > 0).sum()),
        "bearish_coins":    int((df["price_change_24h"] <= 0).sum()),
        "total_coins":      len(df),
    }


# ── API routes ─────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "service": "realtime-data-pipeline", "version": "2.0.0"}


@app.get("/run", summary="Run the full Bronze → Silver → Gold pipeline on live CoinGecko data")
def run_pipeline():
    t0     = datetime.now(timezone.utc)
    raw_df, source = fetch_coins()
    bronze         = run_bronze(raw_df.copy())
    silver_df, silver = run_silver(raw_df.copy())
    gold           = run_gold(silver_df)
    elapsed        = round((datetime.now(timezone.utc) - t0).total_seconds() * 1000, 1)

    result = {
        "run_at": t0.isoformat(), "elapsed_ms": elapsed, "source": source,
        "pipeline": {"bronze": bronze, "silver": silver, "gold": gold},
    }
    global _last_run
    _last_run = result
    return result


@app.get("/status", summary="Return the last pipeline run")
def status():
    return _last_run if _last_run else run_pipeline()


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard():
    data   = run_pipeline()
    p      = data["pipeline"]
    b, s, g = p["bronze"], p["silver"], p["gold"]
    source = data.get("source", "")

    tier_rows = ""
    for row in g["by_tier"]:
        chg   = row["avg_change_24h"]
        color = "#34d399" if chg >= 0 else "#ef4444"
        sign  = "+" if chg >= 0 else ""
        tier_rows += f"""<tr>
          <td>{row['category']}</td>
          <td>${row['total_market_cap_B']:,.1f}B</td>
          <td>${row['total_volume_B']:,.2f}B</td>
          <td>{row['coin_count']}</td>
          <td style="color:{color};font-weight:600">{sign}{chg}%</td>
          <td>{int(row['anomaly_count'])}</td>
        </tr>"""

    gainer_rows = ""
    for c in g["top_gainers"]:
        p_str = f"${c['price_usd']:,.4f}" if c['price_usd'] < 1 else f"${c['price_usd']:,.2f}"
        gainer_rows += f"""<tr>
          <td><b>{c['symbol']}</b></td><td>{c['name']}</td>
          <td>{p_str}</td>
          <td style="color:#34d399;font-weight:700">+{c['price_change_24h']:.2f}%</td>
        </tr>"""

    loser_rows = ""
    for c in g["top_losers"]:
        p_str = f"${c['price_usd']:,.4f}" if c['price_usd'] < 1 else f"${c['price_usd']:,.2f}"
        loser_rows += f"""<tr>
          <td><b>{c['symbol']}</b></td><td>{c['name']}</td>
          <td>{p_str}</td>
          <td style="color:#ef4444;font-weight:700">{c['price_change_24h']:.2f}%</td>
        </tr>"""

    sentiment_color = "#34d399" if "Bullish" in s["market_sentiment"] else "#ef4444"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Realtime Data Pipeline</title>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{background:#0d0a06;color:#e2e8f0;font-family:'Segoe UI',sans-serif;padding:2rem;line-height:1.5}}
    h1{{color:#f97316;font-size:1.6rem;margin-bottom:.25rem}}
    h2{{color:#f97316;font-size:.95rem;font-weight:700;letter-spacing:1px;text-transform:uppercase;margin:1.8rem 0 .7rem}}
    .sub{{color:#94a3b8;font-size:.88rem;margin-bottom:1rem}}
    .src{{display:flex;align-items:center;gap:.6rem;margin-bottom:1.2rem;
          background:rgba(52,211,153,.08);border:1px solid rgba(52,211,153,.25);
          border-radius:8px;padding:.5rem 1rem;font-size:.82rem;flex-wrap:wrap}}
    .dot{{width:9px;height:9px;background:#34d399;border-radius:50%;animation:pulse 1.5s infinite;flex-shrink:0}}
    @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.25}}}}
    .src a{{color:#34d399;text-decoration:none;font-weight:600}}
    .rbar{{background:rgba(249,115,22,.06);border:1px solid rgba(249,115,22,.14);
           border-radius:8px;padding:.55rem 1rem;font-size:.8rem;color:#94a3b8;
           display:flex;justify-content:space-between;align-items:center;margin-bottom:1.4rem}}
    #cd{{color:#f97316;font-weight:700}}
    /* pipeline flow */
    .pipeline{{display:flex;align-items:center;gap:.6rem;margin-bottom:2rem;flex-wrap:wrap}}
    .layer{{border-radius:10px;padding:1rem 1.4rem;min-width:160px}}
    .b{{background:rgba(249,115,22,.1);border:1px solid rgba(249,115,22,.3)}}
    .sv{{background:rgba(56,189,248,.08);border:1px solid rgba(56,189,248,.25)}}
    .g{{background:rgba(52,211,153,.08);border:1px solid rgba(52,211,153,.25)}}
    .lname{{font-size:.68rem;font-weight:700;letter-spacing:2px;text-transform:uppercase;margin-bottom:.4rem}}
    .b .lname{{color:#f97316}}.sv .lname{{color:#38bdf8}}.g .lname{{color:#34d399}}
    .lnum{{font-size:1.7rem;font-weight:800}}
    .b .lnum{{color:#f97316}}.sv .lnum{{color:#38bdf8}}.g .lnum{{color:#34d399}}
    .ldesc{{font-size:.74rem;color:#94a3b8;margin-top:.2rem}}
    .arrow{{font-size:1.4rem;color:#f97316}}
    /* stats */
    .stats{{display:flex;gap:.8rem;flex-wrap:wrap;margin-bottom:1.8rem}}
    .stat{{background:rgba(249,115,22,.08);border:1px solid rgba(249,115,22,.18);
           border-radius:10px;padding:.9rem 1.3rem;min-width:120px}}
    .stat-n{{font-size:1.8rem;font-weight:800;color:#f97316}}
    .stat-l{{color:#94a3b8;font-size:.75rem;margin-top:.15rem}}
    /* tables */
    table{{width:100%;border-collapse:collapse;background:rgba(20,12,4,.9);
           border-radius:10px;overflow:hidden;margin-bottom:1.5rem}}
    th{{background:rgba(249,115,22,.12);color:#f97316;padding:.65rem 1rem;
        text-align:left;font-size:.76rem;letter-spacing:1px;text-transform:uppercase}}
    td{{padding:.65rem 1rem;border-bottom:1px solid rgba(249,115,22,.07);font-size:.84rem}}
    tr:last-child td{{border-bottom:none}}
    tr:hover td{{background:rgba(249,115,22,.04)}}
    .two{{display:grid;grid-template-columns:1fr 1fr;gap:1.5rem}}
    @media(max-width:700px){{.two{{grid-template-columns:1fr}}}}
    .btns{{display:flex;gap:.6rem;flex-wrap:wrap;margin-top:1.2rem}}
    .btn{{display:inline-block;padding:.5rem 1.2rem;border-radius:8px;text-decoration:none;
          font-weight:700;font-size:.85rem;transition:.2s}}
    .solid{{background:#f97316;color:#000}}.solid:hover{{background:#ea580c}}
    .ghost{{background:rgba(249,115,22,.12);color:#f97316;border:1px solid rgba(249,115,22,.35)}}
    .ghost:hover{{background:rgba(249,115,22,.22)}}
    .green{{background:rgba(52,211,153,.12);color:#34d399;border:1px solid rgba(52,211,153,.3)}}
    .green:hover{{background:rgba(52,211,153,.22)}}
  </style>
</head>
<body>
  <h1>⚡ Realtime Data Pipeline</h1>
  <p class="sub">Bronze → Silver → Gold medallion architecture &nbsp;·&nbsp;
     {data['run_at'][:19]} UTC &nbsp;·&nbsp; {data['elapsed_ms']} ms</p>

  <div class="src">
    <span class="dot"></span>
    <span>Live data from</span>
    <a href="https://www.coingecko.com" target="_blank">CoinGecko</a>
    <span style="color:#475569">·</span>
    <span style="color:#94a3b8">{source}</span>
  </div>

  <div class="rbar">
    <span>Auto-refreshing in <span id="cd">60</span>s</span>
    <span style="color:{sentiment_color};font-weight:700">{s['market_sentiment']}</span>
  </div>

  <!-- Medallion Pipeline -->
  <div class="pipeline">
    <div class="layer b">
      <div class="lname">Bronze</div>
      <div class="lnum">{b['rows']}</div>
      <div class="ldesc">Raw coin records ingested<br/>{b['size_kb']} KB · {b['bullish']} bullish · {b['bearish']} bearish</div>
    </div>
    <div class="arrow">→</div>
    <div class="layer sv">
      <div class="lname">Silver</div>
      <div class="lnum">{s['rows_out']}</div>
      <div class="ldesc">{s['dropped']} dropped · {s['price_anomalies']} price anomalies<br/>{s['volume_spikes']} volume spikes · {s['anomaly_rate_pct']}% flagged</div>
    </div>
    <div class="arrow">→</div>
    <div class="layer g">
      <div class="lname">Gold</div>
      <div class="lnum">${g['total_market_cap_B']:,.0f}B</div>
      <div class="ldesc">Total market cap<br/>${g['total_volume_B']:,.1f}B volume · {g['total_coins']} coins</div>
    </div>
  </div>

  <!-- Market stats -->
  <div class="stats">
    <div class="stat"><div class="stat-n">{g['bullish_coins']}</div><div class="stat-l">Bullish Coins</div></div>
    <div class="stat"><div class="stat-n" style="color:#ef4444">{g['bearish_coins']}</div><div class="stat-l">Bearish Coins</div></div>
    <div class="stat"><div class="stat-n">{s['price_anomalies']}</div><div class="stat-l">Price Anomalies</div></div>
    <div class="stat"><div class="stat-n">{s['volume_spikes']}</div><div class="stat-l">Volume Spikes</div></div>
    <div class="stat"><div class="stat-n" style="color:{sentiment_color}">{s['avg_change_24h']:+.2f}%</div><div class="stat-l">Avg 24h Change</div></div>
  </div>

  <h2>By Market Cap Tier</h2>
  <table>
    <thead><tr><th>Tier</th><th>Market Cap</th><th>24h Volume</th><th>Coins</th><th>Avg 24h Change</th><th>Anomalies</th></tr></thead>
    <tbody>{tier_rows}</tbody>
  </table>

  <div class="two">
    <div>
      <h2>Top 5 Gainers (24h)</h2>
      <table>
        <thead><tr><th>Symbol</th><th>Name</th><th>Price</th><th>24h Change</th></tr></thead>
        <tbody>{gainer_rows}</tbody>
      </table>
    </div>
    <div>
      <h2>Top 5 Losers (24h)</h2>
      <table>
        <thead><tr><th>Symbol</th><th>Name</th><th>Price</th><th>24h Change</th></tr></thead>
        <tbody>{loser_rows}</tbody>
      </table>
    </div>
  </div>

  <div class="btns">
    <a class="btn solid" href="/" onclick="location.reload();return false;">↻ Re-run Pipeline</a>
    <a class="btn ghost" href="/docs">📖 API Docs</a>
    <a class="btn green" href="https://www.coingecko.com/en/coins/bitcoin" target="_blank">🌐 Live Data Source</a>
  </div>

  <script>
    let t=60; const el=document.getElementById("cd");
    setInterval(()=>{{t--;el.textContent=t;if(t<=0)location.reload();}},1000);
  </script>
</body>
</html>"""
